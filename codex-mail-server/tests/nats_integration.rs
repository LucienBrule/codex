use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_nats::ConnectOptions;
use codex_mail_server::config::{BrokerConfig, DeliveryBackendKind, MailServerConfig};
use codex_mail_server::registry::RegistryWatcher;
use codex_mail_server::server::MailDispatcherServer;
use codex_protocol::mailbox::{MailboxAudience, MailboxMessage, MailboxSenderRole};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const NATS_URL: &str = "nats://127.0.0.1:4222";
const STACK_SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/tasks/mailbox_nats_stack.sh"
);
const REPO_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

struct NatsStackGuard {
    project: String,
}

impl NatsStackGuard {
    async fn start() -> Result<Self> {
        let project = format!("codex-mailbox-nats-test-{}", std::process::id());
        run_stack_command(&project, "up").await?;
        wait_for_nats().await?;
        Ok(Self { project })
    }
}

impl Drop for NatsStackGuard {
    fn drop(&mut self) {
        let _ = Command::new(STACK_SCRIPT)
            .current_dir(REPO_ROOT)
            .arg("--project")
            .arg(&self.project)
            .arg("down")
            .status();
    }
}

async fn run_stack_command(project: &str, command: &str) -> Result<()> {
    let status = tokio::process::Command::new(STACK_SCRIPT)
        .current_dir(REPO_ROOT)
        .arg("--project")
        .arg(project)
        .arg(command)
        .status()
        .await
        .with_context(|| format!("failed to invoke mailbox_nats_stack.sh {command}"))?;
    if !status.success() {
        return Err(anyhow!(
            "mailbox_nats_stack.sh {command} exited with status {status}"
        ));
    }
    Ok(())
}

async fn wait_for_nats() -> Result<()> {
    for attempt in 0..20 {
        match ConnectOptions::new().connect(NATS_URL).await {
            Ok(client) => {
                let _ = client.drain().await;
                return Ok(());
            }
            Err(err) if attempt < 19 => {
                sleep(Duration::from_millis(200)).await;
                tracing::debug!(target: "codex::mailbox", %err, "waiting for nats broker");
            }
            Err(err) => return Err(anyhow!("timed out waiting for nats: {err}")),
        }
    }
    Err(anyhow!("unreachable"))
}

fn broker_config() -> BrokerConfig {
    BrokerConfig {
        url: NATS_URL.to_string(),
        token: None,
        subject_prefix: "codex.mail".to_string(),
        request_timeout: Duration::from_secs(2),
    }
}

#[allow(clippy::field_reassign_with_default)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nats_round_trip_delivery() -> Result<()> {
    let _guard = NatsStackGuard::start().await?;

    let namespace = "test";
    let target_conversation = Uuid::now_v7();
    let source_conversation = Uuid::now_v7();

    let dir_a = TempDir::new().context("create temp dir for dispatcher A")?;
    let dir_b = TempDir::new().context("create temp dir for dispatcher B")?;

    let dispatcher_socket_a = dir_a.path().join("dispatcher.sock");
    let dispatcher_socket_b = dir_b.path().join("dispatcher.sock");
    let registry_path_a = dir_a.path().join("registry.json");
    let registry_path_b = dir_b.path().join("registry.json");
    let mailbox_socket_b = dir_b.path().join("mailbox.sock");
    let mailbox_listener =
        UnixListener::bind(&mailbox_socket_b).context("bind mailbox listener")?;

    tokio::fs::write(
        &registry_path_a,
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "entries": {}
        }))?,
    )
    .await
    .context("write registry a")?;

    tokio::fs::write(
        &registry_path_b,
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "entries": {
                target_conversation.to_string(): {
                    "session_id": target_conversation.to_string(),
                    "pid": std::process::id(),
                    "socket_path": mailbox_socket_b.to_string_lossy(),
                    "namespace": namespace,
                }
            }
        }))?,
    )
    .await
    .context("write registry b")?;

    let broker = Some(broker_config());

    let config_a = MailServerConfig {
        namespace: namespace.to_string(),
        codex_home: dir_a.path().to_path_buf(),
        socket_path: dispatcher_socket_a.clone(),
        registry_path: registry_path_a.clone(),
        ack_timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_millis(250),
        retry_backoff: vec![Duration::from_millis(50)],
        registry_poll_interval: Duration::from_millis(100),
        max_inflight: 8,
        delivery_backend: DeliveryBackendKind::Nats,
        broker: broker.clone(),
    };

    let config_b = MailServerConfig {
        namespace: namespace.to_string(),
        codex_home: dir_b.path().to_path_buf(),
        socket_path: dispatcher_socket_b.clone(),
        registry_path: registry_path_b.clone(),
        ack_timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_millis(250),
        retry_backoff: vec![Duration::from_millis(50)],
        registry_poll_interval: Duration::from_millis(100),
        max_inflight: 8,
        delivery_backend: DeliveryBackendKind::Nats,
        broker,
    };

    let watcher_a = RegistryWatcher::new(&config_a).await?;
    let watcher_b = RegistryWatcher::new(&config_b).await?;

    let server_a = MailDispatcherServer::new(config_a.clone(), watcher_a).await?;
    let server_b = MailDispatcherServer::new(config_b.clone(), watcher_b).await?;

    let handle_a = tokio::spawn(async move {
        let _ = server_a.run().await;
    });
    let handle_b = tokio::spawn(async move {
        let _ = server_b.run().await;
    });

    sleep(Duration::from_millis(400)).await;

    let (delivered_tx, delivered_rx) = oneshot::channel();
    let listener = mailbox_listener;
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_ok()
                && let Ok(msg) = serde_json::from_str::<MailboxMessage>(line.trim())
            {
                let ack = json!({
                    "ok": true,
                    "submission_id": "sub-1",
                    "message_id": msg.message_id,
                    "queue_depth": 1
                });
                let mut stream = reader.into_inner();
                let _ = stream
                    .write_all(serde_json::to_string(&ack).unwrap().as_bytes())
                    .await;
                let _ = stream.write_all(b"\n").await;
                let _ = stream.flush().await;
                let _ = delivered_tx.send(msg);
            }
        }
    });

    let mut message = MailboxMessage::default();
    message.message_id = Uuid::now_v7();
    message.sender.id = "test.dispatcher".to_string();
    message.sender.role = MailboxSenderRole::Automation;
    message.body.subject = Some("NATS Dispatch".to_string());
    message.body.content = "hello via nats".to_string();
    message.audit.request_id = Some("req-nats-integration".to_string());
    message.audience = Some(MailboxAudience {
        conversation_id: Some(target_conversation),
        worker_id: None,
        scopes: None,
        allow_broadcast: false,
    });

    let request = json!({
        "submission_id": "sub-1",
        "source_conversation_id": source_conversation,
        "target_conversation_id": target_conversation,
        "message": message,
    });

    let mut stream = UnixStream::connect(&dispatcher_socket_a)
        .await
        .context("connect to dispatcher a")?;
    let payload = serde_json::to_vec(&request)?;
    stream.write_all(&payload).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let response: serde_json::Value = serde_json::from_str(line.trim())?;
    assert_eq!(response["status"], "delivered");

    let delivered = timeout(Duration::from_secs(5), delivered_rx)
        .await
        .context("waiting for mailbox delivery")??;
    assert_eq!(delivered.body.content, "hello via nats");

    handle_a.abort();
    handle_b.abort();

    Ok(())
}
