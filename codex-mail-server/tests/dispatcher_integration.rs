use anyhow::{Context, Result};
use codex_mail_server::config::MailServerConfig;
use codex_mail_server::registry::RegistryWatcher;
use codex_mail_server::server::MailDispatcherServer;
use codex_protocol::mailbox::{MailboxAudience, MailboxMessage, MailboxSenderRole};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatcher_routes_message_between_sessions() -> Result<()> {
    let dir = TempDir::new().context("temp dir")?;
    let dispatcher_socket = dir.path().join("dispatcher.sock");
    let registry_path = dir.path().join("registry.json");
    let target_socket = dir.path().join("target.sock");

    let target_listener =
        UnixListener::bind(&target_socket).context("bind target mailbox socket")?;
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        if let Ok((stream, _)) = target_listener.accept().await {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_ok() {
                if let Ok(msg) = serde_json::from_str::<MailboxMessage>(line.trim()) {
                    let ack = json!({
                        "ok": true,
                        "submission_id": "sub-1",
                        "message_id": msg.message_id,
                        "queue_depth": 1
                    });
                    let mut stream = reader.into_inner();
                    let payload = serde_json::to_vec(&ack).unwrap();
                    let _ = stream.write_all(&payload).await;
                    let _ = stream.write_all(b"\n").await;
                    let _ = stream.flush().await;
                    let _ = tx.send(msg);
                }
            }
        }
    });

    let conversation_id = Uuid::now_v7();
    let registry = json!({
        "version": 1,
        "entries": {
            conversation_id.to_string(): {
                "session_id": conversation_id.to_string(),
                "pid": std::process::id(),
                "socket_path": target_socket.to_string_lossy(),
                "namespace": "test"
            }
        }
    });
    tokio::fs::write(&registry_path, serde_json::to_vec_pretty(&registry)?)
        .await
        .context("write registry")?;

    let config = MailServerConfig {
        namespace: "test".to_string(),
        codex_home: dir.path().to_path_buf(),
        socket_path: dispatcher_socket.clone(),
        registry_path: registry_path.clone(),
        ack_timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_millis(500),
        retry_backoff: vec![Duration::from_millis(50), Duration::from_millis(100)],
        registry_poll_interval: Duration::from_millis(50),
        max_inflight: 16,
    };

    let watcher = RegistryWatcher::new(&config).await?;
    sleep(Duration::from_millis(120)).await;

    let server = MailDispatcherServer::new(config.clone(), watcher);
    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(120)).await;

    let mut message = MailboxMessage::default();
    message.message_id = Uuid::now_v7();
    message.sender.id = "integration.test".to_string();
    message.sender.role = MailboxSenderRole::Automation;
    message.body.subject = Some("Integration".to_string());
    message.body.content = "Hello dispatcher".to_string();
    message.audit.request_id = Some("req-integration".to_string());
    message.audience = Some(MailboxAudience {
        conversation_id: Some(conversation_id),
        worker_id: None,
        scopes: None,
        allow_broadcast: false,
    });

    let request = json!({
        "submission_id": "sub-1",
        "source_conversation_id": Uuid::now_v7(),
        "target_conversation_id": conversation_id,
        "message": message,
    });

    let mut stream = UnixStream::connect(&dispatcher_socket)
        .await
        .context("connect to dispatcher")?;
    let payload = serde_json::to_vec(&request)?;
    stream.write_all(&payload).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let response: serde_json::Value = serde_json::from_str(line.trim())?;
    assert_eq!(response["status"], "delivered");

    let delivered = rx.await.context("receive forwarded message")?;
    assert_eq!(delivered.body.content, "Hello dispatcher");

    server_handle.abort();
    tokio::fs::remove_file(dispatcher_socket).await.ok();
    tokio::fs::remove_file(target_socket).await.ok();

    Ok(())
}
