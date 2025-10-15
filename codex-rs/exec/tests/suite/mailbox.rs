#![allow(clippy::unwrap_used, clippy::expect_used)]
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_core::AuthManager;
use codex_core::ConversationManager;
use codex_core::NewConversation;
use codex_core::config::Config;
use codex_core::config::ConfigOverrides;
use codex_core::config::ConfigToml;
use codex_core::protocol::EventMsg;
use codex_core::protocol::MailboxDeliveryState;
use codex_core::protocol::SessionSource;
use codex_exec::MailboxServer;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[test]
fn mailbox_listener_accepts_ipc_message() -> Result<()> {
    unsafe {
        std::env::set_var("CODEX_MAILBOX_ENABLE_FORCE", "1");
        std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
    }

    let rt = Runtime::new().context("create tokio runtime")?;
    rt.block_on(async move {
        let codex_home = TempDir::new().context("create codex home")?;
        let mut config = load_default_config_for_test(&codex_home);
        config.codex_home = codex_home.path().to_path_buf();

        let auth = AuthManager::shared(config.codex_home.clone(), true);
        let manager = ConversationManager::new(auth, SessionSource::Exec);
        let NewConversation {
            conversation_id,
            conversation,
            session_configured: _,
        } = manager
            .new_conversation(config.clone())
            .await
            .context("spawn conversation")?;

        let server =
            MailboxServer::start_if_enabled(&config, conversation.clone(), conversation_id)
                .await?
                .expect("mailbox should be enabled");

        let namespace = std::env::var("CODEX_NAMESPACE").unwrap_or_else(|_| "codex".to_string());
        let mailbox_dir = config.codex_home.join(&namespace).join("mailbox");
        let socket_path = mailbox_dir.join(format!("{}.sock", conversation_id));
        wait_for_socket(&socket_path, Duration::from_secs(5)).await?;

        let stream = UnixStream::connect(&socket_path)
            .await
            .with_context(|| format!("connect to {}", socket_path.display()))?;
        let (reader, mut writer) = stream.into_split();

        let message_id = Uuid::new_v4();
        let request_id = format!("req-{}", Uuid::new_v4());
        let payload = serde_json::json!({
            "message": {
                "message_id": message_id,
                "priority": "normal",
                "sender": {"id": "ipc.tester", "role": "orchestrator"},
                "audience": {"allow_broadcast": false},
                "body": {
                    "subject": "IPC",
                    "content": "hello from mailbox test",
                    "content_type": "text/plain"
                },
                "ack_policy": {"mode": "none"},
                "audit": {"request_id": request_id},
                "metadata": {},
                "tags": {}
            }
        });
        let payload = serde_json::to_string(&payload)?;
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        let ack_task = async {
            let mut reader = tokio::io::BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            Ok::<String, anyhow::Error>(line)
        };

        let event_task = async {
            loop {
                let event = conversation.next_event().await?;
                server.handle_event(&event).await;
                match &event.msg {
                    EventMsg::MailboxDelivery(delivery)
                        if matches!(
                            delivery.state,
                            MailboxDeliveryState::Enqueued | MailboxDeliveryState::Delivered
                        ) =>
                    {
                        break;
                    }
                    EventMsg::Error(err) => {
                        bail!("mailbox enqueue failed: {}", err.message);
                    }
                    _ => {}
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        let (ack_line, _) = tokio::join!(ack_task, event_task);
        let ack_line = ack_line?;
        let ack: Value = serde_json::from_str(ack_line.trim())?;
        assert_eq!(ack.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            ack.get("message_id").and_then(Value::as_str),
            Some(message_id.to_string().as_str())
        );

        server.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn load_default_config_for_test(codex_home: &TempDir) -> Config {
    Config::load_from_base_config_with_overrides(
        ConfigToml::default(),
        ConfigOverrides::default(),
        codex_home.path().to_path_buf(),
    )
    .expect("defaults for test should always succeed")
}

async fn wait_for_socket(path: &PathBuf, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("timed out waiting for mailbox socket {}", path.display());
}
