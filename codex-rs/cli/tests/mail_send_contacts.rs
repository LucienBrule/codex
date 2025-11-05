use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use anyhow::Context;
use anyhow::Result;
use assert_cmd::Command;
use serde_json::Value as JsonValue;
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

fn codex_command(home: &Path) -> Result<Command> {
    let mut cmd = Command::cargo_bin("codex")?;
    cmd.env("CODEX_HOME", home);
    cmd.env("HOME", home);
    cmd.env("CODEX_MAILBOX_OOB_FORCE", "1");
    cmd.env("OPENAI_API_KEY", "test");
    Ok(cmd)
}

fn registry_dir(home: &Path, namespace: &str) -> PathBuf {
    home.join(namespace).join("mailbox")
}

fn write_registry(
    home: &Path,
    namespace: &str,
    conversation_id: Uuid,
    socket_path: &Path,
) -> Result<PathBuf> {
    let dir = registry_dir(home, namespace);
    fs::create_dir_all(&dir)?;
    let registry = json!({
        "version": 1,
        "namespace": namespace,
        "updated_at": "2025-10-14T00:00:00Z",
        "entries": {
            (conversation_id.to_string()): {
                "conversation_id": conversation_id,
                "socket_path": socket_path.to_string_lossy(),
                "pid": 4242,
                "worker_id": "worker.test",
                "last_heartbeat": "2025-10-14T00:00:00Z"
            }
        }
    });
    let path = dir.join("registry.json");
    fs::write(&path, serde_json::to_vec_pretty(&registry)?)?;
    Ok(path)
}

fn spawn_mailbox_listener(
    socket_path: PathBuf,
    ack_payload: serde_json::Value,
) -> (thread::JoinHandle<Result<()>>, mpsc::Receiver<()>) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || -> Result<()> {
        let _ = fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        ready_tx.send(()).ok();

        if let Ok((stream, _addr)) = listener.accept() {
            let mut cloned = stream
                .try_clone()
                .context("failed to clone unix stream for reading")?;
            let mut reader = BufReader::new(&mut cloned);
            let mut payload = String::new();
            reader
                .read_line(&mut payload)
                .context("failed to read mailbox payload")?;

            let mut stream = stream;
            let mut ack_line = ack_payload.to_string();
            ack_line.push('\n');
            stream
                .write_all(ack_line.as_bytes())
                .context("failed to write ack")?;
            stream.flush().context("failed to flush ack")?;
        }

        Ok(())
    });

    (handle, ready_rx)
}

#[test]
fn mail_send_with_to_resolves_contact() -> Result<()> {
    let codex_home = TempDir::new()?;
    let namespace = "codex";
    let ns_dir = codex_home.path().join(namespace);
    fs::create_dir_all(&ns_dir)?;

    // contacts.toml mapping
    fs::write(
        ns_dir.join("contacts.toml"),
        r#"
[contacts]
impl.codex.search = "11111111-1111-4111-8111-111111111111"
"#,
    )?;

    let conversation_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let message_id = Uuid::now_v7();
    let mailbox_dir = registry_dir(codex_home.path(), namespace);
    fs::create_dir_all(&mailbox_dir)?;
    let socket_path = mailbox_dir.join(format!("{conversation_id}.sock"));
    write_registry(codex_home.path(), namespace, conversation_id, &socket_path)?;

    let ack_payload = json!({
        "ok": true,
        "ack": "delivered",
        "queue_depth": 0,
        "message_id": message_id,
        "correlation_id": "test-correlation"
    });
    let (listener, ready_rx) = spawn_mailbox_listener(socket_path.clone(), ack_payload);
    ready_rx.recv().context("listener not ready")?;

    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "orchestrator.test",
            "--content",
            "Hello contacts",
            "--priority",
            "normal",
            "--timeout",
            "5s",
            "--audit-request-id",
            "req-test",
            "--to",
            "impl.codex.search",
            "--message-id",
            &message_id.to_string(),
            "--json",
        ])
        .output()
        .context("failed to run codex mail send")?;

    listener.join().expect("listener join")?;

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    let payload: JsonValue = serde_json::from_str(&stdout)?;
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        payload.get("conversation_id").and_then(|v| v.as_str()),
        Some(conversation_id.to_string().as_str())
    );
    Ok(())
}

#[test]
fn mail_send_with_unknown_to_errors() -> Result<()> {
    let codex_home = TempDir::new()?;
    let namespace = "codex";
    let ns_dir = codex_home.path().join(namespace);
    fs::create_dir_all(&ns_dir)?;
    fs::write(
        ns_dir.join("contacts.toml"),
        "[contacts]\nknown.contact = \"550e8400-e29b-41d4-a716-446655440000\"\n",
    )?;

    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "orchestrator.test",
            "--content",
            "Hello contacts",
            "--priority",
            "normal",
            "--audit-request-id",
            "req-test",
            "--to",
            "missing.contact",
        ])
        .output()
        .context("failed to run codex mail send")?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Contact 'missing.contact' not found"));
    Ok(())
}
