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
fn mail_send_registry_success() -> Result<()> {
    let codex_home = TempDir::new()?;
    let namespace = "codex";
    let conversation_id = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let mailbox_dir = registry_dir(codex_home.path(), namespace);
    fs::create_dir_all(&mailbox_dir)?;
    let socket_path = mailbox_dir.join(format!("{conversation_id}.sock"));
    write_registry(codex_home.path(), namespace, conversation_id, &socket_path)?;

    let message_id_str = message_id.to_string();
    let conversation_id_str = conversation_id.to_string();
    let socket_path_str = socket_path.display().to_string();

    let ack_payload = json!({
        "ok": true,
        "ack": "delivered",
        "queue_depth": 0,
        "message_id": message_id,
        "correlation_id": "test-correlation"
    });
    let (listener, ready_rx) = spawn_mailbox_listener(socket_path.clone(), ack_payload);
    ready_rx
        .recv()
        .context("listener did not signal readiness")?;

    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "orchestrator.test",
            "--subject",
            "Mailbox integration test",
            "--content",
            "Hello from test",
            "--priority",
            "normal",
            "--timeout",
            "5s",
            "--audit-request-id",
            "req-test",
            "--conversation-id",
            &conversation_id.to_string(),
            "--message-id",
            &message_id.to_string(),
            "--json",
        ])
        .output()
        .context("failed to run codex mail send")?;

    listener.join().expect("listener join")?;

    assert!(
        output.status.success(),
        "mail send exited with {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    let payload: JsonValue = serde_json::from_str(&stdout)?;
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        payload.get("message_id").and_then(|v| v.as_str()),
        Some(message_id_str.as_str())
    );
    assert_eq!(
        payload.get("conversation_id").and_then(|v| v.as_str()),
        Some(conversation_id_str.as_str())
    );
    assert_eq!(
        payload.pointer("/queue_depth").and_then(|v| v.as_u64()),
        Some(0)
    );
    assert_eq!(
        payload.get("socket_path").and_then(|v| v.as_str()),
        Some(socket_path_str.as_str())
    );
    Ok(())
}

#[test]
fn mail_send_registry_json_mode_none_minimal_output() -> Result<()> {
    // Arrange a registry and a listener that accepts but does not send an ack line,
    // simulating an ack-less server. With --json and ack-mode none, the CLI must
    // still print a minimal JSON payload and exit success without waiting.
    let codex_home = TempDir::new()?;
    let namespace = "codex";
    let conversation_id = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let mailbox_dir = registry_dir(codex_home.path(), namespace);
    fs::create_dir_all(&mailbox_dir)?;
    let socket_path = mailbox_dir.join(format!("{conversation_id}.sock"));
    write_registry(codex_home.path(), namespace, conversation_id, &socket_path)?;

    // Listener: accept and do nothing (no ack)
    let (ready_tx, ready_rx) = mpsc::channel();
    let sp = socket_path.clone();
    let listener = thread::spawn(move || -> Result<()> {
        let _ = fs::remove_file(&sp);
        let listener = UnixListener::bind(&sp)
            .with_context(|| format!("failed to bind {}", sp.display()))?;
        ready_tx.send(()).ok();
        // accept one connection and intentionally do not reply
        let _ = listener.accept();
        Ok(())
    });
    ready_rx.recv().context("listener did not signal readiness")?;

    // Act: send with ack-mode none and --json
    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "orchestrator.test",
            "--content",
            "noop ackless",
            "--priority",
            "normal",
            "--ack-mode",
            "none",
            "--timeout",
            "1s",
            "--audit-request-id",
            "req-test",
            "--conversation-id",
            &conversation_id.to_string(),
            "--message-id",
            &message_id.to_string(),
            "--json",
        ])
        .output()
        .context("failed to run codex mail send")?;

    listener.join().expect("listener join")?;

    // Assert: success and minimal JSON present
    assert!(
        output.status.success(),
        "mail send exited with {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    let payload: JsonValue = serde_json::from_str(&stdout)?;
    assert_eq!(payload.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(payload.get("ack").and_then(|v| v.as_str()), Some("none"));
    assert_eq!(
        payload.get("message_id").and_then(|v| v.as_str()),
        Some(message_id.to_string().as_str())
    );
    assert_eq!(
        payload.get("conversation_id").and_then(|v| v.as_str()),
        Some(conversation_id.to_string().as_str())
    );
    Ok(())
}

#[test]
fn mail_send_registry_missing_socket() -> Result<()> {
    let codex_home = TempDir::new()?;
    let namespace = "codex";
    let conversation_id = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let mailbox_dir = registry_dir(codex_home.path(), namespace);
    fs::create_dir_all(&mailbox_dir)?;
    let socket_path = mailbox_dir.join(format!("{conversation_id}.sock"));
    write_registry(codex_home.path(), namespace, conversation_id, &socket_path)?;

    // Intentionally do not create the socket; CLI should treat registry entry as stale.
    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "orchestrator.test",
            "--content",
            "stale socket test",
            "--priority",
            "normal",
            "--audit-request-id",
            "req-test",
            "--conversation-id",
            &conversation_id.to_string(),
            "--message-id",
            &message_id.to_string(),
        ])
        .output()
        .context("failed to run codex mail send")?;

    assert_eq!(output.status.code(), Some(64));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Run `codex_ctl mailbox sweep`"),
        "expected stale registry guidance, got: {stderr}"
    );

    Ok(())
}

#[test]
fn mail_send_registry_queue_full() -> Result<()> {
    let codex_home = TempDir::new()?;
    let namespace = "codex";
    let conversation_id = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let mailbox_dir = registry_dir(codex_home.path(), namespace);
    fs::create_dir_all(&mailbox_dir)?;
    let socket_path = mailbox_dir.join(format!("{conversation_id}.sock"));
    write_registry(codex_home.path(), namespace, conversation_id, &socket_path)?;

    let ack_payload = json!({
        "ok": false,
        "err": "queue_full",
        "queue_depth": 64,
        "reason": "simulated backpressure"
    });
    let (listener, ready_rx) = spawn_mailbox_listener(socket_path.clone(), ack_payload);
    ready_rx
        .recv()
        .context("listener did not signal readiness")?;

    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "orchestrator.test",
            "--content",
            "queue full test",
            "--priority",
            "normal",
            "--audit-request-id",
            "req-test",
            "--conversation-id",
            &conversation_id.to_string(),
            "--message-id",
            &message_id.to_string(),
        ])
        .output()
        .context("failed to run codex mail send")?;

    listener.join().expect("listener join")?;

    assert_eq!(
        output.status.code(),
        Some(69),
        "expected queue full exit code"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Mailbox queue is full"),
        "unexpected stderr: {stderr}"
    );

    Ok(())
}

#[test]
fn mail_send_rejects_automation_high_priority() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut cmd = codex_command(codex_home.path())?;
    let output = cmd
        .args([
            "mail",
            "send",
            "--sender-id",
            "automation.bot",
            "--sender-role",
            "automation",
            "--content",
            "automation high priority attempt",
            "--priority",
            "high",
            "--audit-request-id",
            "req-auto",
            "--audit-change-ticket",
            "CHG-42",
            "--audit-justification",
            "validation",
        ])
        .output()
        .context("failed to run codex mail send")?;
    assert!(
        !output.status.success(),
        "mail send should fail for automation high priority"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("automation role may only send low or normal priority messages"),
        "unexpected stderr: {stderr}"
    );
    Ok(())
}
