use std::path::Path;

use anyhow::Result;
use assert_cmd::Command;
use serde_json::Value as JsonValue;
use tempfile::TempDir;

fn codex_command(home: &Path) -> Result<Command> {
    let mut cmd = Command::cargo_bin("codex")?;
    cmd.env("CODEX_HOME", home);
    cmd.env("HOME", home);
    cmd.env("CODEX_MAILBOX_OOB_FORCE", "1");
    cmd.env("OPENAI_API_KEY", "test");
    Ok(cmd)
}

#[test]
fn mail_send_basic_json_output() -> Result<()> {
    let codex_home = TempDir::new()?;
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
            "10s",
            "--audit-request-id",
            "req-test",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "mail send exited with {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    let payload: JsonValue = serde_json::from_str(&stdout)?;
    assert!(payload.get("message_id").is_some(), "message_id missing");
    assert_eq!(
        payload
            .get("submission_id")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty()),
        Some(false)
    );
    let delivered_queue_depth = payload
        .pointer("/delivered/queue_depth")
        .and_then(|v| v.as_u64())
        .unwrap_or_default();
    assert_eq!(
        delivered_queue_depth, 0,
        "expected queue to drain after delivery"
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
        .output()?;
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
