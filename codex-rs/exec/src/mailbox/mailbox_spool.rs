use std::io::Error as IoError;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use codex_core::config::Config;
use codex_protocol::ConversationId;
use codex_protocol::protocol::MailboxDeliveryEvent;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::FormatItem;
use time::macros::format_description;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::info;
use tracing::warn;

// Environment knobs
const DEFAULT_MAX_BYTES: u64 = 5 * 1024 * 1024; // 5 MiB
const DEFAULT_MAX_LINES: u64 = 50_000;
const DEFAULT_TTL_DAYS: i64 = 14;

#[derive(Clone)]
pub struct MailboxSpoolWriter {
    tx: mpsc::Sender<Cmd>,
}

enum Cmd {
    Append(MailboxDeliveryEvent),
    Flush { ack: oneshot::Sender<()> },
}

#[derive(Serialize)]
struct SpoolLine<'a> {
    r#type: &'static str,
    message_id: uuid::Uuid,
    from: &'a str,
    role: String,
    subject: Option<&'a str>,
    content: &'a str,
    content_type: &'a str,
    received_at: Option<OffsetDateTime>,
}

fn ns_from_env() -> String {
    std::env::var("CODEX_NAMESPACE").unwrap_or_else(|_| "codex".to_string())
}

fn limits_from_env() -> (u64, u64, i64) {
    let max_bytes = std::env::var("CODEX_MAILBOX_SPOOL_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_BYTES);
    let max_lines = std::env::var("CODEX_MAILBOX_SPOOL_MAX_LINES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_LINES);
    let ttl_days = std::env::var("CODEX_MAILBOX_INBOX_TTL_DAYS")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(DEFAULT_TTL_DAYS);
    (max_bytes, max_lines, ttl_days)
}

fn inbox_dir(config: &Config, namespace: &str) -> PathBuf {
    config
        .codex_home
        .join(namespace)
        .join("mailbox")
        .join("inbox")
}

fn inbox_active_path(dir: &Path, conversation_id: &ConversationId) -> PathBuf {
    dir.join(format!("inbox-{}.jsonl", conversation_id))
}

fn rotated_filename(conv_id: &str, ts: OffsetDateTime) -> Result<String, IoError> {
    let fmt: &[FormatItem] = format_description!("[year][month][day]-[hour][minute][second]");
    let stamp = ts
        .to_offset(time::UtcOffset::UTC)
        .format(fmt)
        .map_err(|e| IoError::other(format!("format timestamp: {e}")))?;
    Ok(format!("inbox-{}-{}.jsonl", conv_id, stamp))
}

async fn rotate_if_needed(path: &Path, max_bytes: u64, max_lines: u64) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let meta = tokio::fs::metadata(path).await?;
    let size_ok = meta.len() < max_bytes;
    let mut lines_ok = true;
    if !size_ok {
        lines_ok = false; // force rotate
    } else if max_lines > 0 {
        let content = tokio::fs::read(path).await?;
        let count = bytecount::count(&content, b'\n') as u64;
        lines_ok = count < max_lines;
    }
    if size_ok && lines_ok {
        return Ok(());
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("inbox.jsonl");
    // Derive conversation id from active file name prefix `inbox-<id>.jsonl`
    let conv = file_name
        .trim_start_matches("inbox-")
        .trim_end_matches(".jsonl");
    let conv = conv.split('-').next().unwrap_or(conv);
    let ts = OffsetDateTime::now_utc();
    let rotated = rotated_filename(conv, ts)?;
    let rotated_path = parent.join(rotated);
    tokio::fs::rename(path, rotated_path).await?;
    Ok(())
}

async fn prune_old_files(dir: &Path, ttl_days: i64) -> Result<()> {
    if ttl_days <= 0 {
        return Ok(());
    }
    let cutoff = OffsetDateTime::now_utc() - time::Duration::days(ttl_days);
    let mut rd = tokio::fs::read_dir(dir)
        .await
        .with_context(|| format!("read_dir {}", dir.display()))?;
    while let Ok(Some(ent)) = rd.next_entry().await {
        let path = ent.path();
        if !path.is_file() {
            continue;
        }
        if let Ok(meta) = ent.metadata().await {
            if let Ok(modified) = meta.modified() {
                if let Ok(dur) = modified.duration_since(std::time::UNIX_EPOCH) {
                    if let Ok(mod_ts) = OffsetDateTime::from_unix_timestamp(dur.as_secs() as i64) {
                        if mod_ts < cutoff {
                            let _ = tokio::fs::remove_file(&path).await;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

impl MailboxSpoolWriter {
    pub async fn new(
        config: Config,
        namespace: &str,
        conversation_id: ConversationId,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<Cmd>(1024);
        let ns = if namespace.is_empty() {
            ns_from_env()
        } else {
            namespace.to_string()
        };
        let dir = inbox_dir(&config, &ns);
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create inbox dir {}", dir.display()))?;

        let active_path = inbox_active_path(&dir, &conversation_id);
        let (max_bytes, max_lines, ttl_days) = limits_from_env();
        prune_old_files(&dir, ttl_days).await.ok();

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .await
            .with_context(|| format!("open inbox {}", active_path.display()))?;
        // Lock down permissions (best effort)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                tokio::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
                    .await;
        }

        // Announce readiness before spawning writer loop
        info!(target: "codex::mailbox", event = "mailbox.spool.ready", path = %active_path.display(), "mailbox spool initialized");

        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Cmd::Append(ev) => {
                        // rotate if needed (ignore errors)
                        if let Err(err) = rotate_if_needed(&active_path, max_bytes, max_lines).await
                        {
                            warn!(target: "codex::mailbox", event = "mailbox.spool.rotate_failed", ?err, path = %active_path.display());
                        }
                        let role_str = match ev.message.sender.role {
                            codex_protocol::mailbox::MailboxSenderRole::System => {
                                "system".to_string()
                            }
                            codex_protocol::mailbox::MailboxSenderRole::Orchestrator => {
                                "orchestrator".to_string()
                            }
                            codex_protocol::mailbox::MailboxSenderRole::Operator => {
                                "operator".to_string()
                            }
                            codex_protocol::mailbox::MailboxSenderRole::Automation => {
                                "automation".to_string()
                            }
                        };
                        let ct_str = match ev.message.body.content_type {
                            codex_protocol::mailbox::MailboxContentType::TextPlain => "text/plain",
                            codex_protocol::mailbox::MailboxContentType::TextMarkdown => {
                                "text/markdown"
                            }
                            codex_protocol::mailbox::MailboxContentType::ApplicationJson => {
                                "application/json"
                            }
                        };
                        let line = SpoolLine {
                            r#type: match ev.state {
                                codex_protocol::protocol::MailboxDeliveryState::Delivered => {
                                    "delivered"
                                }
                                _ => "enqueued",
                            },
                            message_id: ev.message.message_id,
                            from: &ev.message.sender.id,
                            role: role_str,
                            subject: ev.message.body.subject.as_deref(),
                            content: &ev.message.body.content,
                            content_type: ct_str,
                            received_at: ev.observed_at,
                        };
                        if let Ok(mut json) = serde_json::to_string(&line) {
                            json.push('\n');
                            if let Err(err) = file.write_all(json.as_bytes()).await {
                                warn!(target: "codex::mailbox", event = "mailbox.spool.write_failed", ?err, path = %active_path.display());
                            } else {
                                let _ = file.flush().await;
                            }
                        }
                    }
                    Cmd::Flush { ack } => {
                        let _ = file.flush().await;
                        let _ = ack.send(());
                    }
                }
            }
        });

        Ok(Self { tx })
    }

    pub async fn append_delivery(&self, ev: MailboxDeliveryEvent) -> Result<()> {
        self.tx
            .send(Cmd::Append(ev))
            .await
            .map_err(|e| anyhow::anyhow!("send append cmd: {e}"))
    }

    #[allow(dead_code)]
    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Cmd::Flush { ack: tx }).await;
        let _ = rx.await;
    }
}
