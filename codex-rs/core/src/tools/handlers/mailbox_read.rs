use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryState;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;

use crate::rollout::list::find_conversation_path_by_id_str;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

pub const MAILBOX_READ_TOOL_NAME: &str = "mailbox_read";

// Track acknowledgements per conversation to avoid re-surfacing the same message.
// Keyed by (conversation_id, message_id)
lazy_static! {
    static ref ACKED: Mutex<HashSet<(codex_protocol::ConversationId, Uuid)>> =
        Mutex::new(HashSet::new());
}

#[derive(Debug, Deserialize)]
struct MailboxReadArgs {
    #[serde(default = "default_max")]
    max: u64,
    #[serde(default)]
    ack: bool,
    #[serde(default)]
    filter: Option<MailboxReadFilter>,
}

#[derive(Debug, Deserialize, Default)]
struct MailboxReadFilter {
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    subject_contains: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    conversation_id: Option<String>,
}

fn default_max() -> u64 {
    50
}

#[derive(Debug, Serialize, Deserialize)]
struct MailboxReadMessageOut {
    message_id: Uuid,
    from: String,
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    content: String,
    content_type: String,
    priority: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    received_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MailboxReadResult {
    ok: bool,
    messages: Vec<MailboxReadMessageOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acked: Option<u64>,
}

pub struct MailboxReadHandler;

#[async_trait]
impl ToolHandler for MailboxReadHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(
        &self,
        invocation: ToolInvocation,
    ) -> Result<ToolOutput, crate::function_tool::FunctionCallError> {
        use crate::function_tool::FunctionCallError;
        let ToolInvocation {
            session, payload, ..
        } = invocation;

        let args = match payload {
            ToolPayload::Function { arguments } => {
                serde_json::from_str::<MailboxReadArgs>(&arguments).map_err(|e| {
                    FunctionCallError::RespondToModel(format!(
                        "failed to parse function arguments: {e}"
                    ))
                })?
            }
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "mailbox read handler requires function payload".into(),
                ));
            }
        };

        let (conv_id, rollout_path_opt) = {
            // Resolve conversation id override if provided
            let current_id = session.get_conversation_id();
            if let Some(filter) = &args.filter {
                if let Some(ref conv_override) = filter.conversation_id {
                    let path = find_rollout_path_for(&session, conv_override).await;
                    (current_id, path)
                } else {
                    (current_id, get_current_rollout_path(&session).await)
                }
            } else {
                (current_id, get_current_rollout_path(&session).await)
            }
        };

        let Some(rollout_path) = rollout_path_opt else {
            let out = MailboxReadResult {
                ok: true,
                messages: Vec::new(),
                acked: None,
            };
            let content = serde_json::to_string_pretty(&out)
                .map_err(|e| FunctionCallError::Fatal(format!("serialize output: {e}")))?;
            return Ok(ToolOutput::Function {
                content,
                success: Some(true),
            });
        };

        // Prefer JSONL inbox spool if present; fall back to rollout
        let ns = std::env::var("CODEX_NAMESPACE").unwrap_or_else(|_| "codex".to_string());
        let spool_dir = resolve_spool_dir_from_rollout(&rollout_path, &ns);
        let spool_path = spool_dir
            .as_ref()
            .map(|d| d.join(format!("inbox-{}.jsonl", conv_id)));
        let events = if let Some(p) = &spool_path {
            if p.exists() {
                read_spool_deliveries(p).await.unwrap_or_default()
            } else {
                read_mailbox_events(&rollout_path)
                    .await
                    .map_err(|e| FunctionCallError::Fatal(format!("failed to read rollout: {e}")))?
            }
        } else {
            read_mailbox_events(&rollout_path)
                .await
                .map_err(|e| FunctionCallError::Fatal(format!("failed to read rollout: {e}")))?
        };

        // Keep only latest entry per message_id, prefer Delivered over Enqueued
        use std::collections::HashMap;
        let mut latest: HashMap<Uuid, MailboxDeliveryEvent> = HashMap::new();
        for ev in events.into_iter() {
            let mid = ev.message.message_id;
            match latest.get(&mid) {
                None => {
                    latest.insert(mid, ev);
                }
                Some(prev) => {
                    let better = match (&prev.state, &ev.state) {
                        (MailboxDeliveryState::Delivered, MailboxDeliveryState::Enqueued) => false,
                        (MailboxDeliveryState::Enqueued, MailboxDeliveryState::Delivered) => true,
                        _ => {
                            // compare observed_at
                            ev.observed_at.unwrap_or(OffsetDateTime::UNIX_EPOCH)
                                > prev.observed_at.unwrap_or(OffsetDateTime::UNIX_EPOCH)
                        }
                    };
                    if better {
                        latest.insert(mid, ev);
                    }
                }
            }
        }

        // Apply filters and ack suppression
        let since_ts = args
            .filter
            .as_ref()
            .and_then(|f| f.since.as_ref())
            .and_then(|s| {
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()
            });
        let from_filter = args.filter.as_ref().and_then(|f| f.from.clone());
        let subject_substr = args
            .filter
            .as_ref()
            .and_then(|f| f.subject_contains.clone())
            .map(|s| s.to_ascii_lowercase());

        let mut out_msgs: Vec<MailboxReadMessageOut> = Vec::new();
        let mut to_ack: Vec<Uuid> = Vec::new();
        // Load persisted sidecar acks if available
        let sidecar_acked: Option<std::collections::HashSet<Uuid>> = if let Some(dir) = &spool_dir {
            let sidecar = dir.join(format!("acks-{}.jsonl", conv_id));
            if sidecar.exists() {
                read_sidecar_acks(&sidecar).await.ok()
            } else {
                None
            }
        } else {
            None
        };
        {
            let acked = ACKED.lock().await;
            for ev in latest.values() {
                // Only consider delivered messages for inbox semantics
                if ev.state != MailboxDeliveryState::Delivered {
                    continue;
                }
                if acked.contains(&(conv_id, ev.message.message_id)) {
                    continue;
                }
                if let Some(ref set) = sidecar_acked {
                    if set.contains(&ev.message.message_id) {
                        continue;
                    }
                }
                if let Some(since) = since_ts {
                    if let Some(obs) = ev.observed_at {
                        if obs < since {
                            continue;
                        }
                    }
                }
                if let Some(ref from) = from_filter {
                    if ev.message.sender.id != *from {
                        continue;
                    }
                }
                if let Some(ref needle) = subject_substr {
                    let hay = ev
                        .message
                        .body
                        .subject
                        .as_deref()
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if !hay.contains(needle) {
                        continue;
                    }
                }

                // Build output
                let content_type = match ev.message.body.content_type {
                    codex_protocol::mailbox::MailboxContentType::TextPlain => {
                        "text/plain".to_string()
                    }
                    codex_protocol::mailbox::MailboxContentType::TextMarkdown => {
                        "text/markdown".to_string()
                    }
                    codex_protocol::mailbox::MailboxContentType::ApplicationJson => {
                        "application/json".to_string()
                    }
                };
                out_msgs.push(MailboxReadMessageOut {
                    message_id: ev.message.message_id,
                    from: ev.message.sender.id.clone(),
                    role: format!("{:?}", ev.message.sender.role).to_lowercase(),
                    subject: ev.message.body.subject.clone(),
                    content: ev.message.body.content.clone(),
                    content_type,
                    priority: format!("{:?}", ev.message.priority).to_lowercase(),
                    received_at: ev.observed_at.map(|t| {
                        t.format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_else(|_| "<invalid>".into())
                    }),
                    correlation_id: ev.correlation_id.clone(),
                });
                to_ack.push(ev.message.message_id);
                if out_msgs.len() as u64 >= args.max {
                    break;
                }
            }
        }

        let acked_count = if args.ack && !to_ack.is_empty() {
            let mut acked = ACKED.lock().await;
            for mid in &to_ack {
                acked.insert((conv_id, *mid));
            }
            // Also persist to sidecar if spool directory available (best effort)
            if let Some(dir) = &spool_dir {
                let sidecar = dir.join(format!("acks-{}.jsonl", conv_id));
                let now = time::OffsetDateTime::now_utc();
                for mid in &to_ack {
                    let _ = append_ack_sidecar(&sidecar, *mid, now).await;
                }
            }
            Some(to_ack.len() as u64)
        } else {
            None
        };

        let out = MailboxReadResult {
            ok: true,
            messages: out_msgs,
            acked: acked_count,
        };
        let content = serde_json::to_string_pretty(&out)
            .map_err(|e| FunctionCallError::Fatal(format!("serialize output: {e}")))?;
        Ok(ToolOutput::Function {
            content,
            success: Some(true),
        })
    }
}

async fn get_current_rollout_path(session: &crate::codex::Session) -> Option<PathBuf> {
    let guard = session.services.rollout.lock().await;
    if let Some(rec) = guard.as_ref() {
        // Best-effort flush so readers observe up-to-date file contents
        let _ = rec.flush().await;
        Some(rec.get_rollout_path())
    } else {
        None
    }
}

async fn find_rollout_path_for(session: &crate::codex::Session, id_str: &str) -> Option<PathBuf> {
    // Derive codex_home from current rollout path location: ~/.codex/sessions/YYYY/MM/DD/rollout-....jsonl
    let base = get_current_rollout_path(session).await?;
    let mut cur = base.as_path();
    // Walk up 5 directories to reach ~/.codex
    for _ in 0..5 {
        cur = cur.parent()?;
    }
    let codex_home = cur.to_path_buf();
    match find_conversation_path_by_id_str(&codex_home, id_str).await {
        Ok(Some(p)) => Some(p),
        _ => None,
    }
}

async fn read_mailbox_events(path: &PathBuf) -> std::io::Result<Vec<MailboxDeliveryEvent>> {
    let text = tokio::fs::read_to_string(path).await?;
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: serde_json::Result<RolloutLine> = serde_json::from_str(trimmed);
        let Ok(rl) = parsed else { continue };
        match rl.item {
            RolloutItem::EventMsg(EventMsg::MailboxDelivery(ev)) => out.push(ev),
            _ => {}
        }
    }
    Ok(out)
}

fn resolve_spool_dir_from_rollout(rollout_path: &Path, namespace: &str) -> Option<PathBuf> {
    // layout: <codex_home>/sessions/YYYY/MM/DD/rollout-...jsonl
    let mut cur = rollout_path.to_path_buf();
    for _ in 0..5 {
        cur = cur.parent()?.to_path_buf();
    }
    Some(cur.join(namespace).join("mailbox").join("inbox"))
}

async fn read_spool_deliveries(path: &Path) -> std::io::Result<Vec<MailboxDeliveryEvent>> {
    #[derive(serde::Deserialize)]
    struct SpoolIn {
        #[serde(rename = "type")]
        _t: Option<String>,
        message_id: Uuid,
        from: String,
        role: Option<String>,
        subject: Option<String>,
        content: String,
        content_type: String,
        received_at: Option<time::OffsetDateTime>,
    }
    let text = tokio::fs::read_to_string(path).await?;
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<SpoolIn>(trimmed) {
            Ok(rec) => {
                use codex_protocol::mailbox::*;
                use codex_protocol::protocol::*;
                let mut msg = MailboxMessage::default();
                msg.message_id = rec.message_id;
                msg.sender.id = rec.from;
                if let Some(role) = rec.role {
                    msg.sender.role = match role.to_ascii_lowercase().as_str() {
                        "system" => MailboxSenderRole::System,
                        "orchestrator" => MailboxSenderRole::Orchestrator,
                        "operator" => MailboxSenderRole::Operator,
                        "automation" => MailboxSenderRole::Automation,
                        _ => MailboxSenderRole::System,
                    };
                }
                msg.body.subject = rec.subject;
                msg.body.content = rec.content;
                msg.body.content_type = match rec.content_type.as_str() {
                    "text/markdown" => MailboxContentType::TextMarkdown,
                    "application/json" => MailboxContentType::ApplicationJson,
                    _ => MailboxContentType::TextPlain,
                };
                let ev = MailboxDeliveryEvent {
                    state: MailboxDeliveryState::Delivered,
                    message: msg,
                    observed_at: rec.received_at,
                    queue_depth: None,
                    ingress: None,
                    delivery_latency_ms: None,
                    correlation_id: None,
                };
                out.push(ev);
            }
            Err(_) => continue,
        }
    }
    Ok(out)
}

async fn append_ack_sidecar(
    path: &Path,
    message_id: Uuid,
    ts: time::OffsetDateTime,
) -> std::io::Result<()> {
    #[derive(serde::Serialize)]
    struct AckOut<'a> {
        r#type: &'static str,
        message_id: &'a Uuid,
        #[serde(with = "time::serde::rfc3339")]
        acked_at: time::OffsetDateTime,
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let line = AckOut {
        r#type: "ack",
        message_id: &message_id,
        acked_at: ts,
    };
    let mut json = serde_json::to_string(&line)?;
    json.push('\n');
    file.write_all(json.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

async fn read_sidecar_acks(path: &Path) -> std::io::Result<std::collections::HashSet<Uuid>> {
    #[derive(serde::Deserialize)]
    struct AckIn {
        r#type: String,
        message_id: Uuid,
    }
    let text = tokio::fs::read_to_string(path).await?;
    let mut set = std::collections::HashSet::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(rec) = serde_json::from_str::<AckIn>(trimmed) {
            if rec.r#type == "ack" {
                set.insert(rec.message_id);
            }
        }
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::tests::spawn_test_mailbox_session;
    use crate::tools::context::SharedTurnDiffTracker;
    use crate::tools::context::ToolInvocation;
    use crate::tools::context::ToolPayload;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    #[tokio::test]
    async fn mailbox_read_lists_and_acks() {
        let (_guard, session, turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");

        // First, send a mailbox message and wait for delivery
        let send_handler = crate::tools::handlers::MailboxSendHandler;
        let target_id = uuid::Uuid::now_v7();
        let send_args = json!({
            "message": {
                "sender": { "id": "system.test", "role": "system" },
                "body": { "content": "hello", "content_type": "text/plain", "subject": "greet" },
                "audit": { "request_id": "REQ-99", "justification": "ok", "change_ticket": "CHG-99" },
                "priority": "high"
            },
            "conversation_id": target_id.to_string(),
            "timeout_seconds": 5,
            "wait_for_delivery": true
        });
        let tracker: SharedTurnDiffTracker = Arc::new(TokioMutex::new(TurnDiffTracker::new()));
        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }
        let inv = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker: tracker.clone(),
            sub_id: "sub-1".into(),
            call_id: "call-1".into(),
            tool_name: crate::tools::handlers::MAILBOX_SEND_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: send_args.to_string(),
            },
        };
        let _ = send_handler.handle(inv).await.expect("send ok");

        // Now read without ack
        let read_handler = MailboxReadHandler;
        let read_args = json!({ "max": 10 });
        let inv_read = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker: tracker.clone(),
            sub_id: "sub-2".into(),
            call_id: "call-2".into(),
            tool_name: MAILBOX_READ_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: read_args.to_string(),
            },
        };
        let out1 = read_handler.handle(inv_read).await.expect("read ok");
        if let ToolOutput::Function { content, .. } = out1 {
            let parsed: MailboxReadResult = serde_json::from_str(&content).expect("parse result");
            assert!(parsed.ok);
            assert!(!parsed.messages.is_empty());
        } else {
            panic!("expected function output");
        }

        // Read with ack=true and then ensure subsequent read omits it
        let read_args2 = json!({ "max": 10, "ack": true });
        let inv_read2 = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker: tracker.clone(),
            sub_id: "sub-3".into(),
            call_id: "call-3".into(),
            tool_name: MAILBOX_READ_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: read_args2.to_string(),
            },
        };
        let out2 = read_handler.handle(inv_read2).await.expect("read2 ok");
        let acked = if let ToolOutput::Function { content, .. } = out2 {
            let parsed: MailboxReadResult = serde_json::from_str(&content).expect("parse result");
            parsed.acked.unwrap_or(0)
        } else {
            0
        };
        assert!(acked >= 1);

        let read_args3 = json!({ "max": 10 });
        let inv_read3 = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker,
            sub_id: "sub-4".into(),
            call_id: "call-4".into(),
            tool_name: MAILBOX_READ_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: read_args3.to_string(),
            },
        };
        let out3 = read_handler.handle(inv_read3).await.expect("read3 ok");
        if let ToolOutput::Function { content, .. } = out3 {
            let parsed: MailboxReadResult = serde_json::from_str(&content).expect("parse result");
            assert!(parsed.messages.is_empty());
        }

        submission_task.abort();
    }

    // Sidecar ack behavior is exercised via integration and soak harnesses.
}
