use std::time::Duration;

use async_trait::async_trait;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryIngress;
use codex_protocol::protocol::MailboxDeliveryState;
use serde::Deserialize;
use serde::Serialize;
use time::format_description::well_known::Rfc3339;
use tokio::time::timeout;
use uuid::Uuid;

use crate::codex::MailboxEnqueueError;
use crate::function_tool::FunctionCallError;
use crate::mailbox::apply_mailbox_defaults;
use crate::mailbox::mailbox_feature_enabled;
use crate::mailbox::validate_mailbox_message;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

// Primary, provider-safe MCP tool name (underscore only)
pub const MAILBOX_SEND_TOOL_NAME: &str = "mailbox_send";
// Back-compat alias retained for older prompts/tests
pub const MAILBOX_SEND_TOOL_ALIAS: &str = "codex_mailbox_send";

pub struct MailboxSendHandler;

#[derive(Debug, Deserialize)]
struct MailboxSendArgs {
    #[serde(default)]
    message: MailboxMessage,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default = "default_timeout_seconds")]
    timeout_seconds: u64,
    #[serde(default = "default_wait_for_delivery")]
    wait_for_delivery: bool,
}

fn default_timeout_seconds() -> u64 {
    30
}

fn default_wait_for_delivery() -> bool {
    true
}

#[derive(Serialize, Deserialize)]
struct MailboxSendJsonOutputNormalized {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    message_id: Uuid,
    request_id: Option<String>,
    conversation_id: codex_protocol::ConversationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    ack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_depth: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
    // For parity with CLI JSON; not meaningful for MCP tools but kept for consistency
    socket_path: String,
}

#[derive(Serialize, Deserialize)]
struct MailboxEventSnapshot {
    state: String,
    queue_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery_latency_ms: Option<u64>,
}

#[async_trait]
impl ToolHandler for MailboxSendHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation {
            session,
            sub_id,
            call_id,
            payload,
            ..
        } = invocation;

        let args = match payload {
            ToolPayload::Function { arguments } => {
                serde_json::from_str::<MailboxSendArgs>(&arguments).map_err(|err| {
                    FunctionCallError::RespondToModel(format!(
                        "failed to parse function arguments: {err}"
                    ))
                })?
            }
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "mailbox send handler requires function payload".to_string(),
                ));
            }
        };

        let MailboxSendArgs {
            mut message,
            to,
            conversation_id,
            timeout_seconds,
            wait_for_delivery,
        } = args;

        // Resolve explicit conversation_id (if provided) or via contacts when 'to' is set.
        let requested_id: Option<Uuid> = if let Some(cid) = conversation_id.as_ref() {
            match Uuid::parse_str(cid) {
                Ok(id) => Some(id),
                Err(_) => {
                    return Err(FunctionCallError::RespondToModel(format!(
                        "invalid conversation_id UUID: {cid}"
                    )));
                }
            }
        } else if let Some(name) = to.as_ref() {
            let (home, ns) = crate::contacts::get_runtime_home_and_namespace();
            match crate::contacts::load_contacts(&home, &ns) {
                Ok(book) => match book.resolve(name) {
                    Some(id) => Some(id),
                    None => {
                        let primary_path = format!("codex_home/{}/contacts.toml", ns);
                        return Err(FunctionCallError::RespondToModel(format!(
                            "Contact '{name}' not found in namespace '{ns}'. Check {primary_path} or use CLI 'codex mail send --conversation-id …'"
                        )));
                    }
                },
                Err(err) => {
                    return Err(FunctionCallError::RespondToModel(format!(
                        "failed to load contacts: {err}"
                    )));
                }
            }
        } else {
            None
        };

        // Phase 1 behavior: only current session supported for MCP
        if let Some(id) = requested_id {
            let current_uuid = uuid::Uuid::parse_str(&session.get_conversation_id().to_string())
                .unwrap_or_else(|_| uuid::Uuid::nil());
            if id != current_uuid {
                return Err(FunctionCallError::RespondToModel(
                    "cross-session mailbox send not yet supported via MCP; use CLI 'codex mail send --conversation-id …'".to_string(),
                ));
            }
        }

        apply_mailbox_defaults(&mut message);
        validate_mailbox_message(&message)
            .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?;

        let submission_id = format!("{sub_id}/{call_id}");
        let mailbox_enabled = mailbox_feature_enabled();

        let delivery_receiver = if wait_for_delivery {
            Some(
                session
                    .register_mailbox_delivery_listener(message.message_id)
                    .await,
            )
        } else {
            None
        };

        let enqueued_event = match session
            .enqueue_mailbox_envelope(submission_id.clone(), message.clone(), mailbox_enabled)
            .await
        {
            Ok(event) => event,
            Err(err) => {
                if delivery_receiver.is_some() {
                    session
                        .cancel_mailbox_delivery_listener(message.message_id)
                        .await;
                }
                return Err(map_enqueue_error(err));
            }
        };

        let delivered_event = if let Some(receiver) = delivery_receiver {
            wait_for_delivery_event(receiver, timeout_seconds, &session, message.message_id).await?
        } else {
            None
        };

        // Normalize to CLI-aligned JSON output
        let (ack, queue_depth, correlation_id) = if let Some(delivery) = &delivered_event {
            (
                Some("delivered".to_string()),
                delivery.queue_depth.map(|q| q as u64),
                delivery.correlation_id.clone(),
            )
        } else {
            (
                Some("enqueued".to_string()),
                enqueued_event.queue_depth.map(|q| q as u64),
                enqueued_event.correlation_id.clone(),
            )
        };

        let output = MailboxSendJsonOutputNormalized {
            ok: true,
            mode: Some(
                if wait_for_delivery {
                    "wait"
                } else {
                    "enqueue_only"
                }
                .to_string(),
            ),
            message_id: message.message_id,
            request_id: message.audit.request_id.clone(),
            conversation_id: session.get_conversation_id(),
            ack,
            queue_depth,
            correlation_id,
            // MCP tools do not use a socket path; return empty for parity.
            socket_path: String::new(),
        };

        let content = serde_json::to_string_pretty(&output).map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize mailbox result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: Some(true),
        })
    }
}

fn map_enqueue_error(err: MailboxEnqueueError) -> FunctionCallError {
    let message = match err {
        MailboxEnqueueError::Disabled => {
            "Mailbox dispatcher disabled (set CODEX_MAILBOX_OOB=1 to enable)".to_string()
        }
        MailboxEnqueueError::Full { capacity } => {
            format!("Mailbox queue is full (capacity = {capacity})")
        }
        MailboxEnqueueError::Closed => "Mailbox dispatcher unavailable".to_string(),
    };
    FunctionCallError::RespondToModel(message)
}

async fn wait_for_delivery_event(
    receiver: tokio::sync::oneshot::Receiver<MailboxDeliveryEvent>,
    timeout_seconds: u64,
    session: &crate::codex::Session,
    message_id: Uuid,
) -> Result<Option<MailboxDeliveryEvent>, FunctionCallError> {
    let timeout_duration = Duration::from_secs(timeout_seconds.max(1));
    match timeout(timeout_duration, async { receiver.await }).await {
        Ok(Ok(event)) => Ok(Some(event)),
        Ok(Err(_)) => Ok(None),
        Err(_) => {
            session.cancel_mailbox_delivery_listener(message_id).await;
            Err(FunctionCallError::RespondToModel(
                "timed out waiting for mailbox delivery acknowledgement".to_string(),
            ))
        }
    }
}

impl From<&MailboxDeliveryEvent> for MailboxEventSnapshot {
    fn from(event: &MailboxDeliveryEvent) -> Self {
        let observed_at = event.observed_at.and_then(|ts| ts.format(&Rfc3339).ok());
        let ingress = event.ingress.as_ref().map(|ingress| {
            match ingress {
                MailboxDeliveryIngress::Cli => "cli",
                MailboxDeliveryIngress::Script => "script",
                MailboxDeliveryIngress::Mcp => "mcp",
                MailboxDeliveryIngress::Vscode => "vscode",
                MailboxDeliveryIngress::Api => "api",
                MailboxDeliveryIngress::Unknown => "unknown",
            }
            .to_string()
        });
        Self {
            state: match event.state {
                MailboxDeliveryState::Enqueued => "enqueued".to_string(),
                MailboxDeliveryState::Delivered => "delivered".to_string(),
            },
            queue_depth: event.queue_depth,
            observed_at,
            correlation_id: event.correlation_id.clone(),
            ingress,
            delivery_latency_ms: event.delivery_latency_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::tests::spawn_test_mailbox_session;
    use crate::tools::context::SharedTurnDiffTracker;
    use crate::tools::context::ToolInvocation;
    use crate::tools::context::ToolOutput;
    use crate::tools::context::ToolPayload;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn mailbox_send_handler_resolves_to_same_session_via_contacts() {
        let (_guard, session, turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");

        // Seed a temporary CODEX_HOME with contacts mapping to current session id
        let home = tempfile::TempDir::new().unwrap();
        unsafe {
            std::env::set_var("CODEX_HOME", home.path());
            std::env::set_var("CODEX_NAMESPACE", "codex");
        }
        let ns = "codex";
        let nsdir = home.path().join(ns);
        std::fs::create_dir_all(&nsdir).unwrap();
        let mapping = format!(
            "[contacts]\nself.session = \"{}\"\n",
            session.get_conversation_id()
        );
        std::fs::write(nsdir.join("contacts.toml"), mapping).unwrap();
        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }

        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));
        let message = serde_json::json!({
            "sender": {"id": "system.test", "role": "system"},
            "body": {"content": "mcp contacts test", "content_type": "text/plain"},
            "audit": {"request_id": "REQ-ct", "justification": "test"}
        });
        let args = serde_json::json!({
            "message": message,
            "to": "self.session",
            "timeout_seconds": 5,
            "wait_for_delivery": true
        });

        let handler = MailboxSendHandler;
        let invocation = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker,
            sub_id: "sub-contacts".to_string(),
            call_id: "call-contacts".to_string(),
            tool_name: MAILBOX_SEND_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: args.to_string(),
            },
        };

        let output = handler.handle(invocation).await.expect("tool success");
        let ToolOutput::Function { content, .. } = output else {
            panic!("expected function output")
        };
        let parsed: MailboxSendJsonOutputNormalized = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.ok, true);
        assert_eq!(parsed.ack.as_deref(), Some("delivered"));
        submission_task.abort();
    }

    #[tokio::test]
    async fn mailbox_send_handler_cross_session_rejected() {
        let (_guard, session, turn_context, _submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");
        let home = tempfile::TempDir::new().unwrap();
        unsafe {
            std::env::set_var("CODEX_HOME", home.path());
            std::env::set_var("CODEX_NAMESPACE", "codex");
        }
        let ns = "codex";
        let nsdir = home.path().join(ns);
        std::fs::create_dir_all(&nsdir).unwrap();
        let other_id = uuid::Uuid::now_v7();
        let mapping = format!("[contacts]\nother.session = \"{other_id}\"\n");
        std::fs::write(nsdir.join("contacts.toml"), mapping).unwrap();
        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }

        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));
        let message = serde_json::json!({
            "sender": {"id": "system.test", "role": "system"},
            "body": {"content": "mcp cross test", "content_type": "text/plain"},
            "audit": {"request_id": "REQ-x", "justification": "test"}
        });
        let args = serde_json::json!({
            "message": message,
            "to": "other.session",
            "timeout_seconds": 2,
            "wait_for_delivery": false
        });

        let handler = MailboxSendHandler;
        let invocation = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker,
            sub_id: "sub-x".to_string(),
            call_id: "call-x".to_string(),
            tool_name: MAILBOX_SEND_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: args.to_string(),
            },
        };

        let err = handler.handle(invocation).await.unwrap_err();
        match err {
            FunctionCallError::RespondToModel(msg) => {
                assert!(msg.contains("cross-session mailbox send not yet supported"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
    #[tokio::test]
    async fn mailbox_send_handler_enqueues_and_waits_for_delivery() {
        let (_guard, session, turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");

        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));

        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }

        let message = json!({
            "sender": {
                "id": "system.test",
                "role": "system"
            },
            "body": {
                "content": "integration test",
                "content_type": "text/plain"
            },
            "audit": {
                "request_id": "REQ-42",
                "change_ticket": "CHG-42",
                "justification": "test delivery"
            },
            "priority": "high"
        });

        let args = json!({
            "message": message,
            "timeout_seconds": 5,
            "wait_for_delivery": true
        });

        let handler = MailboxSendHandler;
        let invocation = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker,
            sub_id: "sub-1".to_string(),
            call_id: "call-1".to_string(),
            tool_name: MAILBOX_SEND_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: args.to_string(),
            },
        };

        let output = handler
            .handle(invocation)
            .await
            .expect("tool should succeed");

        let ToolOutput::Function { content, .. } = output else {
            panic!("expected function output");
        };

        let parsed: MailboxSendJsonOutputNormalized =
            serde_json::from_str(&content).expect("parse result");
        assert_eq!(parsed.ok, true);
        assert_eq!(parsed.ack.as_deref(), Some("delivered"));

        submission_task.abort();
    }
}
