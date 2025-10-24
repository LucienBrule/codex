use std::time::Duration;

use async_trait::async_trait;
use codex_protocol::mailbox::MailboxAckMode;
use codex_protocol::mailbox::MailboxAudience;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryIngress;
use codex_protocol::protocol::MailboxDeliveryState;
use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::time::timeout;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use crate::codex::MailboxEnqueueError;
use crate::function_tool::FunctionCallError;
use crate::mailbox::ROUTING_MODE_METADATA_KEY;
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
pub const MAILBOX_SEND_STORY_TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../mcp/prompts/mailbox_send_story.json"
));

pub struct MailboxSendHandler;

#[derive(Debug, Deserialize)]
struct MailboxSendArgs {
    #[serde(default)]
    message: MailboxMessage,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    ack_mode: Option<String>,
    #[serde(default)]
    ack_deadline: Option<String>,
    #[serde(default)]
    ack_auto_seconds: Option<u64>,
    #[serde(default)]
    ack_escalation_ticket: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    ack_mode: Option<String>,
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
            mut to,
            mut conversation_id,
            ack_mode,
            ack_deadline,
            ack_auto_seconds,
            ack_escalation_ticket,
            timeout_seconds,
            wait_for_delivery,
        } = args;

        to = to.and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        conversation_id = conversation_id.and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

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

        if to.is_none() && conversation_id.is_none() {
            warn!(
                tool = MAILBOX_SEND_TOOL_NAME,
                "mailbox_send missing routing target: set 'to' or 'conversation_id'"
            );
            return Err(FunctionCallError::RespondToModel(
                "mailbox_send requires either 'to' or 'conversation_id' to route the message"
                    .to_string(),
            ));
        }

        let session_uuid = Uuid::parse_str(&session.get_conversation_id().to_string())
            .unwrap_or_else(|_| Uuid::nil());

        let target_conversation_uuid = match requested_id {
            Some(target_id) if target_id == session_uuid => {
                warn!(
                    tool = MAILBOX_SEND_TOOL_NAME,
                    %target_id,
                    %session_uuid,
                    "mailbox_send attempted self-loop; refusing to enqueue"
                );
                return Err(FunctionCallError::RespondToModel(
                    "mailbox_send cannot target the current session; choose a different recipient"
                        .to_string(),
                ));
            }
            Some(target_id) => {
                apply_conversation_audience(&mut message, target_id);
                target_id
            }
            None => {
                warn!(
                    tool = MAILBOX_SEND_TOOL_NAME,
                    "mailbox_send missing routing target after resolution"
                );
                return Err(FunctionCallError::RespondToModel(
                    "mailbox_send requires either 'to' or 'conversation_id' to route the message"
                        .to_string(),
                ));
            }
        };

        let routing_mode = if to.is_some() {
            "contact"
        } else {
            "conversation_id"
        };

        if message
            .metadata
            .get(ROUTING_MODE_METADATA_KEY)
            .and_then(|value| value.as_str())
            .is_none()
        {
            message.metadata.insert(
                ROUTING_MODE_METADATA_KEY.to_string(),
                serde_json::Value::String(routing_mode.to_string()),
            );
        }
        info!(
            target: "codex::mailbox",
            tool = MAILBOX_SEND_TOOL_NAME,
            routing_mode,
            to = to.as_deref(),
            %target_conversation_uuid,
            message_id = %message.message_id,
            request_id = message.audit.request_id.as_deref().unwrap_or(""),
            wait_for_delivery,
            "mailbox_send routing resolved"
        );

        apply_ack_overrides(
            &mut message,
            ack_mode.as_deref(),
            ack_deadline.as_deref(),
            ack_auto_seconds,
            ack_escalation_ticket.as_deref(),
        )?;

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
            ack_mode: Some(ack_mode_label(&message.ack_policy.mode).to_string()),
            message_id: message.message_id,
            request_id: message.audit.request_id.clone(),
            conversation_id: codex_protocol::ConversationId::from_string(
                &target_conversation_uuid.to_string(),
            )
            .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?,
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

fn apply_conversation_audience(message: &mut MailboxMessage, conversation_id: Uuid) {
    match &mut message.audience {
        Some(audience) => {
            if audience.conversation_id.is_none() {
                audience.conversation_id = Some(conversation_id);
            }
        }
        None => {
            message.audience = Some(MailboxAudience {
                conversation_id: Some(conversation_id),
                worker_id: None,
                scopes: None,
                allow_broadcast: false,
            });
        }
    }
}

fn apply_ack_overrides(
    message: &mut MailboxMessage,
    ack_mode: Option<&str>,
    ack_deadline: Option<&str>,
    ack_auto_seconds: Option<u64>,
    ack_escalation_ticket: Option<&str>,
) -> Result<(), FunctionCallError> {
    if let Some(mode_str) = ack_mode {
        let mode = parse_ack_mode(mode_str).map_err(FunctionCallError::RespondToModel)?;
        message.ack_policy.mode = mode;
    }

    if let Some(deadline_str) = ack_deadline {
        let deadline = OffsetDateTime::parse(deadline_str, &Rfc3339)
            .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?;
        message.ack_policy.deadline = Some(deadline);
    }

    if let Some(auto_secs) = ack_auto_seconds {
        message.ack_policy.auto_ack_seconds = Some(auto_secs);
    }

    if let Some(ticket) = ack_escalation_ticket {
        let trimmed = ticket.trim();
        message.ack_policy.escalation_ticket = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }

    Ok(())
}

fn parse_ack_mode(raw: &str) -> Result<MailboxAckMode, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(MailboxAckMode::None),
        "passive" => Ok(MailboxAckMode::Passive),
        "required" => Ok(MailboxAckMode::Required),
        other => Err(format!("invalid ack_mode value: {other}")),
    }
}

fn ack_mode_label(mode: &MailboxAckMode) -> &'static str {
    match mode {
        MailboxAckMode::None => "none",
        MailboxAckMode::Passive => "passive",
        MailboxAckMode::Required => "required",
    }
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
    use serde_json::Value;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct EnvVarGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set_path(key: &'static str, value: &std::path::Path) -> Self {
            let original = std::env::var_os(key);
            unsafe { std::env::set_var(key, value) };
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.original {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            };
        }
    }

    #[test]
    fn story_template_matches_mailbox_schema() {
        let template: Value =
            serde_json::from_str(MAILBOX_SEND_STORY_TEMPLATE).expect("template JSON must parse");
        let tool_name = template
            .get("tool_name")
            .and_then(Value::as_str)
            .expect("template must set tool_name");
        assert_eq!(tool_name, MAILBOX_SEND_TOOL_NAME);

        let arguments = template
            .get("arguments")
            .cloned()
            .expect("template must include arguments");
        let arguments_json =
            serde_json::to_string(&arguments).expect("arguments must be serializable");
        let parsed: MailboxSendArgs =
            serde_json::from_str(&arguments_json).expect("arguments must match Mailbox schema");

        assert!(
            parsed.to.is_some() || parsed.conversation_id.is_some(),
            "template must set routing information",
        );
        assert!(
            parsed
                .message
                .body
                .subject
                .as_ref()
                .is_some_and(|subject| !subject.trim().is_empty()),
            "template must set a non-empty subject",
        );
        assert!(
            !parsed.message.body.content.trim().is_empty(),
            "template must set non-empty content",
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn mailbox_send_handler_rejects_self_routing_via_contacts() {
        let (_guard, session, turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");

        // Seed a temporary contacts file mapping to the current session id
        let contacts_dir = tempfile::TempDir::new().unwrap();
        let contacts_path = contacts_dir.path().join("contacts.toml");
        let mapping = format!(
            "[contacts]\nself.session = \"{}\"\n",
            session.get_conversation_id()
        );
        std::fs::write(&contacts_path, mapping).unwrap();
        let _contacts_guard = EnvVarGuard::set_path("CODEX_CONTACTS_FILE", &contacts_path);
        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }
        let loaded_contacts =
            crate::contacts::load_contacts(std::path::Path::new("."), "codex").unwrap();
        assert!(loaded_contacts.resolve("self.session").is_some());

        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));
        let message = serde_json::json!({
            "sender": {"id": "system.test", "role": "system"},
            "body": {"subject": "Contacts", "content": "mcp contacts test", "content_type": "text/plain"},
            "audit": {"request_id": "REQ-ct", "justification": "test"}
        });
        let args = serde_json::json!({
            "message": message,
            "to": "self.session",
            "timeout_seconds": 5,
            "wait_for_delivery": false
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

        let err = handler
            .handle(invocation)
            .await
            .expect_err("expected self-loop rejection");
        match err {
            FunctionCallError::RespondToModel(message) => {
                assert!(message.contains("cannot target the current session"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
        submission_task.abort();
    }

    #[tokio::test]
    async fn mailbox_send_handler_requires_explicit_routing() {
        let (_guard, session, turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");

        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }

        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));
        let message = serde_json::json!({
            "sender": {"id": "system.test", "role": "system"},
            "body": {"subject": "Missing routing", "content": "mcp routing test", "content_type": "text/plain"},
            "audit": {"request_id": "REQ-route", "justification": "test"}
        });
        let args = serde_json::json!({
            "message": message,
            "timeout_seconds": 5,
            "wait_for_delivery": false
        });

        let handler = MailboxSendHandler;
        let invocation = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker,
            sub_id: "sub-route".to_string(),
            call_id: "call-route".to_string(),
            tool_name: MAILBOX_SEND_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: args.to_string(),
            },
        };

        let err = handler
            .handle(invocation)
            .await
            .expect_err("expected routing validation error");
        match err {
            FunctionCallError::RespondToModel(message) => {
                assert!(message.contains("requires either 'to' or 'conversation_id'"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
        submission_task.abort();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn mailbox_send_handler_cross_session_alias_allowed() {
        let (_guard, session, turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");
        let contacts_dir = tempfile::TempDir::new().unwrap();
        let contacts_path = contacts_dir.path().join("contacts.toml");
        let other_id = uuid::Uuid::now_v7();
        let mapping = format!("[contacts]\nother.session = \"{other_id}\"\n");
        std::fs::write(&contacts_path, mapping).unwrap();
        let _contacts_guard = EnvVarGuard::set_path("CODEX_CONTACTS_FILE", &contacts_path);
        unsafe {
            std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
            std::env::set_var("CODEX_MAILBOX_OOB", "1");
        }
        let loaded_contacts =
            crate::contacts::load_contacts(std::path::Path::new("."), "codex").unwrap();
        assert!(loaded_contacts.resolve("other.session").is_some());

        let tracker: SharedTurnDiffTracker = Arc::new(Mutex::new(TurnDiffTracker::new()));
        let message = serde_json::json!({
            "sender": {"id": "system.test", "role": "system"},
            "body": {"subject": "Cross session", "content": "mcp cross test", "content_type": "text/plain"},
            "audit": {"request_id": "REQ-x", "justification": "test"}
        });
        let args = serde_json::json!({
            "message": message,
            "to": "other.session",
            "timeout_seconds": 5,
            "wait_for_delivery": true
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

        let output = handler.handle(invocation).await.expect("tool success");
        let ToolOutput::Function { content, .. } = output else {
            panic!("expected function output")
        };
        let parsed: MailboxSendJsonOutputNormalized = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.ok, true);
        assert_eq!(parsed.ack.as_deref(), Some("delivered"));
        assert_eq!(parsed.ack_mode.as_deref(), Some("none"));
        assert_eq!(parsed.mode.as_deref(), Some("wait"));
        assert_eq!(parsed.conversation_id.to_string(), other_id.to_string());
        submission_task.abort();
    }
    #[tokio::test]
    async fn mailbox_send_handler_enqueues_without_waiting() {
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
                "subject": "Delivery",
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

        let target_id = uuid::Uuid::now_v7();
        let args = json!({
            "message": message,
            "conversation_id": target_id.to_string(),
            "timeout_seconds": 5,
            "wait_for_delivery": false
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
        assert_eq!(parsed.ack.as_deref(), Some("enqueued"));
        assert_eq!(parsed.mode.as_deref(), Some("enqueue_only"));
        assert_eq!(parsed.conversation_id.to_string(), target_id.to_string());

        submission_task.abort();
    }

    #[tokio::test]
    async fn mailbox_send_handler_applies_ack_overrides() {
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
                "subject": "Ack overrides",
                "content": "ack override",
                "content_type": "text/plain"
            },
            "audit": {
                "request_id": "REQ-ack",
                "justification": "test"
            },
            "priority": "normal"
        });

        let target_id = uuid::Uuid::now_v7();
        let args = json!({
            "message": message,
            "ack_mode": "required",
            "ack_deadline": "2025-10-23T12:00:00Z",
            "conversation_id": target_id.to_string(),
            "wait_for_delivery": false
        });

        let handler = MailboxSendHandler;
        let invocation = ToolInvocation {
            session: Arc::clone(&session),
            turn: Arc::clone(&turn_context),
            tracker,
            sub_id: "sub-ack".to_string(),
            call_id: "call-ack".to_string(),
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
        assert_eq!(parsed.ack.as_deref(), Some("enqueued"));
        assert_eq!(parsed.ack_mode.as_deref(), Some("required"));
        assert_eq!(parsed.conversation_id.to_string(), target_id.to_string());
        submission_task.abort();
    }
}
