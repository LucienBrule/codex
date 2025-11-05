use async_trait::async_trait;
use codex_protocol::protocol::MailboxDeliveryIngress;
use codex_protocol::protocol::MailboxDeliveryState;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::handlers::wait::WaitHandler;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

pub const MAILBOX_WAIT_TOOL_NAME: &str = "mailbox_wait";
const DEFAULT_MAILBOX_WAIT_TIMEOUT_MS: u64 = 30_000;

pub struct MailboxWaitHandler;

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct MailboxWaitHelperArgs {
    expected_subject: Option<String>,
    subject_contains: Option<String>,
    case_sensitive: Option<bool>,
    from_handle: Option<String>,
    sender_id: Option<String>,
    request_id: Option<String>,
    message_id: Option<Uuid>,
    states: Option<Vec<MailboxDeliveryState>>,
    ingress: Option<Vec<MailboxDeliveryIngress>>,
    timeout_ms: Option<u64>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Serialize, Default)]
#[serde(default)]
struct MailboxPredicatePayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject_contains: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    case_sensitive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from_handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    states: Option<Vec<MailboxDeliveryState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress: Option<Vec<MailboxDeliveryIngress>>,
}

#[async_trait]
impl ToolHandler for MailboxWaitHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            tracker,
            sub_id,
            call_id,
            tool_name: _,
            payload,
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "mailbox_wait helper requires function payload".to_string(),
                ));
            }
        };

        let helper_args: MailboxWaitHelperArgs =
            serde_json::from_str(&arguments).map_err(|err| {
                FunctionCallError::RespondToModel(format!(
                    "failed to parse mailbox_wait arguments: {err}"
                ))
            })?;

        let timeout_ms = helper_args
            .timeout_ms
            .or_else(|| {
                helper_args
                    .timeout_seconds
                    .map(|secs| secs.saturating_mul(1_000))
            })
            .unwrap_or(DEFAULT_MAILBOX_WAIT_TIMEOUT_MS);

        if timeout_ms == 0 {
            return Err(FunctionCallError::RespondToModel(
                "timeout_ms must be greater than zero".to_string(),
            ));
        }

        let predicate_payload = MailboxPredicatePayload {
            expected_subject: normalize_string(helper_args.expected_subject),
            subject_contains: normalize_string(helper_args.subject_contains),
            case_sensitive: helper_args.case_sensitive,
            from_handle: normalize_string(helper_args.from_handle),
            sender_id: normalize_string(helper_args.sender_id),
            request_id: normalize_string(helper_args.request_id),
            message_id: helper_args.message_id,
            states: helper_args.states,
            ingress: helper_args.ingress,
        };

        let wait_arguments = json!({
            "type": "mailbox",
            "predicate": predicate_payload,
            "timeout_ms": timeout_ms,
        })
        .to_string();

        let wait_invocation = ToolInvocation {
            session,
            turn,
            tracker,
            sub_id,
            call_id,
            tool_name: MAILBOX_WAIT_TOOL_NAME.to_string(),
            payload: ToolPayload::Function {
                arguments: wait_arguments,
            },
        };

        let wait_handler = WaitHandler;
        wait_handler.handle(wait_invocation).await
    }
}

fn normalize_string(input: Option<String>) -> Option<String> {
    input.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}
