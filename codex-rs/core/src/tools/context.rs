use crate::codex::Session;
use crate::codex::TurnContext;
use crate::tools::TELEMETRY_PREVIEW_MAX_BYTES;
use crate::tools::TELEMETRY_PREVIEW_MAX_LINES;
use crate::tools::TELEMETRY_PREVIEW_TRUNCATION_NOTICE;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_otel::otel_event_manager::OtelEventManager;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ShellToolCallParams;
use codex_protocol::protocol::FileChange;
use codex_utils_string::take_bytes_at_char_boundary;
use mcp_types::CallToolResult;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

pub type SharedTurnDiffTracker = Arc<Mutex<TurnDiffTracker>>;

#[derive(Clone)]
pub struct ToolInvocation {
    pub session: Arc<Session>,
    pub turn: Arc<TurnContext>,
    pub tracker: SharedTurnDiffTracker,
    pub sub_id: String,
    pub call_id: String,
    pub tool_name: String,
    pub payload: ToolPayload,
}

#[derive(Clone)]
pub enum ToolPayload {
    Function {
        arguments: String,
    },
    Custom {
        input: String,
    },
    LocalShell {
        params: ShellToolCallParams,
    },
    UnifiedExec {
        arguments: String,
    },
    Mcp {
        server: String,
        tool: String,
        raw_arguments: String,
    },
}

impl ToolPayload {
    pub fn log_payload(&self) -> Cow<'_, str> {
        match self {
            ToolPayload::Function { arguments } => Cow::Borrowed(arguments),
            ToolPayload::Custom { input } => Cow::Borrowed(input),
            ToolPayload::LocalShell { params } => Cow::Owned(params.command.join(" ")),
            ToolPayload::UnifiedExec { arguments } => Cow::Borrowed(arguments),
            ToolPayload::Mcp { raw_arguments, .. } => Cow::Borrowed(raw_arguments),
        }
    }
}

impl ToolInvocation {
    pub fn wait_triggers(&self) -> WaitTriggerContext {
        WaitTriggerContext {
            session: Arc::clone(&self.session),
            sub_id: self.sub_id.clone(),
            call_id: self.call_id.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ToolOutput {
    Function {
        content: String,
        success: Option<bool>,
    },
    Mcp {
        result: Result<CallToolResult, String>,
    },
}

impl ToolOutput {
    pub fn log_preview(&self) -> String {
        match self {
            ToolOutput::Function { content, .. } => telemetry_preview(content),
            ToolOutput::Mcp { result } => format!("{result:?}"),
        }
    }

    pub fn success_for_logging(&self) -> bool {
        match self {
            ToolOutput::Function { success, .. } => success.unwrap_or(true),
            ToolOutput::Mcp { result } => result.is_ok(),
        }
    }

    pub fn into_response(self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        match self {
            ToolOutput::Function { content, success } => {
                if matches!(payload, ToolPayload::Custom { .. }) {
                    ResponseInputItem::CustomToolCallOutput {
                        call_id: call_id.to_string(),
                        output: content,
                    }
                } else {
                    ResponseInputItem::FunctionCallOutput {
                        call_id: call_id.to_string(),
                        output: FunctionCallOutputPayload { content, success },
                    }
                }
            }
            ToolOutput::Mcp { result } => ResponseInputItem::McpToolCallOutput {
                call_id: call_id.to_string(),
                result,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct WaitTriggerSpec {
    pub predicate_id: String,
    pub wake_deadline: Option<OffsetDateTime>,
    pub fire_quota: u32,
    pub request_id: Option<String>,
}

impl WaitTriggerSpec {
    pub fn new(predicate_id: impl Into<String>) -> Self {
        Self {
            predicate_id: predicate_id.into(),
            wake_deadline: None,
            fire_quota: 1,
            request_id: None,
        }
    }

    pub fn with_deadline(mut self, deadline: OffsetDateTime) -> Self {
        self.wake_deadline = Some(deadline);
        self
    }

    pub fn with_fire_quota(mut self, quota: u32) -> Self {
        self.fire_quota = quota.max(1);
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct WaitTriggerCompletion {
    pub inputs: Vec<ResponseInputItem>,
    pub mailbox_message: Option<MailboxMessage>,
    pub summary: Option<String>,
}

impl WaitTriggerCompletion {
    pub fn with_inputs(mut self, inputs: Vec<ResponseInputItem>) -> Self {
        self.inputs = inputs;
        self
    }

    pub fn with_mailbox(mut self, message: MailboxMessage) -> Self {
        self.mailbox_message = Some(message);
        self
    }

    pub fn with_summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub enum WaitTriggerCancelReason {
    Explicit,
    TurnShutdown,
    Dropped,
}

#[derive(Debug, Error)]
pub enum WaitTriggerError {
    #[error("wait trigger requires an active turn")]
    InactiveTurn,
    #[error("wait trigger quota exceeded (limit {limit})")]
    QuotaExceeded { limit: usize },
    #[error("wait trigger already completed or cancelled")]
    AlreadyCompleted,
    #[error("wait trigger channel closed")]
    TriggerClosed,
    #[error("session context dropped")]
    SessionDropped,
    #[error("wait trigger not found")]
    NotFound,
}

#[derive(Clone)]
pub struct WaitTriggerHandle {
    inner: Arc<WaitTriggerHandleInner>,
}

struct WaitTriggerHandleInner {
    trigger_id: Uuid,
    session: Weak<Session>,
    completion_tx: Mutex<Option<oneshot::Sender<WaitTriggerCompletion>>>,
}

impl WaitTriggerHandle {
    pub(crate) fn new(
        session: &Arc<Session>,
        trigger_id: Uuid,
        sender: oneshot::Sender<WaitTriggerCompletion>,
    ) -> Self {
        Self {
            inner: Arc::new(WaitTriggerHandleInner {
                trigger_id,
                session: Arc::downgrade(session),
                completion_tx: Mutex::new(Some(sender)),
            }),
        }
    }

    pub fn id(&self) -> Uuid {
        self.inner.trigger_id
    }

    pub async fn complete(&self, completion: WaitTriggerCompletion) -> Result<(), WaitTriggerError> {
        let sender = {
            let mut guard = self.inner.completion_tx.lock().await;
            guard
                .take()
                .ok_or(WaitTriggerError::AlreadyCompleted)?
        };
        sender
            .send(completion)
            .map_err(|_| WaitTriggerError::TriggerClosed)
    }

    pub async fn cancel(&self) -> Result<(), WaitTriggerError> {
        let session = self
            .inner
            .session
            .upgrade()
            .ok_or(WaitTriggerError::SessionDropped)?;
        {
            let mut guard = self.inner.completion_tx.lock().await;
            guard.take();
        }
        session
            .cancel_wait_trigger(self.inner.trigger_id, WaitTriggerCancelReason::Explicit)
            .await
    }
}

#[derive(Clone)]
pub struct WaitTriggerContext {
    session: Arc<Session>,
    sub_id: String,
    call_id: String,
}

impl WaitTriggerContext {
    pub async fn schedule(
        &self,
        spec: WaitTriggerSpec,
    ) -> Result<WaitTriggerHandle, WaitTriggerError> {
        let session = Arc::clone(&self.session);
        session
            .schedule_wait_trigger(&self.sub_id, &self.call_id, spec)
            .await
    }
}

fn telemetry_preview(content: &str) -> String {
    let truncated_slice = take_bytes_at_char_boundary(content, TELEMETRY_PREVIEW_MAX_BYTES);
    let truncated_by_bytes = truncated_slice.len() < content.len();

    let mut preview = String::new();
    let mut lines_iter = truncated_slice.lines();
    for idx in 0..TELEMETRY_PREVIEW_MAX_LINES {
        match lines_iter.next() {
            Some(line) => {
                if idx > 0 {
                    preview.push('\n');
                }
                preview.push_str(line);
            }
            None => break,
        }
    }
    let truncated_by_lines = lines_iter.next().is_some();

    if !truncated_by_bytes && !truncated_by_lines {
        return content.to_string();
    }

    if preview.len() < truncated_slice.len()
        && truncated_slice
            .as_bytes()
            .get(preview.len())
            .is_some_and(|byte| *byte == b'\n')
    {
        preview.push('\n');
    }

    if !preview.is_empty() && !preview.ends_with('\n') {
        preview.push('\n');
    }
    preview.push_str(TELEMETRY_PREVIEW_TRUNCATION_NOTICE);

    preview
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn custom_tool_calls_should_roundtrip_as_custom_outputs() {
        let payload = ToolPayload::Custom {
            input: "patch".to_string(),
        };
        let response = ToolOutput::Function {
            content: "patched".to_string(),
            success: Some(true),
        }
        .into_response("call-42", &payload);

        match response {
            ResponseInputItem::CustomToolCallOutput { call_id, output } => {
                assert_eq!(call_id, "call-42");
                assert_eq!(output, "patched");
            }
            other => panic!("expected CustomToolCallOutput, got {other:?}"),
        }
    }

    #[test]
    fn function_payloads_remain_function_outputs() {
        let payload = ToolPayload::Function {
            arguments: "{}".to_string(),
        };
        let response = ToolOutput::Function {
            content: "ok".to_string(),
            success: Some(true),
        }
        .into_response("fn-1", &payload);

        match response {
            ResponseInputItem::FunctionCallOutput { call_id, output } => {
                assert_eq!(call_id, "fn-1");
                assert_eq!(output.content, "ok");
                assert_eq!(output.success, Some(true));
            }
            other => panic!("expected FunctionCallOutput, got {other:?}"),
        }
    }

    #[test]
    fn wait_trigger_spec_defaults() {
        let spec = WaitTriggerSpec::new("timer");
        assert_eq!(spec.predicate_id, "timer");
        assert_eq!(spec.fire_quota, 1);
        assert!(spec.wake_deadline.is_none());
        assert!(spec.request_id.is_none());
    }

    #[test]
    fn telemetry_preview_returns_original_within_limits() {
        let content = "short output";
        assert_eq!(telemetry_preview(content), content);
    }

    #[test]
    fn telemetry_preview_truncates_by_bytes() {
        let content = "x".repeat(TELEMETRY_PREVIEW_MAX_BYTES + 8);
        let preview = telemetry_preview(&content);

        assert!(preview.contains(TELEMETRY_PREVIEW_TRUNCATION_NOTICE));
        assert!(
            preview.len()
                <= TELEMETRY_PREVIEW_MAX_BYTES + TELEMETRY_PREVIEW_TRUNCATION_NOTICE.len() + 1
        );
    }

    #[test]
    fn telemetry_preview_truncates_by_lines() {
        let content = (0..(TELEMETRY_PREVIEW_MAX_LINES + 5))
            .map(|idx| format!("line {idx}"))
            .collect::<Vec<_>>()
            .join("\n");

        let preview = telemetry_preview(&content);
        let lines: Vec<&str> = preview.lines().collect();

        assert!(lines.len() <= TELEMETRY_PREVIEW_MAX_LINES + 1);
        assert_eq!(lines.last(), Some(&TELEMETRY_PREVIEW_TRUNCATION_NOTICE));
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExecCommandContext {
    pub(crate) sub_id: String,
    pub(crate) call_id: String,
    pub(crate) command_for_display: Vec<String>,
    pub(crate) cwd: PathBuf,
    pub(crate) apply_patch: Option<ApplyPatchCommandContext>,
    pub(crate) tool_name: String,
    pub(crate) otel_event_manager: OtelEventManager,
}

#[derive(Clone, Debug)]
pub(crate) struct ApplyPatchCommandContext {
    pub(crate) user_explicitly_approved_this_action: bool,
    pub(crate) changes: HashMap<PathBuf, FileChange>,
}
