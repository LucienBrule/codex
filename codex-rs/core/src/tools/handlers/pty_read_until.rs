use async_trait::async_trait;
use std::time::Duration;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

use super::pty_common::{ensure_session, map_vm_pty_error};

pub struct PtyReadUntilHandler;

#[async_trait]
impl ToolHandler for PtyReadUntilHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation { session, turn, payload, .. } = invocation;

        let ToolPayload::Function { arguments } = payload else {
            return Err(FunctionCallError::RespondToModel(
                "pty_read_until expects JSON function arguments".to_string(),
            ));
        };

        let args: serde_json::Value = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse pty_read_until arguments: {err}"
            ))
        })?;

        let pattern = args
            .get("pattern")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FunctionCallError::RespondToModel("pty_read_until requires string pattern".to_string()))?;
        let timeout_ms = args.get("timeout_ms").and_then(|v| v.as_u64());
        let ansi = args.get("ansi").and_then(|v| v.as_str());

        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

        let (client, session_id) = ensure_session(&session, &turn).await?;

        // First attempt
        let mut result = client
            .pty_read_until(&session_id, pattern, timeout_ms, ansi)
            .await
            .map_err(map_vm_pty_error)?;

        // Minimal, bounded retry on timeout/unmatched to reduce flakiness
        let timed_out = result
            .get("timed_out")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let matched = result
            .get("matched")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if timed_out || !matched {
            // Emit a concise retry message for QA visibility
            session
                .notify_background_event(
                    &invocation.sub_id,
                    format!(
                        "[harness] pty_read_until: timeout/unmatched; retrying once in 500ms (pattern='{}')...",
                        pattern
                    ),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            result = client
                .pty_read_until(&session_id, pattern, timeout_ms, ansi)
                .await
                .map_err(map_vm_pty_error)?;
        }

        tracing::info!(
            target = "codex::vm_pty.tools",
            tool = "pty_read_until",
            %session_id,
            pattern = %pattern,
            timeout_ms,
            ansi = ansi.unwrap_or("raw"),
            result = %result,
            "pty tool call"
        );

        // Summarize result: matched flag and data length only.
        let matched = result.get("matched").cloned().unwrap_or(serde_json::json!(null));
        let data_len = result
            .get("data")
            .and_then(|v| v.as_str())
            .map(|s| s.len())
            .unwrap_or(0);
        let summary = serde_json::json!({ "matched": matched, "data_len": data_len });
        session
            .notify_background_event(
                &invocation.sub_id,
                format!("TOOL: {} RESULT: {}", invocation.tool_name, summary),
            )
            .await;

        Ok(ToolOutput::Function { content: result.to_string(), success: Some(true) })
    }
}
