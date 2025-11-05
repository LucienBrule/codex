use async_trait::async_trait;
use serde_json::Value;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

use super::pty_common::{ensure_session, map_vm_pty_error, DEFAULT_COLS, DEFAULT_ROWS};

pub struct PtyResizeHandler;

#[async_trait]
impl ToolHandler for PtyResizeHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            ..
        } = invocation;

        let ToolPayload::Function { arguments } = payload else {
            return Err(FunctionCallError::RespondToModel(
                "pty_resize expects JSON function arguments".to_string(),
            ));
        };

        let args: Value = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse pty_resize arguments: {err}"
            ))
        })?;

        // Strict schema: server requires both cols and rows
        let cols = args
            .get("cols")
            .and_then(|value| value.as_u64())
            .map(|value| value.min(u16::MAX as u64) as u16)
            .ok_or_else(|| FunctionCallError::RespondToModel("pty_resize requires integer `cols`".to_string()))?;
        let rows = args
            .get("rows")
            .and_then(|value| value.as_u64())
            .map(|value| value.min(u16::MAX as u64) as u16)
            .ok_or_else(|| FunctionCallError::RespondToModel("pty_resize requires integer `rows`".to_string()))?;
        let cursor = args.get("cursor").and_then(|value| value.as_u64());

        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

        let (client, session_id) = ensure_session(&session, &turn).await?;
        let result = client
            .pty_resize(&session_id, Some(cols), Some(rows), cursor)
            .await
            .map_err(map_vm_pty_error)?;

        tracing::info!(
            target = "codex::vm_pty.tools",
            tool = "pty_resize",
            %session_id,
            cols,
            rows,
            has_cursor = cursor.is_some(),
            result = %result,
            "pty tool call"
        );

        // Summarize resize ack only.
        let ack = result.get("ack_seq").cloned().unwrap_or(serde_json::json!(null));
        let out = serde_json::json!({ "ack_seq": ack, "cols": cols, "rows": rows });
        session
            .notify_background_event(&invocation.sub_id, format!("TOOL: {} RESULT: {}", invocation.tool_name, out))
            .await;

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
