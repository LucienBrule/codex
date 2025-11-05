use async_trait::async_trait;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

use super::pty_common::{ensure_session, map_vm_pty_error};

pub struct PtyWriteHandler;

#[async_trait]
impl ToolHandler for PtyWriteHandler {
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
                "pty_write expects JSON function arguments".to_string(),
            ));
        };

        let args: serde_json::Value = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse pty_write arguments: {err}"
            ))
        })?;

        // Strict schema: require `data` (server expects `data`)
        let data = args
            .get("data")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "pty_write requires a string `data` argument".to_string(),
                )
            })?;

        let cursor = args.get("cursor").and_then(|value| value.as_u64());

        // Emit human-output log line similar to "thinking" using background events
        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

        let (client, session_id) = ensure_session(&session, &turn).await?;
        let result = client
            .pty_write(&session_id, data, cursor)
            .await
            .map_err(map_vm_pty_error)?;

        // Structured instrumentation for exec-mode debugging
        tracing::info!(
            target: "codex::vm_pty.tools",
            tool = "pty_write",
            %session_id,
            has_cursor = cursor.is_some(),
            data_len = data.len(),
            result = %result,
            "pty tool call"
        );

        // Summarize write ack only.
        let ack = result.get("ack_seq").cloned().unwrap_or(serde_json::json!(null));
        let bytes = result.get("bytes").cloned().unwrap_or(serde_json::json!(null));
        let buffer_depth = result.get("buffer_depth").cloned().unwrap_or(serde_json::json!(null));
        let summary = serde_json::json!({ "ack_seq": ack, "bytes": bytes, "buffer_depth": buffer_depth });
        session
            .notify_background_event(&invocation.sub_id, format!("TOOL: {} RESULT: {}", invocation.tool_name, summary))
            .await;

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
