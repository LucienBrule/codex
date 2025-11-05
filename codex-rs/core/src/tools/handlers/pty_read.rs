use async_trait::async_trait;
use serde_json::json;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

use super::pty_common::{ensure_session, map_vm_pty_error, take_pending_output};

pub struct PtyReadHandler;

#[async_trait]
impl ToolHandler for PtyReadHandler {
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
                "pty_read expects JSON function arguments".to_string(),
            ));
        };

        let args: serde_json::Value = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse pty_read arguments: {err}"
            ))
        })?;

        // Strict schema: server expects `max_bytes`; omit to use server default
        let max_bytes = args.get("max_bytes").and_then(|value| value.as_u64());
        let cursor = args.get("cursor").and_then(|value| value.as_u64());

        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

        let (client, session_id) = ensure_session(&session, &turn).await?;

        if let Some(pending) = take_pending_output(&session).await {
            let seq = pending.as_bytes().len() as u64;
            let response = json!({
                "seq": seq,
                "data": pending,
                "eof": false,
                "buffer_depth": 0,
            });
            return Ok(ToolOutput::Function {
                content: response.to_string(),
                success: Some(true),
            });
        }

        let result = client
            .pty_read(&session_id, max_bytes, cursor)
            .await
            .map_err(map_vm_pty_error)?;

        tracing::info!(
            target = "codex::vm_pty.tools",
            tool = "pty_read",
            %session_id,
            max_bytes,
            has_cursor = cursor.is_some(),
            result = %result,
            "pty tool call"
        );

        // Summarize read: seq, eof, bytes delivered, buffer depth.
        let seq = result.get("seq").cloned().unwrap_or(serde_json::json!(null));
        let eof = result.get("eof").cloned().unwrap_or(serde_json::json!(null));
        let data_len = result
            .get("data")
            .and_then(|v| v.as_str())
            .map(|s| s.len())
            .unwrap_or(0);
        let buffer_depth = result
            .get("buffer_depth")
            .cloned()
            .unwrap_or(serde_json::json!(null));
        let summary = serde_json::json!({
            "seq": seq,
            "eof": eof,
            "data_len": data_len,
            "buffer_depth": buffer_depth,
        });
        session
            .notify_background_event(&invocation.sub_id, format!("TOOL: {} RESULT: {}", invocation.tool_name, summary))
            .await;

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
