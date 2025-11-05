use async_trait::async_trait;
use serde_json::json;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

use super::pty_common::{ensure_session, map_vm_pty_error};

pub struct PtyExecHandler;

#[async_trait]
impl ToolHandler for PtyExecHandler {
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
                "pty_exec expects JSON function arguments".to_string(),
            ));
        };

        let args: serde_json::Value = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse pty_exec arguments: {err}"
            ))
        })?;

        let cmd = args
            .get("cmd")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FunctionCallError::RespondToModel("pty_exec requires string cmd".to_string()))?;
        let timeout_ms = args.get("timeout_ms").and_then(|v| v.as_u64());
        let ansi = args.get("ansi").and_then(|v| v.as_str());

        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

        let (client, session_id) = ensure_session(&session, &turn).await?;

        let result = client
            .pty_exec(&session_id, cmd, timeout_ms, ansi)
            .await
            .map_err(map_vm_pty_error)?;

        tracing::info!(
            target = "codex::vm_pty.tools",
            tool = "pty_exec",
            %session_id,
            cmd = %cmd,
            timeout_ms,
            ansi = ansi.unwrap_or("raw"),
            result = %result,
            "pty tool call"
        );

        // Summarize result to keep logs legible: include exit_code, duration, and stdout length.
        let stdout_len = result
            .get("stdout")
            .and_then(|v| v.as_str())
            .map(|s| s.len())
            .unwrap_or(0);
        let exit_code_val = result.get("exit_code").cloned().unwrap_or(serde_json::json!(null));
        let duration_val = result.get("duration_ms").cloned().unwrap_or(serde_json::json!(null));
        let summary = serde_json::json!({
            "exit_code": exit_code_val,
            "duration_ms": duration_val,
            "stdout_len": stdout_len,
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
