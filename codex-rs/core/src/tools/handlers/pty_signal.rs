use async_trait::async_trait;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

use super::pty_common::{ensure_session, map_vm_pty_error};

const ALLOWED_SIGNALS: &[&str] = &["interrupt", "suspend", "eof", "terminate"];

pub struct PtySignalHandler;

#[async_trait]
impl ToolHandler for PtySignalHandler {
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
                "pty_signal expects JSON function arguments".to_string(),
            ));
        };

        let args: serde_json::Value = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse pty_signal arguments: {err}"
            ))
        })?;

        // Strict schema: server expects `signal`
        let send = args
            .get("signal")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "pty_signal requires a string `signal` argument".to_string(),
                )
            })?
            .to_ascii_lowercase();

        if !ALLOWED_SIGNALS.contains(&send.as_str()) {
            return Err(FunctionCallError::RespondToModel(format!(
                "unsupported pty signal `{send}`"
            )));
        }

        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

        let (client, session_id) = ensure_session(&session, &turn).await?;
        let result = client
            .pty_signal(&session_id, &send)
            .await
            .map_err(map_vm_pty_error)?;

        tracing::info!(
            target = "codex::vm_pty.tools",
            tool = "pty_signal",
            %session_id,
            signal = %send,
            result = %result,
            "pty tool call"
        );

        // Summarize signal kind only.
        let kind = result.get("kind").cloned().unwrap_or(serde_json::json!(null));
        let out = serde_json::json!({ "kind": kind });
        session
            .notify_background_event(&invocation.sub_id, format!("TOOL: {} RESULT: {}", invocation.tool_name, out))
            .await;

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
