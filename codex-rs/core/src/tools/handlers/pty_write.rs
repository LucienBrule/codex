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

        let text = args
            .get("text")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "pty_write requires a string `text` argument".to_string(),
                )
            })?;

        let cursor = args.get("cursor").and_then(|value| value.as_u64());

        let (client, session_id) = ensure_session(&session, &turn).await?;
        let result = client
            .pty_write(&session_id, text, cursor)
            .await
            .map_err(map_vm_pty_error)?;

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
