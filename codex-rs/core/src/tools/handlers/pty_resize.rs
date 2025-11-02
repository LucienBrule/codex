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

        let cols = args
            .get("cols")
            .and_then(|value| value.as_u64())
            .map(|value| value.min(u16::MAX as u64) as u16)
            .unwrap_or(DEFAULT_COLS);
        let rows = args
            .get("rows")
            .and_then(|value| value.as_u64())
            .map(|value| value.min(u16::MAX as u64) as u16)
            .unwrap_or(DEFAULT_ROWS);
        let cursor = args.get("cursor").and_then(|value| value.as_u64());

        let (client, session_id) = ensure_session(&session, &turn).await?;
        let result = client
            .pty_resize(&session_id, Some(cols), Some(rows), cursor)
            .await
            .map_err(map_vm_pty_error)?;

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
