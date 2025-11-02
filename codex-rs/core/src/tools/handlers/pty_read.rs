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

        let max_bytes = args
            .get("maxBytes")
            .or_else(|| args.get("max_bytes"))
            .and_then(|value| value.as_u64());
        let cursor = args.get("cursor").and_then(|value| value.as_u64());

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

        Ok(ToolOutput::Function {
            content: result.to_string(),
            success: Some(true),
        })
    }
}
