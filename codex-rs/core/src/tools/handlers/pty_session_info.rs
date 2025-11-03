use async_trait::async_trait;
use serde_json::json;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

pub struct PtySessionInfoHandler;

#[async_trait]
impl ToolHandler for PtySessionInfoHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation { session, payload, .. } = invocation;

        let requested_vm = match payload {
            ToolPayload::Function { arguments } => {
                let args: serde_json::Value = serde_json::from_str(&arguments).map_err(|err| {
                    FunctionCallError::RespondToModel(format!(
                        "failed to parse pty_session_info arguments: {err}"
                    ))
                })?;
                args.get("vmId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim().to_string())
            }
            _ => None,
        };

        let guard = session.services.pty_sessions.lock().await;
        let (vm_id, session_id) = if let Some(vm) = requested_vm.as_deref() {
            let Some(state) = guard.sessions.get(vm) else {
                return Err(FunctionCallError::RespondToModel(format!(
                    "no PTY session for vmId `{vm}`"
                )));
            };
            (state.vm_id.clone(), state.session_id.clone())
        } else if let Some(default_vm) = &guard.default_vm_id {
            let Some(state) = guard.sessions.get(default_vm) else {
                return Err(FunctionCallError::RespondToModel(
                    "no default PTY session is active".to_string(),
                ));
            };
            (state.vm_id.clone(), state.session_id.clone())
        } else {
            return Err(FunctionCallError::RespondToModel(
                "no PTY session is active".to_string(),
            ));
        };

        let output = json!({ "vm_id": vm_id, "session_id": session_id });
        Ok(ToolOutput::Function {
            content: output.to_string(),
            success: Some(true),
        })
    }
}

