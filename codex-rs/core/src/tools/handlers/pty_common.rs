use std::collections::BTreeMap;
use std::sync::Arc;

use crate::codex::Session;
use crate::codex::TurnContext;
use crate::function_tool::FunctionCallError;
use crate::state::PtySessionState;
use crate::vm_pty::{VmPtyClient, VmPtyClientError, VmPtyOpenRequest};

pub(crate) const DEFAULT_SHELL: &str = "/bin/bash -i";
pub(crate) const DEFAULT_CWD: &str = "/src/workspace";
pub(crate) const DEFAULT_COLS: u16 = 132;
pub(crate) const DEFAULT_ROWS: u16 = 40;

pub(crate) fn map_vm_pty_error(err: VmPtyClientError) -> FunctionCallError {
    match err {
        VmPtyClientError::Server { code, message } => FunctionCallError::RespondToModel(
            format!("pty request failed ({code}): {message}"),
        ),
        VmPtyClientError::InvalidConfig { reason, .. } => FunctionCallError::RespondToModel(
            format!("vm-pty misconfiguration: {reason}"),
        ),
        VmPtyClientError::Timeout { endpoint } => FunctionCallError::RespondToModel(format!(
            "vm-pty request to {} timed out",
            endpoint.display()
        )),
        VmPtyClientError::Connect { endpoint, source } => FunctionCallError::RespondToModel(
            format!(
                "failed to connect to vm-pty daemon at {}: {source}",
                endpoint.display()
            ),
        ),
        VmPtyClientError::Io(err) => FunctionCallError::RespondToModel(format!(
            "vm-pty I/O error: {err}"
        )),
        VmPtyClientError::Serialize(err)
        | VmPtyClientError::Deserialize(err) => FunctionCallError::RespondToModel(format!(
            "vm-pty protocol error: {err}"
        )),
        VmPtyClientError::InvalidResponse(err) => FunctionCallError::RespondToModel(format!(
            "vm-pty protocol error: {err}"
        )),
        VmPtyClientError::MissingResult => FunctionCallError::RespondToModel(
            "vm-pty response missing payload".to_string(),
        ),
        VmPtyClientError::MismatchedResponseId { expected, actual } => {
            FunctionCallError::RespondToModel(format!(
                "vm-pty response id mismatch: expected {expected}, got {actual}"
            ))
        }
    }
}

pub(crate) async fn ensure_session(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
) -> Result<(Arc<VmPtyClient>, String), FunctionCallError> {
    let client = session
        .services
        .vm_pty_client
        .as_ref()
        .cloned()
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "vm-pty tool is disabled; configure tools.vm_pty.socket and enable vm_pty lane".to_string(),
            )
        })?;

    {
        let guard = session.services.pty_state.lock().await;
        if let Some(state) = guard.as_ref() {
            return Ok((client, state.session_id.clone()));
        }
    }

    let vm_id = turn
        .vm_pty_default_vm_id
        .clone()
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "vm-pty worker missing default vm id; configure vm_pty.default_vm_id".to_string(),
            )
        })?;

    let workspace = turn.cwd.to_string_lossy().into_owned();
    let request = VmPtyOpenRequest::new(
        vm_id,
        workspace,
        DEFAULT_CWD.to_string(),
        BTreeMap::new(),
        DEFAULT_SHELL.to_string(),
        DEFAULT_COLS,
        DEFAULT_ROWS,
    );

    let response = client
        .pty_open(request)
        .await
        .map_err(map_vm_pty_error)?;

    let mut guard = session.services.pty_state.lock().await;
    let pending_output = if response.initial_output.is_empty() {
        None
    } else {
        Some(response.initial_output.clone())
    };
    let session_id = response.session_id.clone();
    *guard = Some(PtySessionState {
        session_id: session_id.clone(),
        pending_output,
    });

    Ok((client, session_id))
}

pub(crate) async fn take_pending_output(session: &Arc<Session>) -> Option<String> {
    let mut guard = session.services.pty_state.lock().await;
    guard.as_mut().and_then(|state| state.pending_output.take())
}
