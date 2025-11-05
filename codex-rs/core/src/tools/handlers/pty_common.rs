use std::sync::Arc;

use crate::codex::Session;
use crate::codex::TurnContext;
use crate::function_tool::FunctionCallError;
use crate::state::{PtySessionState, PtySessions};
use crate::vm_pty::{VmPtyClient, VmPtyClientError, VmPtyOpenRequest};
use tracing::info;

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

    // If we already have a default session, reuse it.
    {
        let guard = session.services.pty_sessions.lock().await;
        if let Some(default_vm) = &guard.default_vm_id {
            if let Some(state) = guard.sessions.get(default_vm) {
                return Ok((client, state.session_id.clone()));
            }
        }
    }

    // No active session: open one, allowing handshake when vm_id is absent.
    let requested_vm_id = turn.vm_pty_default_vm_id.clone().unwrap_or_default();
    let workspace = turn.cwd.to_string_lossy().into_owned();
    let request = VmPtyOpenRequest::new(
        requested_vm_id.clone(),
        workspace,
        DEFAULT_CWD.to_string(),
        Default::default(),
        DEFAULT_SHELL.to_string(),
        DEFAULT_COLS,
        DEFAULT_ROWS,
    );

    let response = client
        .pty_open(request)
        .await
        .map_err(map_vm_pty_error)?;

    let assigned_vm_id = response
        .vm_id
        .clone()
        .unwrap_or_else(|| requested_vm_id.clone());

    let pending_output = if response.initial_output.is_empty() {
        None
    } else {
        Some(response.initial_output.clone())
    };
    let session_id = response.session_id.clone();

    {
        let mut guard = session.services.pty_sessions.lock().await;
        // Set default VM if this is the first session.
        if guard.default_vm_id.is_none() {
            guard.default_vm_id = Some(assigned_vm_id.clone());
        }
        guard.sessions.insert(
            assigned_vm_id.clone(),
            PtySessionState {
                vm_id: assigned_vm_id.clone(),
                session_id: session_id.clone(),
                pending_output,
            },
        );
    }

    info!(target: "codex::vm_pty", vm_id = %assigned_vm_id, session_id = %session_id, "opened default vm-pty session");

    Ok((client, session_id))
}

pub(crate) async fn take_pending_output(session: &Arc<Session>) -> Option<String> {
    let mut guard = session.services.pty_sessions.lock().await;
    let default_vm = guard.default_vm_id.clone();
    if let Some(vm) = default_vm {
        if let Some(state) = guard.sessions.get_mut(&vm) {
            return state.pending_output.take();
        }
    }
    None
}
