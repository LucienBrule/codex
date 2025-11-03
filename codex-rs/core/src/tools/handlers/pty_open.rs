use std::collections::BTreeMap;

use async_trait::async_trait;
use codex_protocol::models::PtyOpenToolCallParams;
use std::time::Duration;

use crate::state::PtySessionState;
use crate::codex::TurnContext;
use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;
use crate::vm_pty::VmPtyOpenRequest;
use tracing::info;

use super::pty_common::map_vm_pty_error;

pub struct PtyOpenHandler;

#[async_trait]
impl ToolHandler for PtyOpenHandler {
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
                "pty_open handler received an incompatible payload".to_string(),
            ));
        };

        let params: PtyOpenToolCallParams = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse function arguments: {err}"
            ))
        })?;

        let client = session
            .services
            .vm_pty_client
            .as_ref()
            .ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "pty_open tool is disabled; configure tools.vm_pty.socket and enable the vm-pty lane"
                        .to_string(),
                )
            })?
            .clone();

        // Resolve vmId: use param, else configured default. Allow empty for handshake.
        let vm_id_param = params.vm_id.trim();
        let vm_id = if vm_id_param.is_empty() {
            turn
                .vm_pty_default_vm_id
                .clone()
                .unwrap_or_default()
        } else {
            params.vm_id.clone()
        };

        // Attach-first: try to reattach to an existing session.
        if !vm_id.trim().is_empty() {
            if let Ok(attached) = client.pty_attach(&vm_id).await {
            let session_id = attached.session_id.clone();
                {
                    let mut guard = session.services.pty_sessions.lock().await;
                    if guard.default_vm_id.is_none() {
                        guard.default_vm_id = Some(vm_id.clone());
                    }
                    guard.sessions.insert(vm_id.clone(), PtySessionState {
                        vm_id: vm_id.clone(),
                        session_id: session_id.clone(),
                        pending_output: None,
                    });
                }
                let output = serde_json::json!({
                    "vm_id": vm_id,
                    "session_id": session_id,
                    "initial_output": "",
                    "cols": attached.cols,
                    "rows": attached.rows,
                    "attached": true,
                });
                return Ok(ToolOutput::Function { content: output.to_string(), success: Some(true) });
            }
        }

        // Build the open request with defaults.
        let mut request = build_open_request(&params, turn.as_ref());
        request.vm_id = vm_id.clone();

        // Enforce per-worker concurrency cap when configured, before creating a new VM.
        if let Some(max_vms) = turn.vm_pty_max_concurrent_per_worker {
            if max_vms > 0 {
                let current_sessions = {
                    let guard = session.services.pty_sessions.lock().await;
                    guard.sessions.len()
                };
                // Only enforce when opening a distinct VM (attach-first above would have returned).
                if current_sessions >= max_vms && !vm_id.trim().is_empty() {
                    return Err(FunctionCallError::RespondToModel(format!(
                        "vm-pty policy exceeded: at most {max_vms} VMs per worker"
                    )));
                }
            }
        }

        // Bump timeout for pty_open (slow path on cold boots) using configured timeout.
        let client_open = client.with_request_timeout(turn.vm_pty_open_timeout);
        let response = client_open.pty_open(request).await.map_err(map_vm_pty_error)?;

        let session_id = response.session_id.clone();
        let initial_output = response.initial_output.clone();
        let assigned_vm_id = response
            .vm_id
            .clone()
            .unwrap_or_else(|| vm_id.clone());
        {
            let mut guard = session.services.pty_sessions.lock().await;
            if guard.default_vm_id.is_none() {
                guard.default_vm_id = Some(assigned_vm_id.clone());
            }
            guard.sessions.insert(assigned_vm_id.clone(), PtySessionState {
                vm_id: assigned_vm_id.clone(),
                session_id: session_id.clone(),
                pending_output: if initial_output.is_empty() {
                    None
                } else {
                    Some(initial_output.clone())
                },
            });
        }

        let output = serde_json::json!({
            "vm_id": assigned_vm_id,
            "session_id": session_id,
            "initial_output": initial_output,
            "cols": response.cols,
            "rows": response.rows,
        });

        info!(target: "codex::vm_pty", vm_id = %assigned_vm_id, session_id = %session_id, "vm-pty session opened");

        Ok(ToolOutput::Function {
            content: output.to_string(),
            success: Some(true),
        })
    }
}

fn build_open_request(params: &PtyOpenToolCallParams, turn: &TurnContext) -> VmPtyOpenRequest {
    let workspace = params
        .workspace
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| turn.cwd.to_string_lossy().into_owned());

    let cwd = params
        .cwd
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| workspace.clone());

    let env = params
        .env
        .clone()
        .map(|map| map.into_iter().collect())
        .unwrap_or_else(BTreeMap::new);

    let shell = params
        .shell
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| "/bin/bash".to_string());

    let cols = params.cols.unwrap_or(80);
    let rows = params.rows.unwrap_or(24);

    let mut req = VmPtyOpenRequest::new(
        params.vm_id.clone(),
        workspace,
        cwd,
        env,
        shell,
        cols,
        rows,
    );

    // Default to nonblocking open for agent workflows; allow config override.
    if !turn.vm_pty_open_blocking {
        req.nonblocking = Some(true);
    }

    req
}
