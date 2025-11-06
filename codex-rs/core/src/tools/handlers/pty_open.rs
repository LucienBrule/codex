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

use super::pty_common::{map_vm_pty_error, DEFAULT_SHELL};

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

        // Resolve vmId: use only the explicit parameter; if empty, request auto-provision from server.
        // Do NOT fall back to any configured default here to avoid unintended attach-first to "default".
        let vm_id = params.vm_id.trim().to_string();

        // Emit a human-readable background log line (same channel as "thinking")
        session
            .notify_background_event(&invocation.sub_id, format!(
                "TOOL: {} REQUEST: {}",
                invocation.tool_name, arguments
            ))
            .await;

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
                session
                    .notify_background_event(
                        &invocation.sub_id,
                        format!("TOOL: {} RESULT: {}", invocation.tool_name, output),
                    )
                    .await;
                return Ok(ToolOutput::Function { content: output.to_string(), success: Some(true) });
            }
        }

        // Build the open request with defaults.
        let mut request = build_open_request(&params, turn.as_ref());
        request.vm_id = vm_id.clone();

        // Enforce per-worker concurrency cap when configured, before creating a new VM.
        // At this point, attach-first (when vm_id was provided) has already been attempted
        // and would have returned early on success. Any remaining path implies we are about
        // to spawn a new VM (auto‑provision or failed attach), so enforce purely on count.
        if let Some(max_vms) = turn.vm_pty_max_concurrent_per_worker {
            if max_vms > 0 {
                let current_sessions = {
                    let guard = session.services.pty_sessions.lock().await;
                    guard.sessions.len()
                };
                if current_sessions >= max_vms {
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
        tracing::info!(
            target = "codex::vm_pty.tools",
            tool = "pty_open",
            request = %arguments,
            requested_vm_id = %params.vm_id,
            assigned_vm_id = %assigned_vm_id,
            cols = response.cols,
            rows = response.rows,
            nonblocking = !turn.vm_pty_open_blocking,
            initial_output_len = initial_output.len(),
            "pty tool call"
        );

        // Log a concise summary to avoid flooding logs with ANSI-heavy initial_output.
        let result_summary = serde_json::json!({
            "vm_id": assigned_vm_id,
            "session_id": session_id,
            "cols": response.cols,
            "rows": response.rows,
            "initial_output_len": initial_output.len(),
        });
        session
            .notify_background_event(
                &invocation.sub_id,
                format!("TOOL: {} RESULT: {}", invocation.tool_name, result_summary),
            )
            .await;

        Ok(ToolOutput::Function {
            content: output.to_string(),
            success: Some(true),
        })
    }
}

fn build_open_request(params: &PtyOpenToolCallParams, turn: &TurnContext) -> VmPtyOpenRequest {
    fn normalize_opt(s: Option<String>) -> Option<String> {
        s.and_then(|v| {
            let t = v.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
    }

    let workspace = normalize_opt(params.workspace.clone())
        .unwrap_or_else(|| turn.cwd.to_string_lossy().into_owned());

    let cwd = normalize_opt(params.cwd.clone()).unwrap_or_else(|| workspace.clone());

    let env = params
        .env
        .clone()
        .map(|map| map.into_iter().collect())
        .unwrap_or_else(BTreeMap::new);

    let shell = normalize_opt(params.shell.clone())
        .unwrap_or_else(|| DEFAULT_SHELL.to_string());

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
