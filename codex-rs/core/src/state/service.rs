use crate::RolloutRecorder;
use crate::exec_command::ExecSessionManager;
use crate::executor::Executor;
use crate::mcp_connection_manager::McpConnectionManager;
use crate::unified_exec::UnifiedExecSessionManager;
use crate::user_notification::UserNotifier;
use crate::vm_pty::VmPtyClient;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::summaries::SummariesService;

pub(crate) struct SessionServices {
    pub(crate) mcp_connection_manager: McpConnectionManager,
    pub(crate) session_manager: ExecSessionManager,
    pub(crate) unified_exec_manager: UnifiedExecSessionManager,
    pub(crate) notifier: UserNotifier,
    pub(crate) rollout: Mutex<Option<RolloutRecorder>>,
    pub(crate) user_shell: crate::shell::Shell,
    pub(crate) show_raw_agent_reasoning: bool,
    pub(crate) executor: Executor,
    pub(crate) summaries: Mutex<Option<SummariesService>>,
    pub(crate) vm_pty_client: Option<Arc<VmPtyClient>>,
    pub(crate) pty_sessions: Mutex<PtySessions>,
}

#[derive(Debug, Clone)]
pub(crate) struct PtySessionState {
    pub vm_id: String,
    pub session_id: String,
    pub pending_output: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct PtySessions {
    /// The vm_id for the default PTY session used by pty_read/write/resize/signal.
    pub default_vm_id: Option<String>,
    /// Map of vm_id -> session state for the VM PTY sessions opened this turn/session.
    pub sessions: BTreeMap<String, PtySessionState>,
}
