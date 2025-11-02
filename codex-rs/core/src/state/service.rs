use crate::RolloutRecorder;
use crate::exec_command::ExecSessionManager;
use crate::executor::Executor;
use crate::mcp_connection_manager::McpConnectionManager;
use crate::unified_exec::UnifiedExecSessionManager;
use crate::user_notification::UserNotifier;
use crate::vm_pty::VmPtyClient;
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
    pub(crate) pty_state: Mutex<Option<PtySessionState>>,
}

#[derive(Debug, Clone)]
pub(crate) struct PtySessionState {
    pub session_id: String,
    pub pending_output: Option<String>,
}
