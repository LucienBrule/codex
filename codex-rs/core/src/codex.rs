use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::time::Instant;

use crate::AuthManager;
use crate::client_common::REVIEW_PROMPT;
use crate::event_mapping::map_response_item_to_event_messages;
use crate::function_tool::FunctionCallError;
use crate::review_format::format_review_findings_block;
use crate::terminal;
use crate::user_notification::UserNotifier;
use async_channel::Receiver;
use async_channel::Sender;
use codex_apply_patch::ApplyPatchAction;
use codex_protocol::ConversationId;
use codex_protocol::mailbox::MailboxContentType;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxSenderRole;
use codex_protocol::protocol::ConversationPathResponseEvent;
use codex_protocol::protocol::ExitedReviewModeEvent;
use codex_protocol::protocol::HeartbeatEvent;
use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryState;
use codex_protocol::protocol::ReviewRequest;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TaskStartedEvent;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnContextItem;
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesOrdered;
use mcp_types::CallToolResult;
use serde_json;
use serde_json::Value;
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;
use uuid::Uuid;

use crate::ModelProviderInfo;
use crate::apply_patch::convert_apply_patch_to_protocol;
use crate::client::ModelClient;
use crate::client_common::Prompt;
use crate::client_common::ResponseEvent;
use crate::config::Config;
use crate::config_types::ShellEnvironmentPolicy;
use crate::conversation_history::ConversationHistory;
use crate::environment_context::EnvironmentContext;
use crate::error::CodexErr;
use crate::error::Result as CodexResult;
use crate::exec::ExecToolCallOutput;
#[cfg(test)]
use crate::exec::StreamOutput;
use crate::exec_command::ExecCommandParams;
use crate::exec_command::ExecSessionManager;
use crate::exec_command::WriteStdinParams;
use crate::executor::Executor;
use crate::executor::ExecutorConfig;
use crate::executor::normalize_exec_result;
use crate::mailbox::MAILBOX_QUEUE_CAPACITY;
use crate::mailbox::MailboxEnvelope;
use crate::mailbox::MailboxReceiver;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxWaitFilter;
use crate::mailbox::ROUTING_MODE_METADATA_KEY;
use crate::mailbox::TryEnqueueError;
use crate::mailbox::apply_mailbox_defaults;
use crate::mailbox::mailbox_channel;
use crate::mailbox::mailbox_feature_enabled;
use crate::mailbox::validate_mailbox_message;
use crate::mailbox_dispatcher::DispatchStatus;
use crate::mailbox_dispatcher::MailboxDispatcherClient;
use crate::mcp::auth::compute_auth_statuses;
use crate::mcp_connection_manager::McpConnectionManager;
use crate::model_family::find_family_for_model;
use crate::openai_model_info::get_model_info;
use crate::openai_tools::ToolsConfig;
use crate::openai_tools::ToolsConfigParams;
use crate::parse_command::parse_command;
use crate::project_doc::get_user_instructions;
use crate::protocol::AgentMessageDeltaEvent;
use crate::protocol::AgentReasoningDeltaEvent;
use crate::protocol::AgentReasoningRawContentDeltaEvent;
use crate::protocol::AgentReasoningSectionBreakEvent;
use crate::protocol::ApplyPatchApprovalRequestEvent;
use crate::protocol::AskForApproval;
use crate::protocol::BackgroundEventEvent;
use crate::protocol::ErrorEvent;
use crate::protocol::Event;
use crate::protocol::EventMsg;
use crate::protocol::ExecApprovalRequestEvent;
use crate::protocol::ExecCommandBeginEvent;
use crate::protocol::ExecCommandEndEvent;
use crate::protocol::InputItem;
use crate::protocol::ListCustomPromptsResponseEvent;
use crate::protocol::Op;
use crate::protocol::PatchApplyBeginEvent;
use crate::protocol::PatchApplyEndEvent;
use crate::protocol::RateLimitSnapshot;
use crate::protocol::ReviewDecision;
use crate::protocol::ReviewOutputEvent;
use crate::protocol::SandboxPolicy;
use crate::protocol::SessionConfiguredEvent;
use crate::protocol::StreamErrorEvent;
use crate::protocol::Submission;
use crate::protocol::TokenCountEvent;
use crate::protocol::TokenUsage;
use crate::protocol::TurnDiffEvent;
use crate::protocol::WebSearchBeginEvent;
use crate::rollout::RolloutRecorder;
use crate::rollout::RolloutRecorderParams;
use crate::shell;
use crate::state::ActiveTurn;
use crate::state::SessionServices;
use crate::summaries;
use crate::summaries::SummariesState as SlidingSummariesState;
use crate::summaries::build_prompt as build_sliding_window_prompt;
use crate::tasks::CompactTask;
use crate::tasks::RegularTask;
use crate::tasks::ReviewTask;
use crate::telemetry::MailboxDeliveryTelemetry;
use crate::telemetry::MailboxLivenessSnapshot;
use crate::telemetry::MailboxLivenessTelemetry;
use crate::telemetry::MailboxTelemetryLabels;
use crate::tools::ToolRouter;
use crate::tools::context::SharedTurnDiffTracker;
use crate::tools::context::WaitTriggerCancelReason;
use crate::tools::context::WaitTriggerCompletion;
use crate::tools::context::WaitTriggerError;
use crate::tools::context::WaitTriggerHandle;
use crate::tools::context::WaitTriggerSpec;
use crate::tools::format_exec_output_str;
use crate::tools::names::WAIT_WITH_PREDICATE_TOOL_NAME;
use crate::tools::parallel::ToolCallRuntime;
use crate::turn_diff_tracker::TurnDiffTracker;
use crate::unified_exec::UnifiedExecSessionManager;
use crate::user_instructions::UserInstructions;
use crate::user_notification::UserNotification;
use crate::util::backoff;
use crate::vm_pty::VmPtyClient;
use codex_otel::otel_event_manager::OtelEventManager;
use codex_protocol::config_types::ReasoningEffort as ReasoningEffortConfig;
use codex_protocol::config_types::ReasoningSummary as ReasoningSummaryConfig;
use codex_protocol::custom_prompts::CustomPrompt;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::InitialHistory;
use codex_protocol::protocol::MailboxDeliveryIngress;
use time::OffsetDateTime;

pub mod compact;
use self::compact::build_compacted_history;
use self::compact::collect_user_messages;

/// The high-level interface to the Codex system.
/// It operates as a queue pair where you send submissions and receive events.
pub struct Codex {
    next_id: AtomicU64,
    tx_sub: Sender<Submission>,
    rx_event: Receiver<Event>,
}

/// Wrapper returned by [`Codex::spawn`] containing the spawned [`Codex`],
/// the submission id for the initial `ConfigureSession` request and the
/// unique session id.
pub struct CodexSpawnOk {
    pub codex: Codex,
    pub conversation_id: ConversationId,
}

pub(crate) const INITIAL_SUBMIT_ID: &str = "";
pub(crate) const SUBMISSION_CHANNEL_CAPACITY: usize = 64;
pub(crate) const MAX_WAIT_TRIGGERS_PER_TURN: usize = 16;

impl Codex {
    /// Spawn a new [`Codex`] and initialize the session.
    pub async fn spawn(
        config: Config,
        auth_manager: Arc<AuthManager>,
        conversation_history: InitialHistory,
        session_source: SessionSource,
    ) -> CodexResult<CodexSpawnOk> {
        let (tx_sub, rx_sub) = async_channel::bounded(SUBMISSION_CHANNEL_CAPACITY);
        let (tx_event, rx_event) = async_channel::unbounded();
        let (mailbox_tx, mailbox_rx) = mailbox_channel(MAILBOX_QUEUE_CAPACITY);

        let user_instructions = get_user_instructions(&config).await;

        let config = Arc::new(config);

        let configure_session = ConfigureSession {
            provider: config.model_provider.clone(),
            model: config.model.clone(),
            model_reasoning_effort: config.model_reasoning_effort,
            model_reasoning_summary: config.model_reasoning_summary,
            user_instructions,
            base_instructions: config.base_instructions.clone(),
            approval_policy: config.approval_policy,
            sandbox_policy: config.sandbox_policy.clone(),
            notify: UserNotifier::new(config.notify.clone()),
            cwd: config.cwd.clone(),
        };

        // Generate a unique ID for the lifetime of this Codex session.
        let session_mailbox_tx = mailbox_tx.clone();
        let (session, turn_context) = Session::new(
            configure_session,
            config.clone(),
            auth_manager.clone(),
            tx_event.clone(),
            conversation_history,
            session_source,
            session_mailbox_tx,
        )
        .await
        .map_err(|e| {
            error!("Failed to create session: {e:#}");
            CodexErr::InternalAgentDied
        })?;
        let conversation_id = session.conversation_id;

        let dispatcher_client = MailboxDispatcherClient::from_env().map(Arc::new);

        // This task will run until Op::Shutdown is received.
        tokio::spawn(submission_loop(
            session,
            turn_context,
            config,
            rx_sub,
            mailbox_tx.clone(),
            mailbox_rx,
            dispatcher_client,
        ));
        let codex = Codex {
            next_id: AtomicU64::new(0),
            tx_sub,
            rx_event,
        };

        Ok(CodexSpawnOk {
            codex,
            conversation_id,
        })
    }

    pub(crate) fn allocate_submission_id(&self) -> String {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .to_string()
    }

    /// Submit the `op` wrapped in a `Submission` with a unique ID.
    pub async fn submit(&self, op: Op) -> CodexResult<String> {
        let id = self.allocate_submission_id();
        let sub = Submission { id: id.clone(), op };
        self.submit_with_id(sub).await?;
        Ok(id)
    }

    /// Use sparingly: prefer `submit()` so Codex is responsible for generating
    /// unique IDs for each submission.
    pub async fn submit_with_id(&self, sub: Submission) -> CodexResult<()> {
        self.tx_sub
            .send(sub)
            .await
            .map_err(|_| CodexErr::InternalAgentDied)?;
        Ok(())
    }

    pub async fn next_event(&self) -> CodexResult<Event> {
        let event = self
            .rx_event
            .recv()
            .await
            .map_err(|_| CodexErr::InternalAgentDied)?;
        Ok(event)
    }
}

use crate::state::SessionState;

/// Context for an initialized model agent
///
/// A session has at most 1 running task at a time, and can be interrupted by user input.
pub(crate) struct Session {
    conversation_id: ConversationId,
    tx_event: Sender<Event>,
    state: Mutex<SessionState>,
    pub(crate) active_turn: Mutex<Option<ActiveTurn>>,
    pub(crate) services: SessionServices,
    next_internal_sub_id: AtomicU64,
    mailbox_tx: MailboxSender,
    #[cfg(test)]
    wait_trigger_mailbox_events: Mutex<Vec<MailboxDeliveryEvent>>,
    #[cfg(test)]
    wait_trigger_mailbox_failures: Mutex<Vec<MailboxEnqueueError>>,
    wait_triggers: Mutex<HashMap<Uuid, WaitTriggerState>>,
    mailbox_waiters: Mutex<HashMap<Uuid, Vec<oneshot::Sender<MailboxDeliveryEvent>>>>,
    mailbox_predicate_waiters: Mutex<Vec<MailboxPredicateWaiter>>,
    session_source: SessionSource,
    mailbox_metrics: Mutex<MailboxDeliveryTelemetry>,
    mailbox_liveness: Option<Mutex<MailboxLivenessTelemetry>>,
}

#[derive(Debug, Error)]
pub enum MailboxEnqueueError {
    #[error("mailbox dispatcher disabled (set CODEX_MAILBOX_OOB=1 to enable)")]
    Disabled,
    #[error("mailbox queue is full (capacity = {capacity})")]
    Full { capacity: usize },
    #[error("mailbox dispatcher unavailable")]
    Closed,
}

struct MailboxPredicateWaiter {
    id: Uuid,
    filter: MailboxWaitFilter,
    tx: oneshot::Sender<MailboxDeliveryEvent>,
}

struct WaitTriggerState {
    sub_id: String,
    call_id: String,
    predicate_id: String,
    wake_deadline: Option<OffsetDateTime>,
    request_id: String,
    created_at: OffsetDateTime,
    fire_quota: u32,
    fires: u32,
    abort_handle: AbortHandle,
}

impl WaitTriggerState {
    fn new(
        sub_id: String,
        call_id: String,
        predicate_id: String,
        wake_deadline: Option<OffsetDateTime>,
        request_id: String,
        fire_quota: u32,
        abort_handle: AbortHandle,
    ) -> Self {
        Self {
            sub_id,
            call_id,
            predicate_id,
            wake_deadline,
            request_id,
            created_at: OffsetDateTime::now_utc(),
            fire_quota: fire_quota.max(1),
            fires: 0,
            abort_handle,
        }
    }

    fn record_fire(&mut self) {
        self.fires = self.fires.saturating_add(1);
    }

    fn completion_summary(&self, completion: &WaitTriggerCompletion) -> String {
        let mut summary = match &completion.summary {
            Some(summary) if !summary.is_empty() => format!(
                "wait predicate `{}` (call {}) completed: {summary}",
                self.predicate_id, self.call_id
            ),
            _ => format!(
                "wait predicate `{}` (call {}) completed",
                self.predicate_id, self.call_id
            ),
        };

        let elapsed = OffsetDateTime::now_utc() - self.created_at;
        summary.push_str(&format!(
            " after {:.1}s (delivery {}/{})",
            elapsed.as_seconds_f32(),
            self.fires + 1,
            self.fire_quota
        ));

        if let Some(deadline) = self.wake_deadline
            && let Ok(formatted) = deadline.format(&Rfc3339)
        {
            summary.push_str(&format!("; deadline {formatted}"));
        }

        summary
    }

    fn cancellation_summary(&self, reason: WaitTriggerCancelReason) -> String {
        let mut message = match reason {
            WaitTriggerCancelReason::Explicit => format!(
                "wait predicate `{}` (call {}) cancelled by tool context",
                self.predicate_id, self.call_id
            ),
            WaitTriggerCancelReason::TurnShutdown => format!(
                "wait predicate `{}` (call {}) cancelled when turn ended",
                self.predicate_id, self.call_id
            ),
            WaitTriggerCancelReason::Dropped => format!(
                "wait predicate `{}` (call {}) cancelled because the trigger handle was dropped",
                self.predicate_id, self.call_id
            ),
        };

        if let Some(deadline) = self.wake_deadline
            && let Ok(formatted) = deadline.format(&Rfc3339)
        {
            message.push_str(&format!("; deadline {formatted}"));
        }

        message.push_str(&format!(
            " (deliveries so far {}/{})",
            self.fires, self.fire_quota
        ));

        message
    }

    fn default_mailbox_message(&self, summary: &str) -> MailboxMessage {
        let mut message = MailboxMessage::default();
        message.sender.id = WAIT_WITH_PREDICATE_TOOL_NAME.to_string();
        message.sender.role = MailboxSenderRole::Automation;
        message.body.subject = Some(format!("Wait trigger completed for call {}", self.call_id));
        message.body.content = summary.to_string();
        message.body.content_type = MailboxContentType::TextPlain;
        message.audit.request_id = Some(self.request_id.clone());
        message
    }
}

/// The context needed for a single turn of the conversation.
#[derive(Debug)]
pub(crate) struct TurnContext {
    pub(crate) client: ModelClient,
    /// The session's current working directory. All relative paths provided by
    /// the model as well as sandbox policies are resolved against this path
    /// instead of `std::env::current_dir()`.
    pub(crate) cwd: PathBuf,
    pub(crate) base_instructions: Option<String>,
    pub(crate) user_instructions: Option<String>,
    pub(crate) approval_policy: AskForApproval,
    pub(crate) sandbox_policy: SandboxPolicy,
    pub(crate) shell_environment_policy: ShellEnvironmentPolicy,
    pub(crate) tools_config: ToolsConfig,
    // Resolved vm-pty defaults
    pub(crate) vm_pty_default_vm_id: Option<String>,
    pub(crate) vm_pty_open_timeout: std::time::Duration,
    pub(crate) vm_pty_open_blocking: bool,
    pub(crate) vm_pty_max_concurrent_per_worker: Option<usize>,
    pub(crate) is_review_mode: bool,
    pub(crate) final_output_json_schema: Option<Value>,
}

impl TurnContext {
    pub(crate) fn resolve_path(&self, path: Option<String>) -> PathBuf {
        path.as_ref()
            .map(PathBuf::from)
            .map_or_else(|| self.cwd.clone(), |p| self.cwd.join(p))
    }
}

/// Configure the model session.
struct ConfigureSession {
    /// Provider identifier ("openai", "openrouter", ...).
    provider: ModelProviderInfo,

    /// If not specified, server will use its default model.
    model: String,

    model_reasoning_effort: Option<ReasoningEffortConfig>,
    model_reasoning_summary: ReasoningSummaryConfig,

    /// Model instructions that are appended to the base instructions.
    user_instructions: Option<String>,

    /// Base instructions override.
    base_instructions: Option<String>,

    /// When to escalate for approval for execution
    approval_policy: AskForApproval,
    /// How to sandbox commands executed in the system
    sandbox_policy: SandboxPolicy,

    notify: UserNotifier,

    /// Working directory that should be treated as the *root* of the
    /// session. All relative paths supplied by the model as well as the
    /// execution sandbox are resolved against this directory **instead**
    /// of the process-wide current working directory. CLI front-ends are
    /// expected to expand this to an absolute path before sending the
    /// `ConfigureSession` operation so that the business-logic layer can
    /// operate deterministically.
    cwd: PathBuf,
}

impl Session {
    async fn new(
        configure_session: ConfigureSession,
        config: Arc<Config>,
        auth_manager: Arc<AuthManager>,
        tx_event: Sender<Event>,
        initial_history: InitialHistory,
        session_source: SessionSource,
        mailbox_tx: MailboxSender,
    ) -> anyhow::Result<(Arc<Self>, TurnContext)> {
        let ConfigureSession {
            provider,
            model,
            model_reasoning_effort,
            model_reasoning_summary,
            user_instructions,
            base_instructions,
            approval_policy,
            sandbox_policy,
            notify,
            cwd,
        } = configure_session;
        debug!("Configuring session: model={model}; provider={provider:?}");
        if !cwd.is_absolute() {
            return Err(anyhow::anyhow!("cwd is not absolute: {cwd:?}"));
        }

        let (conversation_id, rollout_params) = match &initial_history {
            InitialHistory::New | InitialHistory::Forked(_) => {
                let conversation_id = ConversationId::default();
                (
                    conversation_id,
                    RolloutRecorderParams::new(
                        conversation_id,
                        user_instructions.clone(),
                        session_source,
                    ),
                )
            }
            InitialHistory::Resumed(resumed_history) => (
                resumed_history.conversation_id,
                RolloutRecorderParams::resume(resumed_history.rollout_path.clone()),
            ),
        };

        // Error messages to dispatch after SessionConfigured is sent.
        let mut post_session_configured_error_events = Vec::<Event>::new();

        // Kick off independent async setup tasks in parallel to reduce startup latency.
        //
        // - initialize RolloutRecorder with new or resumed session info
        // - spin up MCP connection manager
        // - perform default shell discovery
        // - load history metadata
        let rollout_fut = RolloutRecorder::new(&config, rollout_params);

        let mcp_fut = McpConnectionManager::new(
            config.mcp_servers.clone(),
            config.use_experimental_use_rmcp_client,
            config.mcp_oauth_credentials_store_mode,
        );
        let default_shell_fut = shell::default_user_shell();
        let history_meta_fut = crate::message_history::history_metadata(&config);

        // Join all independent futures.
        let (rollout_recorder, mcp_res, default_shell, (history_log_id, history_entry_count)) =
            tokio::join!(rollout_fut, mcp_fut, default_shell_fut, history_meta_fut);

        let rollout_recorder = rollout_recorder.map_err(|e| {
            error!("failed to initialize rollout recorder: {e:#}");
            anyhow::anyhow!("failed to initialize rollout recorder: {e:#}")
        })?;
        let rollout_path = rollout_recorder.rollout_path.clone();
        // Create the mutable state for the Session.
        let state = SessionState::new();

        // Handle MCP manager result and record any startup failures.
        let (mcp_connection_manager, failed_clients) = match mcp_res {
            Ok((mgr, failures)) => (mgr, failures),
            Err(e) => {
                let message = format!("Failed to create MCP connection manager: {e:#}");
                error!("{message}");
                post_session_configured_error_events.push(Event {
                    id: INITIAL_SUBMIT_ID.to_owned(),
                    msg: EventMsg::Error(ErrorEvent { message }),
                });
                (McpConnectionManager::default(), Default::default())
            }
        };

        // Surface individual client start-up failures to the user.
        if !failed_clients.is_empty() {
            for (server_name, err) in failed_clients {
                let message = format!("MCP client for `{server_name}` failed to start: {err:#}");
                error!("{message}");
                post_session_configured_error_events.push(Event {
                    id: INITIAL_SUBMIT_ID.to_owned(),
                    msg: EventMsg::Error(ErrorEvent { message }),
                });
            }
        }

        let otel_event_manager = OtelEventManager::new(
            conversation_id,
            config.model.as_str(),
            config.model_family.slug.as_str(),
            auth_manager.auth().and_then(|a| a.get_account_id()),
            auth_manager.auth().map(|a| a.mode),
            config.otel.log_user_prompt,
            terminal::user_agent(),
        );

        otel_event_manager.conversation_starts(
            config.model_provider.name.as_str(),
            config.model_reasoning_effort,
            config.model_reasoning_summary,
            config.model_context_window,
            config.model_max_output_tokens,
            config.model_auto_compact_token_limit,
            config.approval_policy,
            config.sandbox_policy.clone(),
            config.mcp_servers.keys().map(String::as_str).collect(),
            config.active_profile.clone(),
        );

        // Now that the conversation id is final (may have been updated by resume),
        // construct the model client.
        let client = ModelClient::new(
            config.clone(),
            Some(auth_manager.clone()),
            otel_event_manager,
            provider.clone(),
            model_reasoning_effort,
            model_reasoning_summary,
            conversation_id,
        );
        let vm_pty_client = if config.include_vm_pty_tool {
            if let Some(endpoint) = &config.vm_pty_socket {
                Some(Arc::new(VmPtyClient::new(
                    endpoint.clone(),
                    config.vm_pty_connect_timeout,
                    config.vm_pty_request_timeout,
                )))
            } else {
                None
            }
        } else {
            None
        };
        let include_vm_pty_tool = config.include_vm_pty_tool && vm_pty_client.is_some();
        let include_vm_pty_open_tool = config.include_vm_pty_open_tool && include_vm_pty_tool;
        let route_shell_via_pty = include_vm_pty_tool
            && matches!(config.active_profile.as_deref(), Some("worker-pty-sandbox"));

        let turn_context = TurnContext {
            client,
            tools_config: ToolsConfig::new(&ToolsConfigParams {
                model_family: &config.model_family,
                include_plan_tool: config.include_plan_tool,
                include_apply_patch_tool: config.include_apply_patch_tool,
                include_web_search_request: config.tools_web_search_request,
                use_streamable_shell_tool: config.use_experimental_streamable_shell_tool,
                include_shell_tool: config.include_shell_tool,
                include_view_image_tool: config.include_view_image_tool,
                include_vm_pty_tool,
                include_vm_pty_open_tool,
                route_shell_via_pty,
                experimental_unified_exec_tool: config.use_experimental_unified_exec_tool,
                debug_tools: config.debug_tools,
            }),
            user_instructions,
            base_instructions,
            approval_policy,
            sandbox_policy,
            shell_environment_policy: config.shell_environment_policy.clone(),
            cwd,
            vm_pty_default_vm_id: config.vm_pty_default_vm_id.clone(),
            vm_pty_open_timeout: config.vm_pty_open_timeout,
            vm_pty_open_blocking: config.vm_pty_open_blocking,
            vm_pty_max_concurrent_per_worker: config.vm_pty_max_concurrent_per_worker,
            is_review_mode: false,
            final_output_json_schema: None,
        };
        let services = SessionServices {
            mcp_connection_manager,
            session_manager: ExecSessionManager::default(),
            unified_exec_manager: UnifiedExecSessionManager::default(),
            notifier: notify,
            rollout: Mutex::new(Some(rollout_recorder)),
            user_shell: default_shell,
            show_raw_agent_reasoning: config.show_raw_agent_reasoning,
            executor: Executor::new(ExecutorConfig::new(
                turn_context.sandbox_policy.clone(),
                turn_context.cwd.clone(),
                config.codex_linux_sandbox_exe.clone(),
                matches!(config.active_profile.as_deref(), Some("management")),
            )),
            summaries: Mutex::new(None),
            vm_pty_client: vm_pty_client.clone(),
            pty_sessions: Mutex::new(Default::default()),
        };

        let mailbox_liveness = if config.mailbox_liveness.enabled {
            Some(Mutex::new(MailboxLivenessTelemetry::new(
                config.mailbox_liveness.clone(),
            )))
        } else {
            None
        };

        // Read any existing summary checkpoint for this conversation to seed the summaries state.
        let initial_summary = match summaries::read_summary_checkpoint(&conversation_id).await {
            Ok(s) => s,
            Err(e) => {
                warn!("failed to read summary checkpoint: {e}");
                None
            }
        };

        let sess = Arc::new(Session {
            conversation_id,
            tx_event: tx_event.clone(),
            state: Mutex::new(state),
            active_turn: Mutex::new(None),
            services,
            next_internal_sub_id: AtomicU64::new(0),
            mailbox_tx,
            #[cfg(test)]
            wait_trigger_mailbox_events: Mutex::new(Vec::new()),
            #[cfg(test)]
            wait_trigger_mailbox_failures: Mutex::new(Vec::new()),
            wait_triggers: Mutex::new(HashMap::new()),
            mailbox_waiters: Mutex::new(HashMap::new()),
            mailbox_predicate_waiters: Mutex::new(Vec::new()),
            session_source,
            mailbox_metrics: Mutex::new(MailboxDeliveryTelemetry::new()),
            mailbox_liveness,
        });

        // Spawn background continuous summaries if enabled and install the handle.
        if let Some(svc) = crate::summaries::SummariesService::spawn(
            Arc::clone(&sess),
            config.summaries.clone(),
            initial_summary,
        ) {
            let mut slot = sess.services.summaries.lock().await;
            *slot = Some(svc);
        }

        // Dispatch the SessionConfiguredEvent first and then report any errors.
        // If resuming, include converted initial messages in the payload so UIs can render them immediately.
        let initial_messages = initial_history.get_event_msgs();
        sess.record_initial_history(&turn_context, initial_history)
            .await;

        let events = std::iter::once(Event {
            id: INITIAL_SUBMIT_ID.to_owned(),
            msg: EventMsg::SessionConfigured(SessionConfiguredEvent {
                session_id: conversation_id,
                model,
                reasoning_effort: model_reasoning_effort,
                history_log_id,
                history_entry_count,
                initial_messages,
                rollout_path,
            }),
        })
        .chain(post_session_configured_error_events.into_iter());
        for event in events {
            sess.send_event(event).await;
        }

        Ok((sess, turn_context))
    }

    pub(crate) fn get_conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    pub(crate) fn get_tx_event(&self) -> Sender<Event> {
        self.tx_event.clone()
    }

    fn next_internal_sub_id(&self) -> String {
        let id = self
            .next_internal_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("auto-compact-{id}")
    }

    async fn record_initial_history(
        &self,
        turn_context: &TurnContext,
        conversation_history: InitialHistory,
    ) {
        match conversation_history {
            InitialHistory::New => {
                // Build and record initial items (user instructions + environment context)
                let items = self.build_initial_context(turn_context);
                self.record_conversation_items(&items).await;
            }
            InitialHistory::Resumed(_) | InitialHistory::Forked(_) => {
                let rollout_items = conversation_history.get_rollout_items();
                let persist = matches!(conversation_history, InitialHistory::Forked(_));

                // Always add response items to conversation history
                let reconstructed_history =
                    self.reconstruct_history_from_rollout(turn_context, &rollout_items);
                if !reconstructed_history.is_empty() {
                    self.record_into_history(&reconstructed_history).await;
                }

                // If persisting, persist all rollout items as-is (recorder filters)
                if persist && !rollout_items.is_empty() {
                    self.persist_rollout_items(&rollout_items).await;
                }
            }
        }
    }

    /// Persist the event to rollout and send it to clients.
    pub(crate) async fn send_event(&self, event: Event) {
        // Persist the event into rollout (recorder filters as needed)
        let mut rollout_items = vec![RolloutItem::EventMsg(event.msg.clone())];

        // For background summary updates, also persist a durable snapshot and write a checkpoint.
        if let EventMsg::SummaryUpdated(crate::protocol::SummaryUpdatedEvent { summary }) =
            &event.msg
        {
            rollout_items.push(RolloutItem::SummarySnapshot(
                crate::protocol::SummarySnapshotItem {
                    summary: summary.clone(),
                },
            ));

            // Fire-and-forget checkpoint write to avoid blocking the event loop.
            let cid = self.conversation_id;
            let summary_for_write = summary.clone();
            tokio::spawn(async move {
                if let Err(e) = summaries::write_summary_checkpoint(&cid, &summary_for_write).await
                {
                    tracing::warn!("failed to write summary checkpoint: {e}");
                }
            });
        }

        self.persist_rollout_items(&rollout_items).await;
        if let Err(e) = self.tx_event.send(event).await {
            error!("failed to send tool call event: {e}");
        }
    }

    pub(crate) async fn enqueue_mailbox_envelope(
        &self,
        submission_id: String,
        message: MailboxMessage,
        mailbox_enabled: bool,
    ) -> Result<MailboxDeliveryEvent, MailboxEnqueueError> {
        if !mailbox_enabled {
            let event = Event {
                id: submission_id,
                msg: EventMsg::BackgroundEvent(BackgroundEventEvent {
                    message: "Mailbox dispatcher disabled (set CODEX_MAILBOX_OOB=1 to enable)"
                        .to_string(),
                }),
            };
            self.send_event(event).await;
            return Err(MailboxEnqueueError::Disabled);
        }

        let observed_at = OffsetDateTime::now_utc();
        let pending = MailboxEnvelope::new(submission_id.clone(), message.clone());
        match self.mailbox_tx.try_enqueue(pending.clone()) {
            Ok(queue_depth) => {
                let expires_at = pending
                    .message
                    .expires_at
                    .map(|ts| ts.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into()));
                let ack_deadline = pending
                    .message
                    .ack_policy
                    .deadline
                    .map(|ts| ts.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into()));
                let rate_scope = pending
                    .message
                    .rate_limit
                    .as_ref()
                    .map(|r| format!("{:?}", r.scope).to_lowercase());
                let rate_capacity = pending.message.rate_limit.as_ref().and_then(|r| r.capacity);
                let rate_interval = pending
                    .message
                    .rate_limit
                    .as_ref()
                    .and_then(|r| r.interval_seconds);

                info!(
                    target: "codex::mailbox",
                    event = "enqueued",
                    submission_id = %pending.submission_id,
                    conversation_id = %self.conversation_id,
                    message_id = %pending.message.message_id,
                    sender_id = %pending.message.sender.id,
                    sender_role = ?pending.message.sender.role,
                    priority = ?pending.message.priority,
                    request_id = pending.message.audit.request_id.as_deref().unwrap_or(""),
                    ack_mode = ?pending.message.ack_policy.mode,
                    ack_deadline = ack_deadline.as_deref(),
                    expires_at = expires_at.as_deref(),
                    rate_scope = rate_scope.as_deref(),
                    rate_capacity,
                    rate_interval,
                    queue_depth,
                );

                let ingress = self.mailbox_ingress(&pending.message);
                let enqueued_event = MailboxDeliveryEvent {
                    message,
                    state: MailboxDeliveryState::Enqueued,
                    queue_depth: Some(queue_depth),
                    observed_at: Some(observed_at),
                    correlation_id: Some(submission_id.clone()),
                    ingress: Some(ingress.clone()),
                    delivery_latency_ms: None,
                };

                {
                    let labels = MailboxTelemetryLabels::new(ingress, &enqueued_event.message);
                    let mut telemetry = self.mailbox_metrics.lock().await;
                    telemetry.record_enqueued(&enqueued_event, &labels);
                }

                let event = Event {
                    id: submission_id,
                    msg: EventMsg::MailboxDelivery(enqueued_event.clone()),
                };
                self.send_event(event).await;
                self.notify_mailbox_predicate_waiters(&enqueued_event).await;
                Ok(enqueued_event)
            }
            Err(TryEnqueueError::Full { capacity, .. }) => {
                let error_msg = format!(
                    "Mailbox queue is full (capacity = {capacity}); dropping message {}",
                    pending.message.message_id
                );
                warn!(
                    target: "codex::mailbox",
                    event = "dropped_full",
                    message_id = %pending.message.message_id,
                    submission_id = %pending.submission_id,
                    conversation_id = %self.conversation_id,
                    sender_id = %pending.message.sender.id,
                    priority = ?pending.message.priority,
                    request_id = pending.message.audit.request_id.as_deref().unwrap_or(""),
                    capacity
                );
                let event = Event {
                    id: submission_id,
                    msg: EventMsg::Error(ErrorEvent {
                        message: error_msg.clone(),
                    }),
                };
                self.send_event(event).await;
                Err(MailboxEnqueueError::Full { capacity })
            }
            Err(TryEnqueueError::Closed(_)) => {
                warn!(
                    target: "codex::mailbox",
                    event = "dispatcher_closed",
                    submission_id = %pending.submission_id,
                    conversation_id = %self.conversation_id,
                    message_id = %pending.message.message_id,
                    sender_id = %pending.message.sender.id,
                    request_id = pending.message.audit.request_id.as_deref().unwrap_or("")
                );
                let event = Event {
                    id: submission_id,
                    msg: EventMsg::Error(ErrorEvent {
                        message: "Mailbox dispatcher unavailable".to_string(),
                    }),
                };
                self.send_event(event).await;
                Err(MailboxEnqueueError::Closed)
            }
        }
    }

    pub(crate) async fn register_mailbox_delivery_listener(
        &self,
        message_id: Uuid,
    ) -> oneshot::Receiver<MailboxDeliveryEvent> {
        let (tx, rx) = oneshot::channel();
        let mut waiters = self.mailbox_waiters.lock().await;
        waiters.entry(message_id).or_default().push(tx);
        rx
    }

    pub(crate) async fn cancel_mailbox_delivery_listener(&self, message_id: Uuid) {
        let mut waiters = self.mailbox_waiters.lock().await;
        waiters.remove(&message_id);
    }

    pub(crate) async fn register_mailbox_predicate_waiter(
        &self,
        filter: MailboxWaitFilter,
    ) -> (Uuid, oneshot::Receiver<MailboxDeliveryEvent>) {
        let id = Uuid::now_v7();
        let (tx, rx) = oneshot::channel();
        let mut waiters = self.mailbox_predicate_waiters.lock().await;
        waiters.retain(|waiter| !waiter.tx.is_closed());
        waiters.push(MailboxPredicateWaiter { id, filter, tx });
        (id, rx)
    }

    pub(crate) async fn cancel_mailbox_predicate_waiter(&self, waiter_id: Uuid) {
        let mut waiters = self.mailbox_predicate_waiters.lock().await;
        waiters.retain(|waiter| waiter.id != waiter_id && !waiter.tx.is_closed());
    }

    pub(crate) async fn notify_mailbox_predicate_waiters(&self, event: &MailboxDeliveryEvent) {
        let mut waiters = self.mailbox_predicate_waiters.lock().await;
        let mut idx = 0;
        while idx < waiters.len() {
            if waiters[idx].tx.is_closed() {
                waiters.swap_remove(idx);
                continue;
            }

            if waiters[idx].filter.matches(event) {
                let waiter = waiters.swap_remove(idx);
                let _ = waiter.tx.send(event.clone());
                continue;
            }

            idx += 1;
        }
    }

    async fn notify_mailbox_delivery_listeners(&self, delivery: &MailboxDeliveryEvent) {
        let mut waiters = self.mailbox_waiters.lock().await;
        if let Some(listeners) = waiters.remove(&delivery.message.message_id) {
            for tx in listeners {
                let _ = tx.send(delivery.clone());
            }
        }
    }

    /// Emit an exec approval request event and await the user's decision.
    ///
    /// The request is keyed by `sub_id`/`call_id` so matching responses are delivered
    /// to the correct in-flight turn. If the task is aborted, this returns the
    /// default `ReviewDecision` (`Denied`).
    pub async fn request_command_approval(
        &self,
        sub_id: String,
        call_id: String,
        command: Vec<String>,
        cwd: PathBuf,
        reason: Option<String>,
    ) -> ReviewDecision {
        // Add the tx_approve callback to the map before sending the request.
        let (tx_approve, rx_approve) = oneshot::channel();
        let event_id = sub_id.clone();
        let prev_entry = {
            let mut active = self.active_turn.lock().await;
            match active.as_mut() {
                Some(at) => {
                    let mut ts = at.turn_state.lock().await;
                    ts.insert_pending_approval(sub_id, tx_approve)
                }
                None => None,
            }
        };
        if prev_entry.is_some() {
            warn!("Overwriting existing pending approval for sub_id: {event_id}");
        }

        let event = Event {
            id: event_id,
            msg: EventMsg::ExecApprovalRequest(ExecApprovalRequestEvent {
                call_id,
                command,
                cwd,
                reason,
            }),
        };
        self.send_event(event).await;
        rx_approve.await.unwrap_or_default()
    }

    pub async fn request_patch_approval(
        &self,
        sub_id: String,
        call_id: String,
        action: &ApplyPatchAction,
        reason: Option<String>,
        grant_root: Option<PathBuf>,
    ) -> oneshot::Receiver<ReviewDecision> {
        // Add the tx_approve callback to the map before sending the request.
        let (tx_approve, rx_approve) = oneshot::channel();
        let event_id = sub_id.clone();
        let prev_entry = {
            let mut active = self.active_turn.lock().await;
            match active.as_mut() {
                Some(at) => {
                    let mut ts = at.turn_state.lock().await;
                    ts.insert_pending_approval(sub_id, tx_approve)
                }
                None => None,
            }
        };
        if prev_entry.is_some() {
            warn!("Overwriting existing pending approval for sub_id: {event_id}");
        }

        let event = Event {
            id: event_id,
            msg: EventMsg::ApplyPatchApprovalRequest(ApplyPatchApprovalRequestEvent {
                call_id,
                changes: convert_apply_patch_to_protocol(action),
                reason,
                grant_root,
            }),
        };
        self.send_event(event).await;
        rx_approve
    }

    pub async fn notify_approval(&self, sub_id: &str, decision: ReviewDecision) {
        let entry = {
            let mut active = self.active_turn.lock().await;
            match active.as_mut() {
                Some(at) => {
                    let mut ts = at.turn_state.lock().await;
                    ts.remove_pending_approval(sub_id)
                }
                None => None,
            }
        };
        match entry {
            Some(tx_approve) => {
                tx_approve.send(decision).ok();
            }
            None => {
                warn!("No pending approval found for sub_id: {sub_id}");
            }
        }
    }

    /// Records input items: always append to conversation history and
    /// persist these response items to rollout.
    async fn record_conversation_items(&self, items: &[ResponseItem]) {
        self.record_into_history(items).await;
        self.persist_rollout_response_items(items).await;
    }

    fn reconstruct_history_from_rollout(
        &self,
        turn_context: &TurnContext,
        rollout_items: &[RolloutItem],
    ) -> Vec<ResponseItem> {
        let mut history = ConversationHistory::new();
        for item in rollout_items {
            match item {
                RolloutItem::ResponseItem(response_item) => {
                    history.record_items(std::iter::once(response_item));
                }
                RolloutItem::Compacted(compacted) => {
                    let snapshot = history.contents();
                    let user_messages = collect_user_messages(&snapshot);
                    let rebuilt = build_compacted_history(
                        self.build_initial_context(turn_context),
                        &user_messages,
                        &compacted.message,
                    );
                    history.replace(rebuilt);
                }
                _ => {}
            }
        }
        history.contents()
    }

    /// Append ResponseItems to the in-memory conversation history only.
    async fn record_into_history(&self, items: &[ResponseItem]) {
        let mut state = self.state.lock().await;
        state.record_items(items.iter());
    }

    async fn replace_history(&self, items: Vec<ResponseItem>) {
        let mut state = self.state.lock().await;
        state.replace_history(items);
    }

    async fn persist_rollout_response_items(&self, items: &[ResponseItem]) {
        let rollout_items: Vec<RolloutItem> = items
            .iter()
            .cloned()
            .map(RolloutItem::ResponseItem)
            .collect();
        self.persist_rollout_items(&rollout_items).await;
    }

    pub(crate) fn build_initial_context(&self, turn_context: &TurnContext) -> Vec<ResponseItem> {
        let mut items = Vec::<ResponseItem>::with_capacity(2);
        if let Some(user_instructions) = turn_context.user_instructions.as_deref() {
            items.push(UserInstructions::new(user_instructions.to_string()).into());
        }
        items.push(ResponseItem::from(EnvironmentContext::new(
            Some(turn_context.cwd.clone()),
            Some(turn_context.approval_policy),
            Some(turn_context.sandbox_policy.clone()),
            Some(self.user_shell().clone()),
        )));
        items
    }

    async fn persist_rollout_items(&self, items: &[RolloutItem]) {
        let recorder = {
            let guard = self.services.rollout.lock().await;
            guard.clone()
        };
        if let Some(rec) = recorder
            && let Err(e) = rec.record_items(items).await
        {
            error!("failed to record rollout items: {e:#}");
        }
    }

    pub(crate) async fn history_snapshot(&self) -> Vec<ResponseItem> {
        let state = self.state.lock().await;
        state.history_snapshot()
    }

    async fn update_token_usage_info(
        &self,
        sub_id: &str,
        turn_context: &TurnContext,
        token_usage: Option<&TokenUsage>,
    ) {
        {
            let mut state = self.state.lock().await;
            if let Some(token_usage) = token_usage {
                state.update_token_info_from_usage(
                    token_usage,
                    turn_context.client.get_model_context_window(),
                );
            }
        }
        self.send_token_count_event(sub_id).await;
    }

    async fn update_rate_limits(&self, sub_id: &str, new_rate_limits: RateLimitSnapshot) {
        {
            let mut state = self.state.lock().await;
            state.set_rate_limits(new_rate_limits);
        }
        self.send_token_count_event(sub_id).await;
    }

    async fn send_token_count_event(&self, sub_id: &str) {
        let (info, rate_limits) = {
            let state = self.state.lock().await;
            state.token_info_and_rate_limits()
        };
        let event = Event {
            id: sub_id.to_string(),
            msg: EventMsg::TokenCount(TokenCountEvent { info, rate_limits }),
        };
        self.send_event(event).await;
    }

    async fn set_total_tokens_full(&self, sub_id: &str, turn_context: &TurnContext) {
        let context_window = turn_context.client.get_model_context_window();
        if let Some(context_window) = context_window {
            {
                let mut state = self.state.lock().await;
                state.set_token_usage_full(context_window);
            }
            self.send_token_count_event(sub_id).await;
        }
    }

    /// Record a user input item to conversation history and also persist a
    /// corresponding UserMessage EventMsg to rollout.
    async fn record_input_and_rollout_usermsg(&self, response_input: &ResponseInputItem) {
        let response_item: ResponseItem = response_input.clone().into();
        // Add to conversation history and persist response item to rollout
        self.record_conversation_items(std::slice::from_ref(&response_item))
            .await;

        // Derive user message events and persist only UserMessage to rollout
        let msgs =
            map_response_item_to_event_messages(&response_item, self.show_raw_agent_reasoning());
        let user_msgs: Vec<RolloutItem> = msgs
            .into_iter()
            .filter_map(|m| match m {
                EventMsg::UserMessage(ev) => Some(RolloutItem::EventMsg(EventMsg::UserMessage(ev))),
                _ => None,
            })
            .collect();
        if !user_msgs.is_empty() {
            self.persist_rollout_items(&user_msgs).await;
        }
    }

    async fn on_exec_command_begin(
        &self,
        turn_diff_tracker: SharedTurnDiffTracker,
        exec_command_context: ExecCommandContext,
    ) {
        let ExecCommandContext {
            sub_id,
            call_id,
            command_for_display,
            cwd,
            apply_patch,
            ..
        } = exec_command_context;
        let msg = match apply_patch {
            Some(ApplyPatchCommandContext {
                user_explicitly_approved_this_action,
                changes,
            }) => {
                {
                    let mut tracker = turn_diff_tracker.lock().await;
                    tracker.on_patch_begin(&changes);
                }

                EventMsg::PatchApplyBegin(PatchApplyBeginEvent {
                    call_id,
                    auto_approved: !user_explicitly_approved_this_action,
                    changes,
                })
            }
            None => EventMsg::ExecCommandBegin(ExecCommandBeginEvent {
                call_id,
                command: command_for_display.clone(),
                cwd,
                parsed_cmd: parse_command(&command_for_display)
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }),
        };
        let event = Event {
            id: sub_id.to_string(),
            msg,
        };
        self.send_event(event).await;
    }

    async fn on_exec_command_end(
        &self,
        turn_diff_tracker: SharedTurnDiffTracker,
        sub_id: &str,
        call_id: &str,
        output: &ExecToolCallOutput,
        is_apply_patch: bool,
    ) {
        let ExecToolCallOutput {
            stdout,
            stderr,
            aggregated_output,
            duration,
            exit_code,
            timed_out: _,
        } = output;
        // Send full stdout/stderr to clients; do not truncate.
        let stdout = stdout.text.clone();
        let stderr = stderr.text.clone();
        let formatted_output = format_exec_output_str(output);
        let aggregated_output: String = aggregated_output.text.clone();

        let msg = if is_apply_patch {
            EventMsg::PatchApplyEnd(PatchApplyEndEvent {
                call_id: call_id.to_string(),
                stdout,
                stderr,
                success: *exit_code == 0,
            })
        } else {
            EventMsg::ExecCommandEnd(ExecCommandEndEvent {
                call_id: call_id.to_string(),
                stdout,
                stderr,
                aggregated_output,
                exit_code: *exit_code,
                duration: *duration,
                formatted_output,
            })
        };

        let event = Event {
            id: sub_id.to_string(),
            msg,
        };
        self.send_event(event).await;

        // If this is an apply_patch, after we emit the end patch, emit a second event
        // with the full turn diff if there is one.
        if is_apply_patch {
            let unified_diff = {
                let mut tracker = turn_diff_tracker.lock().await;
                tracker.get_unified_diff()
            };
            if let Ok(Some(unified_diff)) = unified_diff {
                let msg = EventMsg::TurnDiff(TurnDiffEvent { unified_diff });
                let event = Event {
                    id: sub_id.into(),
                    msg,
                };
                self.send_event(event).await;
            }
        }
    }
    /// Runs the exec tool call and emits events for the begin and end of the
    /// command even on error.
    ///
    /// Returns the output of the exec tool call.
    pub(crate) async fn run_exec_with_events(
        &self,
        turn_diff_tracker: SharedTurnDiffTracker,
        prepared: PreparedExec,
        approval_policy: AskForApproval,
    ) -> Result<ExecToolCallOutput, ExecError> {
        let PreparedExec { context, request } = prepared;
        let is_apply_patch = context.apply_patch.is_some();
        let sub_id = context.sub_id.clone();
        let call_id = context.call_id.clone();

        self.on_exec_command_begin(turn_diff_tracker.clone(), context.clone())
            .await;

        let result = self
            .services
            .executor
            .run(request, self, approval_policy, &context)
            .await;

        let normalized = normalize_exec_result(&result);
        let borrowed = normalized.event_output();

        self.on_exec_command_end(
            turn_diff_tracker,
            &sub_id,
            &call_id,
            borrowed,
            is_apply_patch,
        )
        .await;

        drop(normalized);

        result
    }

    /// Helper that emits a BackgroundEvent with the given message. This keeps
    /// the call‑sites terse so adding more diagnostics does not clutter the
    /// core agent logic.
    pub(crate) async fn notify_background_event(&self, sub_id: &str, message: impl Into<String>) {
        let event = Event {
            id: sub_id.to_string(),
            msg: EventMsg::BackgroundEvent(BackgroundEventEvent {
                message: message.into(),
            }),
        };
        self.send_event(event).await;
    }

    async fn notify_stream_error(&self, sub_id: &str, message: impl Into<String>) {
        let event = Event {
            id: sub_id.to_string(),
            msg: EventMsg::StreamError(StreamErrorEvent {
                message: message.into(),
            }),
        };
        self.send_event(event).await;
    }

    /// Build the full turn input by concatenating the current conversation
    /// history with additional items for this turn.
    pub async fn turn_input_with_history(&self, extra: Vec<ResponseItem>) -> Vec<ResponseItem> {
        let history = {
            let state = self.state.lock().await;
            state.history_snapshot()
        };
        [history, extra].concat()
    }

    /// Returns the input if there was no task running to inject into
    pub async fn inject_input(&self, input: Vec<InputItem>) -> Result<(), Vec<InputItem>> {
        let mut active = self.active_turn.lock().await;
        match active.as_mut() {
            Some(at) => {
                let mut ts = at.turn_state.lock().await;
                ts.push_pending_input(input.into());
                Ok(())
            }
            None => Err(input),
        }
    }

    pub async fn inject_response_items(
        &self,
        items: Vec<ResponseInputItem>,
    ) -> Result<(), Vec<ResponseInputItem>> {
        if items.is_empty() {
            return Ok(());
        }

        let mut active = self.active_turn.lock().await;
        match active.as_mut() {
            Some(at) => {
                let mut ts = at.turn_state.lock().await;
                let mut pending = items;
                for item in pending.drain(..) {
                    ts.push_pending_input(item);
                }
                Ok(())
            }
            None => Err(items),
        }
    }

    pub async fn get_pending_input(&self) -> Vec<ResponseInputItem> {
        let mut active = self.active_turn.lock().await;
        match active.as_mut() {
            Some(at) => {
                let mut ts = at.turn_state.lock().await;
                ts.take_pending_input()
            }
            None => Vec::with_capacity(0),
        }
    }

    pub async fn call_tool(
        &self,
        server: &str,
        tool: &str,
        arguments: Option<serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        self.services
            .mcp_connection_manager
            .call_tool(server, tool, arguments)
            .await
    }

    pub(crate) fn parse_mcp_tool_name(&self, tool_name: &str) -> Option<(String, String)> {
        self.services
            .mcp_connection_manager
            .parse_tool_name(tool_name)
    }

    pub(crate) async fn schedule_wait_trigger(
        self: &Arc<Self>,
        sub_id: &str,
        call_id: &str,
        spec: WaitTriggerSpec,
    ) -> Result<WaitTriggerHandle, WaitTriggerError> {
        let WaitTriggerSpec {
            predicate_id,
            wake_deadline,
            fire_quota,
            request_id,
        } = spec;

        let quota = fire_quota.max(1);
        let (trigger_id, effective_request_id) = {
            let mut active = self.active_turn.lock().await;
            let Some(active_turn) = active.as_mut() else {
                return Err(WaitTriggerError::InactiveTurn);
            };
            let mut ts = active_turn.turn_state.lock().await;
            if ts.trigger_count() >= MAX_WAIT_TRIGGERS_PER_TURN {
                return Err(WaitTriggerError::QuotaExceeded {
                    limit: MAX_WAIT_TRIGGERS_PER_TURN,
                });
            }
            ts.create_trigger(
                predicate_id.clone(),
                sub_id.to_string(),
                call_id.to_string(),
                wake_deadline,
                quota,
                request_id.clone(),
            )
        };

        let (completion_tx, completion_rx) = oneshot::channel();
        let session = Arc::clone(self);
        let trigger_for_task = trigger_id;
        let task_handle = tokio::spawn(async move {
            match completion_rx.await {
                Ok(completion) => {
                    session
                        .finalize_wait_trigger(trigger_for_task, Some(completion))
                        .await;
                }
                Err(_) => {
                    session.finalize_wait_trigger(trigger_for_task, None).await;
                }
            }
        });
        let abort_handle = task_handle.abort_handle();

        let state = WaitTriggerState::new(
            sub_id.to_string(),
            call_id.to_string(),
            predicate_id,
            wake_deadline,
            effective_request_id,
            quota,
            abort_handle,
        );

        {
            let mut guard = self.wait_triggers.lock().await;
            if guard.contains_key(&trigger_id) {
                state.abort_handle.clone().abort();
                return Err(WaitTriggerError::TriggerClosed);
            }
            guard.insert(trigger_id, state);
        }

        Ok(WaitTriggerHandle::new(self, trigger_id, completion_tx))
    }

    pub(crate) async fn cancel_wait_trigger(
        self: &Arc<Self>,
        trigger_id: Uuid,
        reason: WaitTriggerCancelReason,
    ) -> Result<(), WaitTriggerError> {
        if self
            .cancel_wait_trigger_internal(trigger_id, reason)
            .await
            .is_some()
        {
            Ok(())
        } else {
            Err(WaitTriggerError::NotFound)
        }
    }

    pub(crate) async fn cancel_wait_trigger_internal(
        &self,
        trigger_id: Uuid,
        reason: WaitTriggerCancelReason,
    ) -> Option<()> {
        let state = {
            let mut guard = self.wait_triggers.lock().await;
            guard.remove(&trigger_id)
        };

        if let Some(state) = state {
            state.abort_handle.abort();
            self.remove_trigger_from_turn(&trigger_id).await;
            self.handle_wait_trigger_cancellation(state, reason).await;
            Some(())
        } else {
            None
        }
    }

    async fn finalize_wait_trigger(
        self: &Arc<Self>,
        trigger_id: Uuid,
        completion: Option<WaitTriggerCompletion>,
    ) {
        let state = {
            let mut guard = self.wait_triggers.lock().await;
            guard.remove(&trigger_id)
        };
        let _ = self.remove_trigger_from_turn(&trigger_id).await;

        match (state, completion) {
            (Some(mut state), Some(completion)) => {
                state.record_fire();
                self.handle_wait_trigger_success(state, completion).await;
            }
            (Some(state), None) => {
                self.handle_wait_trigger_cancellation(state, WaitTriggerCancelReason::Dropped)
                    .await;
            }
            _ => {}
        }
    }

    async fn handle_wait_trigger_success(
        &self,
        state: WaitTriggerState,
        completion: WaitTriggerCompletion,
    ) {
        let summary = state.completion_summary(&completion);
        self.notify_background_event(&state.sub_id, summary.clone())
            .await;

        if !completion.inputs.is_empty() {
            if let Err(unconsumed) = self.inject_response_items(completion.inputs.clone()).await {
                warn!(
                    predicate = %state.predicate_id,
                    remaining = unconsumed.len(),
                    "failed to inject wait trigger completion because the turn is no longer active"
                );
            }
        }

        if mailbox_feature_enabled() {
            let mut message = completion
                .mailbox_message
                .unwrap_or_else(|| state.default_mailbox_message(&summary));
            if message.audit.request_id.is_none() {
                message.audit.request_id = Some(state.request_id.clone());
            }
            apply_mailbox_defaults(&mut message);
            if let Err(err) = validate_mailbox_message(&message) {
                warn!(
                    predicate = %state.predicate_id,
                    ?err,
                    "wait trigger completion produced invalid mailbox message"
                );
            } else {
                match self
                    .enqueue_mailbox_envelope(state.sub_id.clone(), message, true)
                    .await
                {
                    Ok(enqueued) => {
                        #[cfg(test)]
                        {
                            let mut guard = self.wait_trigger_mailbox_events.lock().await;
                            guard.push(enqueued.clone());
                        }
                        #[cfg(not(test))]
                        {
                            let _ = enqueued;
                        }
                    }
                    Err(err) => {
                        warn!(
                            predicate = %state.predicate_id,
                            ?err,
                            "failed to enqueue wait trigger mailbox notification"
                        );
                        #[cfg(test)]
                        {
                            let mut guard = self.wait_trigger_mailbox_failures.lock().await;
                            guard.push(err);
                        }
                    }
                }
            }
        }
    }

    async fn handle_wait_trigger_cancellation(
        &self,
        state: WaitTriggerState,
        reason: WaitTriggerCancelReason,
    ) {
        let message = state.cancellation_summary(reason);
        self.notify_background_event(&state.sub_id, message).await;
    }

    async fn remove_trigger_from_turn(&self, trigger_id: &Uuid) -> bool {
        let mut active = self.active_turn.lock().await;
        if let Some(active_turn) = active.as_mut() {
            let mut ts = active_turn.turn_state.lock().await;
            ts.remove_trigger(trigger_id).is_some()
        } else {
            false
        }
    }

    pub(crate) async fn handle_exec_command_tool(
        &self,
        params: ExecCommandParams,
    ) -> Result<String, FunctionCallError> {
        let result = self
            .services
            .session_manager
            .handle_exec_command_request(params)
            .await;
        match result {
            Ok(output) => Ok(output.to_text_output()),
            Err(err) => Err(FunctionCallError::RespondToModel(err)),
        }
    }

    pub(crate) async fn handle_write_stdin_tool(
        &self,
        params: WriteStdinParams,
    ) -> Result<String, FunctionCallError> {
        self.services
            .session_manager
            .handle_write_stdin_request(params)
            .await
            .map(|output| output.to_text_output())
            .map_err(FunctionCallError::RespondToModel)
    }

    pub(crate) async fn run_unified_exec_request(
        &self,
        request: crate::unified_exec::UnifiedExecRequest<'_>,
    ) -> Result<crate::unified_exec::UnifiedExecResult, crate::unified_exec::UnifiedExecError> {
        self.services
            .unified_exec_manager
            .handle_request(request)
            .await
    }

    pub async fn interrupt_task(self: &Arc<Self>) {
        info!("interrupt received: abort current task, if any");
        self.abort_all_tasks(TurnAbortReason::Interrupted).await;
    }

    fn interrupt_task_sync(&self) {
        if let Ok(mut active) = self.active_turn.try_lock()
            && let Some(at) = active.as_mut()
        {
            let trigger_ids = at.try_clear_pending_sync();
            let tasks = at.drain_tasks();
            *active = None;
            if let Ok(mut guard) = self.wait_triggers.try_lock() {
                for trigger_id in trigger_ids {
                    if let Some(state) = guard.remove(&trigger_id) {
                        state.abort_handle.abort();
                    }
                }
            }
            for (_sub_id, task) in tasks {
                task.handle.abort();
            }
        }
    }

    pub(crate) fn notifier(&self) -> &UserNotifier {
        &self.services.notifier
    }

    pub(crate) fn user_shell(&self) -> &shell::Shell {
        &self.services.user_shell
    }

    fn show_raw_agent_reasoning(&self) -> bool {
        self.services.show_raw_agent_reasoning
    }

    fn mailbox_ingress(&self, message: &MailboxMessage) -> MailboxDeliveryIngress {
        if let Some(value) = message
            .metadata
            .get("ingress")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_ascii_lowercase())
        {
            match value.as_str() {
                "script" => return MailboxDeliveryIngress::Script,
                "cli" => return MailboxDeliveryIngress::Cli,
                "mcp" => return MailboxDeliveryIngress::Mcp,
                "vscode" => return MailboxDeliveryIngress::Vscode,
                "api" => return MailboxDeliveryIngress::Api,
                _ => {}
            }
        }

        match self.session_source {
            SessionSource::Cli | SessionSource::Exec => MailboxDeliveryIngress::Cli,
            SessionSource::VSCode => MailboxDeliveryIngress::Vscode,
            SessionSource::Mcp => MailboxDeliveryIngress::Mcp,
            SessionSource::Unknown => MailboxDeliveryIngress::Unknown,
        }
    }

    async fn record_mailbox_liveness(
        &self,
        transport_lag: Duration,
    ) -> Option<MailboxLivenessSnapshot> {
        let telemetry = self.mailbox_liveness.as_ref()?;

        if !mailbox_feature_enabled() {
            return None;
        }

        let queue_depth = self.mailbox_tx.len();
        let now = Instant::now();
        let observed_at = OffsetDateTime::now_utc();
        let mut guard = telemetry.lock().await;
        guard.record(now, observed_at, transport_lag, queue_depth)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.interrupt_task_sync();
    }
}

async fn submission_loop(
    sess: Arc<Session>,
    turn_context: TurnContext,
    config: Arc<Config>,
    rx_sub: Receiver<Submission>,
    mailbox_tx: MailboxSender,
    mailbox_rx: MailboxReceiver,
    dispatcher: Option<Arc<MailboxDispatcherClient>>,
) {
    // Wrap once to avoid cloning TurnContext for each task.
    let mut turn_context = Arc::new(turn_context);
    // To break out of this loop, send Op::Shutdown.
    let submission_rx = rx_sub;
    'outer: while let Some((sub, mailbox_enabled)) = {
        let mailbox_enabled = mailbox_feature_enabled();
        #[allow(clippy::let_unit_value)]
        let next = tokio::select! {
            biased;
            envelope = mailbox_rx.recv(), if mailbox_enabled => {
                match envelope {
                    Ok(envelope) => {
                        if let Some(client) = dispatcher.as_ref() {
                            let client = Arc::clone(client);
                            let sess_clone = Arc::clone(&sess);
                            let mailbox_tx_clone = mailbox_tx.clone();
                            tokio::spawn(async move {
                                dispatch_mailbox_envelope(client, sess_clone, mailbox_tx_clone, envelope).await;
                            });
                        } else {
                            handle_mailbox_delivery(&sess, &mailbox_tx, envelope).await;
                        }
                        continue 'outer;
                    }
                    Err(_) => {
                        if submission_rx.is_closed() {
                            break 'outer;
                        }
                        continue 'outer;
                    }
                }
            }
            sub = submission_rx.recv() => {
                match sub {
                    Ok(sub) => (sub, mailbox_enabled),
                    Err(_) => break 'outer,
                }
            }
        };
        Some(next)
    } {
        debug!(?sub, "Submission");
        match sub.op {
            Op::Interrupt => {
                sess.interrupt_task().await;
            }
            Op::OverrideTurnContext {
                cwd,
                approval_policy,
                sandbox_policy,
                model,
                effort,
                summary,
            } => {
                // Recalculate the persistent turn context with provided overrides.
                let prev = Arc::clone(&turn_context);
                let provider = prev.client.get_provider();

                // Effective model + family
                let (effective_model, effective_family) = if let Some(ref m) = model {
                    let fam =
                        find_family_for_model(m).unwrap_or_else(|| config.model_family.clone());
                    (m.clone(), fam)
                } else {
                    (prev.client.get_model(), prev.client.get_model_family())
                };

                // Effective reasoning settings
                let effective_effort = effort.unwrap_or(prev.client.get_reasoning_effort());
                let effective_summary = summary.unwrap_or(prev.client.get_reasoning_summary());

                let auth_manager = prev.client.get_auth_manager();

                // Build updated config for the client
                let mut updated_config = (*config).clone();
                updated_config.model = effective_model.clone();
                updated_config.model_family = effective_family.clone();
                if let Some(model_info) = get_model_info(&effective_family) {
                    updated_config.model_context_window = Some(model_info.context_window);
                }

                let otel_event_manager = prev.client.get_otel_event_manager().with_model(
                    updated_config.model.as_str(),
                    updated_config.model_family.slug.as_str(),
                );

                let client = ModelClient::new(
                    Arc::new(updated_config),
                    auth_manager,
                    otel_event_manager,
                    provider,
                    effective_effort,
                    effective_summary,
                    sess.conversation_id,
                );

                let new_approval_policy = approval_policy.unwrap_or(prev.approval_policy);
                let new_sandbox_policy = sandbox_policy
                    .clone()
                    .unwrap_or(prev.sandbox_policy.clone());
                let new_cwd = cwd.clone().unwrap_or_else(|| prev.cwd.clone());

                let include_vm_pty_tool =
                    config.include_vm_pty_tool && sess.services.vm_pty_client.is_some();
                let include_vm_pty_open_tool =
                    config.include_vm_pty_open_tool && include_vm_pty_tool;
                let route_shell_via_pty = include_vm_pty_tool
                    && matches!(config.active_profile.as_deref(), Some("worker-pty-sandbox"));
                let tools_config = ToolsConfig::new(&ToolsConfigParams {
                    model_family: &effective_family,
                    include_plan_tool: config.include_plan_tool,
                    include_apply_patch_tool: config.include_apply_patch_tool,
                    include_web_search_request: config.tools_web_search_request,
                    use_streamable_shell_tool: config.use_experimental_streamable_shell_tool,
                    include_shell_tool: config.include_shell_tool,
                    include_view_image_tool: config.include_view_image_tool,
                    include_vm_pty_tool,
                    include_vm_pty_open_tool,
                    route_shell_via_pty,
                    experimental_unified_exec_tool: config.use_experimental_unified_exec_tool,
                    debug_tools: config.debug_tools,
                });

                let new_turn_context = TurnContext {
                    client,
                    tools_config,
                    user_instructions: prev.user_instructions.clone(),
                    base_instructions: prev.base_instructions.clone(),
                    approval_policy: new_approval_policy,
                    sandbox_policy: new_sandbox_policy.clone(),
                    shell_environment_policy: prev.shell_environment_policy.clone(),
                    cwd: new_cwd.clone(),
                    vm_pty_default_vm_id: config.vm_pty_default_vm_id.clone(),
                    vm_pty_open_timeout: config.vm_pty_open_timeout,
                    vm_pty_open_blocking: config.vm_pty_open_blocking,
                    vm_pty_max_concurrent_per_worker: config.vm_pty_max_concurrent_per_worker,
                    is_review_mode: false,
                    final_output_json_schema: None,
                };

                // Install the new persistent context for subsequent tasks/turns.
                turn_context = Arc::new(new_turn_context);

                // Optionally persist changes to model / effort
                if cwd.is_some() || approval_policy.is_some() || sandbox_policy.is_some() {
                    sess.record_conversation_items(&[ResponseItem::from(EnvironmentContext::new(
                        cwd,
                        approval_policy,
                        sandbox_policy,
                        // Shell is not configurable from turn to turn
                        None,
                    ))])
                    .await;
                }
            }
            Op::UserInput { items } => {
                turn_context
                    .client
                    .get_otel_event_manager()
                    .user_prompt(&items);
                // attempt to inject input into current task
                if let Err(items) = sess.inject_input(items).await {
                    // no current task, spawn a new one
                    sess.spawn_task(Arc::clone(&turn_context), sub.id, items, RegularTask)
                        .await;
                }
            }
            Op::UserTurn {
                items,
                cwd,
                approval_policy,
                sandbox_policy,
                model,
                effort,
                summary,
                final_output_json_schema,
            } => {
                turn_context
                    .client
                    .get_otel_event_manager()
                    .user_prompt(&items);
                // attempt to inject input into current task
                if let Err(items) = sess.inject_input(items).await {
                    // Derive a fresh TurnContext for this turn using the provided overrides.
                    let provider = turn_context.client.get_provider();
                    let auth_manager = turn_context.client.get_auth_manager();

                    // Derive a model family for the requested model; fall back to the session's.
                    let model_family = find_family_for_model(&model)
                        .unwrap_or_else(|| config.model_family.clone());

                    // Create a per‑turn Config clone with the requested model/family.
                    let mut per_turn_config = (*config).clone();
                    per_turn_config.model = model.clone();
                    per_turn_config.model_family = model_family.clone();
                    if let Some(model_info) = get_model_info(&model_family) {
                        per_turn_config.model_context_window = Some(model_info.context_window);
                    }

                    let otel_event_manager =
                        turn_context.client.get_otel_event_manager().with_model(
                            per_turn_config.model.as_str(),
                            per_turn_config.model_family.slug.as_str(),
                        );

                    // Build a new client with per‑turn reasoning settings.
                    // Reuse the same provider and session id; auth defaults to env/API key.
                    let client = ModelClient::new(
                        Arc::new(per_turn_config),
                        auth_manager,
                        otel_event_manager,
                        provider,
                        effort,
                        summary,
                        sess.conversation_id,
                    );

                    let fresh_turn_context = TurnContext {
                        client,
                        tools_config: {
                            let include_vm_pty_tool = config.include_vm_pty_tool
                                && sess.services.vm_pty_client.is_some();
                            let include_vm_pty_open_tool =
                                config.include_vm_pty_open_tool && include_vm_pty_tool;
                            let route_shell_via_pty = include_vm_pty_tool
                                && matches!(config.active_profile.as_deref(), Some("worker-pty-sandbox"));
                            ToolsConfig::new(&ToolsConfigParams {
                                model_family: &model_family,
                                include_plan_tool: config.include_plan_tool,
                                include_apply_patch_tool: config.include_apply_patch_tool,
                                include_web_search_request: config.tools_web_search_request,
                                use_streamable_shell_tool: config
                                    .use_experimental_streamable_shell_tool,
                                include_shell_tool: config.include_shell_tool,
                                include_view_image_tool: config.include_view_image_tool,
                                include_vm_pty_tool,
                                include_vm_pty_open_tool,
                                route_shell_via_pty,
                                experimental_unified_exec_tool: config
                                    .use_experimental_unified_exec_tool,
                                debug_tools: config.debug_tools,
                            })
                        },
                        user_instructions: turn_context.user_instructions.clone(),
                        base_instructions: turn_context.base_instructions.clone(),
                        approval_policy,
                        sandbox_policy,
                        shell_environment_policy: turn_context.shell_environment_policy.clone(),
                        cwd,
                        vm_pty_default_vm_id: config.vm_pty_default_vm_id.clone(),
                        vm_pty_open_timeout: config.vm_pty_open_timeout,
                        vm_pty_open_blocking: config.vm_pty_open_blocking,
                        vm_pty_max_concurrent_per_worker: config.vm_pty_max_concurrent_per_worker,
                        is_review_mode: false,
                        final_output_json_schema,
                    };

                    // if the environment context has changed, record it in the conversation history
                    let previous_env_context = EnvironmentContext::from(turn_context.as_ref());
                    let new_env_context = EnvironmentContext::from(&fresh_turn_context);
                    if !new_env_context.equals_except_shell(&previous_env_context) {
                        let env_response_item = ResponseItem::from(new_env_context);
                        sess.record_conversation_items(std::slice::from_ref(&env_response_item))
                            .await;
                        for msg in map_response_item_to_event_messages(
                            &env_response_item,
                            sess.show_raw_agent_reasoning(),
                        ) {
                            let event = Event {
                                id: sub.id.clone(),
                                msg,
                            };
                            sess.send_event(event).await;
                        }
                    }

                    // Install the new persistent context for subsequent tasks/turns.
                    turn_context = Arc::new(fresh_turn_context);

                    // no current task, spawn a new one with the per-turn context
                    sess.spawn_task(Arc::clone(&turn_context), sub.id, items, RegularTask)
                        .await;
                }
            }
            Op::ExecApproval { id, decision } => match decision {
                ReviewDecision::Abort => {
                    sess.interrupt_task().await;
                }
                other => sess.notify_approval(&id, other).await,
            },
            Op::PatchApproval { id, decision } => match decision {
                ReviewDecision::Abort => {
                    sess.interrupt_task().await;
                }
                other => sess.notify_approval(&id, other).await,
            },
            Op::AddToHistory { text } => {
                let id = sess.conversation_id;
                let config = config.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::message_history::append_entry(&text, &id, &config).await
                    {
                        warn!("failed to append to message history: {e}");
                    }
                });
            }

            Op::GetHistoryEntryRequest { offset, log_id } => {
                let config = config.clone();
                let sess_clone = sess.clone();
                let sub_id = sub.id.clone();

                tokio::spawn(async move {
                    // Run lookup in blocking thread because it does file IO + locking.
                    let entry_opt = tokio::task::spawn_blocking(move || {
                        crate::message_history::lookup(log_id, offset, &config)
                    })
                    .await
                    .unwrap_or(None);

                    let event = Event {
                        id: sub_id,
                        msg: EventMsg::GetHistoryEntryResponse(
                            crate::protocol::GetHistoryEntryResponseEvent {
                                offset,
                                log_id,
                                entry: entry_opt.map(|e| {
                                    codex_protocol::message_history::HistoryEntry {
                                        conversation_id: e.session_id,
                                        ts: e.ts,
                                        text: e.text,
                                    }
                                }),
                            },
                        ),
                    };

                    sess_clone.send_event(event).await;
                });
            }
            Op::ListMcpTools => {
                let sub_id = sub.id.clone();

                // This is a cheap lookup from the connection manager's cache.
                let tools = sess.services.mcp_connection_manager.list_all_tools();
                let auth_statuses = compute_auth_statuses(
                    config.mcp_servers.iter(),
                    config.mcp_oauth_credentials_store_mode,
                )
                .await;
                let event = Event {
                    id: sub_id,
                    msg: EventMsg::McpListToolsResponse(
                        crate::protocol::McpListToolsResponseEvent {
                            tools,
                            auth_statuses,
                        },
                    ),
                };
                sess.send_event(event).await;
            }
            Op::MailboxEnvelope { envelope } => {
                let _ = sess
                    .enqueue_mailbox_envelope(sub.id.clone(), envelope.clone(), mailbox_enabled)
                    .await;
            }
            Op::ListCustomPrompts => {
                let sub_id = sub.id.clone();

                let custom_prompts: Vec<CustomPrompt> =
                    if let Some(dir) = crate::custom_prompts::default_prompts_dir() {
                        crate::custom_prompts::discover_prompts_in(&dir).await
                    } else {
                        Vec::new()
                    };

                let event = Event {
                    id: sub_id,
                    msg: EventMsg::ListCustomPromptsResponse(ListCustomPromptsResponseEvent {
                        custom_prompts,
                    }),
                };
                sess.send_event(event).await;
            }
            Op::Compact => {
                // Attempt to inject input into current task
                if let Err(items) = sess
                    .inject_input(vec![InputItem::Text {
                        text: compact::SUMMARIZATION_PROMPT.to_string(),
                    }])
                    .await
                {
                    sess.spawn_task(Arc::clone(&turn_context), sub.id, items, CompactTask)
                        .await;
                }
            }
            Op::Shutdown => {
                sess.abort_all_tasks(TurnAbortReason::Interrupted).await;
                info!("Shutting down Codex instance");

                // Gracefully flush and shutdown rollout recorder on session end so tests
                // that inspect the rollout file do not race with the background writer.
                let recorder_opt = {
                    let mut guard = sess.services.rollout.lock().await;
                    guard.take()
                };
                if let Some(rec) = recorder_opt
                    && let Err(e) = rec.shutdown().await
                {
                    warn!("failed to shutdown rollout recorder: {e}");
                    let event = Event {
                        id: sub.id.clone(),
                        msg: EventMsg::Error(ErrorEvent {
                            message: "Failed to shutdown rollout recorder".to_string(),
                        }),
                    };
                    sess.send_event(event).await;
                }

                let event = Event {
                    id: sub.id.clone(),
                    msg: EventMsg::ShutdownComplete,
                };
                sess.send_event(event).await;
                break;
            }
            Op::GetPath => {
                let sub_id = sub.id.clone();
                // Flush rollout writes before returning the path so readers observe a consistent file.
                let (path, rec_opt) = {
                    let guard = sess.services.rollout.lock().await;
                    match guard.as_ref() {
                        Some(rec) => (rec.get_rollout_path(), Some(rec.clone())),
                        None => {
                            error!("rollout recorder not found");
                            continue;
                        }
                    }
                };
                if let Some(rec) = rec_opt
                    && let Err(e) = rec.flush().await
                {
                    warn!("failed to flush rollout recorder before GetHistory: {e}");
                }
                let event = Event {
                    id: sub_id.clone(),
                    msg: EventMsg::ConversationPath(ConversationPathResponseEvent {
                        conversation_id: sess.conversation_id,
                        path,
                    }),
                };
                sess.send_event(event).await;
            }
            Op::Review { review_request } => {
                spawn_review_thread(
                    sess.clone(),
                    config.clone(),
                    turn_context.clone(),
                    sub.id,
                    review_request,
                )
                .await;
            }
            _ => {
                // Ignore unknown ops; enum is non_exhaustive to allow extensions.
            }
        }
    }
    debug!("Agent loop exited");
}

async fn handle_mailbox_delivery(
    sess: &Arc<Session>,
    mailbox_tx: &MailboxSender,
    envelope: MailboxEnvelope,
) {
    let routing_mode = envelope
        .message
        .metadata
        .get(ROUTING_MODE_METADATA_KEY)
        .and_then(|value| value.as_str())
        .unwrap_or_else(|| {
            if envelope
                .message
                .audience
                .as_ref()
                .and_then(|audience| audience.conversation_id)
                .is_some()
            {
                "conversation_id"
            } else {
                "unknown"
            }
        });
    let remaining = mailbox_tx.len();
    let observed_at = OffsetDateTime::now_utc();
    let expires_at = envelope
        .message
        .expires_at
        .map(|ts| ts.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into()));
    let ack_deadline = envelope
        .message
        .ack_policy
        .deadline
        .map(|ts| ts.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into()));
    let rate_scope = envelope
        .message
        .rate_limit
        .as_ref()
        .map(|r| format!("{:?}", r.scope).to_lowercase());
    let rate_capacity = envelope
        .message
        .rate_limit
        .as_ref()
        .and_then(|r| r.capacity);
    let rate_interval = envelope
        .message
        .rate_limit
        .as_ref()
        .and_then(|r| r.interval_seconds);

    info!(
        target: "codex::mailbox",
        event = "delivered",
        submission_id = %envelope.submission_id,
        conversation_id = %sess.conversation_id,
        message_id = %envelope.message.message_id,
        routing_mode,
        sender_id = %envelope.message.sender.id,
        sender_role = ?envelope.message.sender.role,
        priority = ?envelope.message.priority,
        request_id = envelope.message.audit.request_id.as_deref().unwrap_or(""),
        ack_mode = ?envelope.message.ack_policy.mode,
        ack_deadline = ack_deadline.as_deref(),
        expires_at = expires_at.as_deref(),
        rate_scope = rate_scope.as_deref(),
        rate_capacity,
        rate_interval,
        queue_depth = remaining,
    );

    let ingress = sess.mailbox_ingress(&envelope.message);
    let mut delivery_event = MailboxDeliveryEvent {
        message: envelope.message.clone(),
        state: MailboxDeliveryState::Delivered,
        queue_depth: Some(remaining),
        observed_at: Some(observed_at),
        correlation_id: Some(envelope.submission_id.clone()),
        ingress: Some(ingress.clone()),
        delivery_latency_ms: None,
    };

    {
        let labels = MailboxTelemetryLabels::new(ingress, &delivery_event.message);
        let mut telemetry = sess.mailbox_metrics.lock().await;
        telemetry.record_delivered(&mut delivery_event, &labels);
    }

    let event = Event {
        id: envelope.submission_id.clone(),
        msg: EventMsg::MailboxDelivery(delivery_event.clone()),
    };
    sess.send_event(event).await;
    sess.notify_mailbox_delivery_listeners(&delivery_event)
        .await;
    sess.notify_mailbox_predicate_waiters(&delivery_event).await;
}

async fn dispatch_mailbox_envelope(
    client: Arc<MailboxDispatcherClient>,
    sess: Arc<Session>,
    mailbox_tx: MailboxSender,
    envelope: MailboxEnvelope,
) {
    let routing_mode = envelope
        .message
        .metadata
        .get(ROUTING_MODE_METADATA_KEY)
        .and_then(|value| value.as_str())
        .unwrap_or_else(|| {
            if envelope
                .message
                .audience
                .as_ref()
                .and_then(|audience| audience.conversation_id)
                .is_some()
            {
                "conversation_id"
            } else {
                "unknown"
            }
        });
    let target_id = envelope
        .message
        .audience
        .as_ref()
        .and_then(|aud| aud.conversation_id);

    let Some(target_conversation_id) = target_id else {
        warn!(
            target: "codex::mailbox",
            event = "mailbox.dispatch.invalid_target",
            submission_id = %envelope.submission_id,
            message_id = %envelope.message.message_id,
            "mailbox envelope missing conversation_id audience"
        );
        emit_mailbox_dispatch_error(
            &sess,
            &envelope,
            "Mailbox dispatcher unavailable (missing conversation target)".to_string(),
        )
        .await;
        return;
    };

    let source_conversation_uuid = match Uuid::parse_str(&sess.get_conversation_id().to_string()) {
        Ok(uuid) => uuid,
        Err(err) => {
            warn!(
                target: "codex::mailbox",
                event = "mailbox.dispatch.invalid_source",
                conversation = %sess.get_conversation_id(),
                ?err,
                "failed to parse source conversation id"
            );
            Uuid::nil()
        }
    };

    let request_id = envelope
        .message
        .audit
        .request_id
        .clone()
        .unwrap_or_default();

    info!(
        target: "codex::mailbox",
        event = "mailbox.dispatch.request",
        submission_id = %envelope.submission_id,
        message_id = %envelope.message.message_id,
        source_conversation = %source_conversation_uuid,
        target_conversation = %target_conversation_id,
        routing_mode,
        request_id = %request_id,
        endpoint = %client.endpoint().display(),
        "dispatching mailbox envelope via dispatcher"
    );

    match client
        .dispatch(
            &envelope.submission_id,
            source_conversation_uuid,
            target_conversation_id,
            &envelope.message,
        )
        .await
    {
        Ok(outcome) => match outcome.status {
            DispatchStatus::Delivered => {
                info!(
                    target: "codex::mailbox",
                    event = "mailbox.dispatch.delivered",
                    submission_id = %envelope.submission_id,
                    message_id = %envelope.message.message_id,
                    routing_mode,
                    queue_depth = outcome.queue_depth,
                    "dispatcher reported delivery; forwarding to local handlers"
                );
                handle_mailbox_delivery(&sess, &mailbox_tx, envelope).await;
                return;
            }
            DispatchStatus::QueueFull => {
                let capacity = outcome.capacity.unwrap_or(0);
                let message = format!("Mailbox queue is full (capacity = {capacity})");
                emit_mailbox_dispatch_error(&sess, &envelope, message).await;
                return;
            }
            DispatchStatus::InvalidRequest => {
                let mut message = "Mailbox dispatcher unavailable".to_string();
                if let Some(detail) = outcome.detail {
                    if !detail.is_empty() {
                        message.push_str(": ");
                        message.push_str(&detail);
                    }
                }
                emit_mailbox_dispatch_error(&sess, &envelope, message).await;
                return;
            }
            DispatchStatus::Disabled
            | DispatchStatus::Timeout
            | DispatchStatus::UnknownSession
            | DispatchStatus::TransportError
            | DispatchStatus::DispatcherClosed => {
                warn!(
                    target: "codex::mailbox",
                    event = "mailbox.dispatch.fallback",
                    submission_id = %envelope.submission_id,
                    message_id = %envelope.message.message_id,
                    routing_mode,
                    status = ?outcome.status,
                    "dispatcher unavailable; falling back to local delivery"
                );
                handle_mailbox_delivery(&sess, &mailbox_tx, envelope).await;
                return;
            }
        },
        Err(err) => {
            error!(
                target: "codex::mailbox",
                event = "mailbox.dispatch.error",
                source_conversation = %source_conversation_uuid,
                %target_conversation_id,
                %request_id,
                submission_id = %envelope.submission_id,
                message_id = %envelope.message.message_id,
                routing_mode,
                ?err,
                "failed to forward mailbox envelope via dispatcher"
            );

            warn!(
                target: "codex::mailbox",
                event = "mailbox.dispatch.fallback",
                submission_id = %envelope.submission_id,
                message_id = %envelope.message.message_id,
                routing_mode,
                "dispatcher error; falling back to local delivery"
            );

            handle_mailbox_delivery(&sess, &mailbox_tx, envelope).await;
            return;
        }
    }
}

async fn emit_mailbox_dispatch_error(
    sess: &Arc<Session>,
    envelope: &MailboxEnvelope,
    message: String,
) {
    warn!(
        target: "codex::mailbox",
        event = "mailbox.dispatch.failed",
        submission_id = %envelope.submission_id,
        message_id = %envelope.message.message_id,
        sender_id = %envelope.message.sender.id,
        detail = %message,
        "dispatcher delivery failed"
    );

    let event = Event {
        id: envelope.submission_id.clone(),
        msg: EventMsg::Error(ErrorEvent { message }),
    };
    sess.send_event(event).await;
    sess.cancel_mailbox_delivery_listener(envelope.message.message_id)
        .await;
}

/// Spawn a review thread using the given prompt.
async fn spawn_review_thread(
    sess: Arc<Session>,
    config: Arc<Config>,
    parent_turn_context: Arc<TurnContext>,
    sub_id: String,
    review_request: ReviewRequest,
) {
    let model = config.review_model.clone();
    let review_model_family = find_family_for_model(&model)
        .unwrap_or_else(|| parent_turn_context.client.get_model_family());
    let tools_config = ToolsConfig::new(&ToolsConfigParams {
        model_family: &review_model_family,
        include_plan_tool: false,
        include_apply_patch_tool: config.include_apply_patch_tool,
        include_web_search_request: false,
        use_streamable_shell_tool: false,
        include_shell_tool: false,
        include_view_image_tool: false,
        include_vm_pty_tool: false,
        include_vm_pty_open_tool: false,
        route_shell_via_pty: false,
        experimental_unified_exec_tool: config.use_experimental_unified_exec_tool,
        debug_tools: config.debug_tools,
    });

    let base_instructions = REVIEW_PROMPT.to_string();
    let review_prompt = review_request.prompt.clone();
    let provider = parent_turn_context.client.get_provider();
    let auth_manager = parent_turn_context.client.get_auth_manager();
    let model_family = review_model_family.clone();

    // Build per‑turn client with the requested model/family.
    let mut per_turn_config = (*config).clone();
    per_turn_config.model = model.clone();
    per_turn_config.model_family = model_family.clone();
    per_turn_config.model_reasoning_effort = Some(ReasoningEffortConfig::Low);
    per_turn_config.model_reasoning_summary = ReasoningSummaryConfig::Detailed;
    if let Some(model_info) = get_model_info(&model_family) {
        per_turn_config.model_context_window = Some(model_info.context_window);
    }

    let otel_event_manager = parent_turn_context
        .client
        .get_otel_event_manager()
        .with_model(
            per_turn_config.model.as_str(),
            per_turn_config.model_family.slug.as_str(),
        );

    let per_turn_config = Arc::new(per_turn_config);
    let client = ModelClient::new(
        per_turn_config.clone(),
        auth_manager,
        otel_event_manager,
        provider,
        per_turn_config.model_reasoning_effort,
        per_turn_config.model_reasoning_summary,
        sess.conversation_id,
    );

    let review_turn_context = TurnContext {
        client,
        tools_config,
        user_instructions: None,
        base_instructions: Some(base_instructions.clone()),
        approval_policy: parent_turn_context.approval_policy,
        sandbox_policy: parent_turn_context.sandbox_policy.clone(),
        shell_environment_policy: parent_turn_context.shell_environment_policy.clone(),
        cwd: parent_turn_context.cwd.clone(),
        vm_pty_default_vm_id: config.vm_pty_default_vm_id.clone(),
        vm_pty_open_timeout: config.vm_pty_open_timeout,
        vm_pty_open_blocking: config.vm_pty_open_blocking,
        vm_pty_max_concurrent_per_worker: config.vm_pty_max_concurrent_per_worker,
        is_review_mode: true,
        final_output_json_schema: None,
    };

    // Seed the child task with the review prompt as the initial user message.
    let input: Vec<InputItem> = vec![InputItem::Text {
        text: review_prompt,
    }];
    let tc = Arc::new(review_turn_context);

    // Clone sub_id for the upcoming announcement before moving it into the task.
    let sub_id_for_event = sub_id.clone();
    sess.spawn_task(tc.clone(), sub_id, input, ReviewTask).await;

    // Announce entering review mode so UIs can switch modes.
    sess.send_event(Event {
        id: sub_id_for_event,
        msg: EventMsg::EnteredReviewMode(review_request),
    })
    .await;
}

/// Takes a user message as input and runs a loop where, at each turn, the model
/// replies with either:
///
/// - requested function calls
/// - an assistant message
///
/// While it is possible for the model to return multiple of these items in a
/// single turn, in practice, we generally one item per turn:
///
/// - If the model requests a function call, we execute it and send the output
///   back to the model in the next turn.
/// - If the model sends only an assistant message, we record it in the
///   conversation history and consider the task complete.
///
/// Review mode: when `turn_context.is_review_mode` is true, the turn runs in an
/// isolated in-memory thread without the parent session's prior history or
/// user_instructions. Emits ExitedReviewMode upon final review message.
pub(crate) async fn run_task(
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
    sub_id: String,
    input: Vec<InputItem>,
) -> Option<String> {
    if input.is_empty() {
        return None;
    }
    let event = Event {
        id: sub_id.clone(),
        msg: EventMsg::TaskStarted(TaskStartedEvent {
            model_context_window: turn_context.client.get_model_context_window(),
        }),
    };
    sess.send_event(event).await;

    let initial_input_for_turn: ResponseInputItem = ResponseInputItem::from(input);
    // For review threads, keep an isolated in-memory history so the
    // model sees a fresh conversation without the parent session's history.
    // For normal turns, continue recording to the session history as before.
    let is_review_mode = turn_context.is_review_mode;
    let mut review_thread_history: Vec<ResponseItem> = Vec::new();
    if is_review_mode {
        // Seed review threads with environment context so the model knows the working directory.
        review_thread_history.extend(sess.build_initial_context(turn_context.as_ref()));
        review_thread_history.push(initial_input_for_turn.into());
    } else {
        sess.record_input_and_rollout_usermsg(&initial_input_for_turn)
            .await;
    }

    let mut last_agent_message: Option<String> = None;
    // Although from the perspective of codex.rs, TurnDiffTracker has the lifecycle of a Task which contains
    // many turns, from the perspective of the user, it is a single turn.
    let turn_diff_tracker = Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new()));
    let mut auto_compact_recently_attempted = false;

    loop {
        // Note that pending_input would be something like a message the user
        // submitted through the UI while the model was running. Though the UI
        // may support this, the model might not.
        let pending_input = sess
            .get_pending_input()
            .await
            .into_iter()
            .map(ResponseItem::from)
            .collect::<Vec<ResponseItem>>();

        // Construct the input that we will send to the model.
        //
        // - For review threads, use the isolated in-memory history so the
        //   model sees a fresh conversation (no parent history/user_instructions).
        //
        // - For normal turns, use the session's full history. When using the
        //   chat completions API (or ZDR clients), the model needs the full
        //   conversation history on each turn. The rollout file, however, should
        //   only record the new items that originated in this turn so that it
        //   represents an append-only log without duplicates.
        let turn_input: Vec<ResponseItem> = if is_review_mode {
            if !pending_input.is_empty() {
                review_thread_history.extend(pending_input);
            }
            review_thread_history.clone()
        } else {
            // Preserve existing behavior: record pending items to history and rollout.
            sess.record_conversation_items(&pending_input).await;

            // Route prompt assembly through the sliding window prompt builder.
            // If summaries are unavailable, the builder returns `history + pending` unchanged.
            let history = sess.history_snapshot().await;

            // Snapshot summaries state if the background service is enabled.
            let summaries_state_snapshot: SlidingSummariesState = {
                let svc_opt = {
                    let guard = sess.services.summaries.lock().await;
                    guard.as_ref().map(|svc| svc.state_arc())
                };
                if let Some(state_arc) = svc_opt {
                    let s = state_arc.lock().await;
                    s.clone()
                } else {
                    SlidingSummariesState::default()
                }
            };

            // Budget is currently measured in item count; pass the full length
            // so default behavior (no trimming) is preserved unless a future
            // change opts into a smaller budget.
            let budget = history.len().saturating_add(pending_input.len());
            build_sliding_window_prompt(&summaries_state_snapshot, &history, &pending_input, budget)
        };

        let turn_input_messages: Vec<String> = turn_input
            .iter()
            .filter_map(|item| match item {
                ResponseItem::Message { content, .. } => Some(content),
                _ => None,
            })
            .flat_map(|content| {
                content.iter().filter_map(|item| match item {
                    ContentItem::OutputText { text } => Some(text.clone()),
                    _ => None,
                })
            })
            .collect();
        match run_turn(
            Arc::clone(&sess),
            Arc::clone(&turn_context),
            Arc::clone(&turn_diff_tracker),
            sub_id.clone(),
            turn_input,
        )
        .await
        {
            Ok(turn_output) => {
                let TurnRunResult {
                    processed_items,
                    total_token_usage,
                } = turn_output;
                let limit = turn_context
                    .client
                    .get_auto_compact_token_limit()
                    .unwrap_or(i64::MAX);
                let total_usage_tokens = total_token_usage
                    .as_ref()
                    .map(TokenUsage::tokens_in_context_window);
                let token_limit_reached = total_usage_tokens
                    .map(|tokens| (tokens as i64) >= limit)
                    .unwrap_or(false);
                let mut items_to_record_in_conversation_history = Vec::<ResponseItem>::new();
                let mut responses = Vec::<ResponseInputItem>::new();
                for processed_response_item in processed_items {
                    let ProcessedResponseItem { item, response } = processed_response_item;
                    match (&item, &response) {
                        (ResponseItem::Message { role, .. }, None) if role == "assistant" => {
                            // If the model returned a message, we need to record it.
                            items_to_record_in_conversation_history.push(item);
                        }
                        (
                            ResponseItem::LocalShellCall { .. },
                            Some(ResponseInputItem::FunctionCallOutput { call_id, output }),
                        ) => {
                            items_to_record_in_conversation_history.push(item);
                            items_to_record_in_conversation_history.push(
                                ResponseItem::FunctionCallOutput {
                                    call_id: call_id.clone(),
                                    output: output.clone(),
                                },
                            );
                        }
                        (
                            ResponseItem::FunctionCall { .. },
                            Some(ResponseInputItem::FunctionCallOutput { call_id, output }),
                        ) => {
                            items_to_record_in_conversation_history.push(item);
                            items_to_record_in_conversation_history.push(
                                ResponseItem::FunctionCallOutput {
                                    call_id: call_id.clone(),
                                    output: output.clone(),
                                },
                            );
                        }
                        (
                            ResponseItem::CustomToolCall { .. },
                            Some(ResponseInputItem::CustomToolCallOutput { call_id, output }),
                        ) => {
                            items_to_record_in_conversation_history.push(item);
                            items_to_record_in_conversation_history.push(
                                ResponseItem::CustomToolCallOutput {
                                    call_id: call_id.clone(),
                                    output: output.clone(),
                                },
                            );
                        }
                        (
                            ResponseItem::FunctionCall { .. },
                            Some(ResponseInputItem::McpToolCallOutput { call_id, result }),
                        ) => {
                            items_to_record_in_conversation_history.push(item);
                            let output = match result {
                                Ok(call_tool_result) => {
                                    convert_call_tool_result_to_function_call_output_payload(
                                        call_tool_result,
                                    )
                                }
                                Err(err) => FunctionCallOutputPayload {
                                    content: err.clone(),
                                    success: Some(false),
                                },
                            };
                            items_to_record_in_conversation_history.push(
                                ResponseItem::FunctionCallOutput {
                                    call_id: call_id.clone(),
                                    output,
                                },
                            );
                        }
                        (
                            ResponseItem::Reasoning {
                                id,
                                summary,
                                content,
                                encrypted_content,
                            },
                            None,
                        ) => {
                            items_to_record_in_conversation_history.push(ResponseItem::Reasoning {
                                id: id.clone(),
                                summary: summary.clone(),
                                content: content.clone(),
                                encrypted_content: encrypted_content.clone(),
                            });
                        }
                        _ => {
                            warn!("Unexpected response item: {item:?} with response: {response:?}");
                        }
                    };
                    if let Some(response) = response {
                        responses.push(response);
                    }
                }

                // Only attempt to take the lock if there is something to record.
                if !items_to_record_in_conversation_history.is_empty() {
                    if is_review_mode {
                        review_thread_history
                            .extend(items_to_record_in_conversation_history.clone());
                    } else {
                        sess.record_conversation_items(&items_to_record_in_conversation_history)
                            .await;
                    }
                }

                if token_limit_reached {
                    if auto_compact_recently_attempted {
                        let limit_str = limit.to_string();
                        let current_tokens = total_usage_tokens
                            .map(|tokens| tokens.to_string())
                            .unwrap_or_else(|| "unknown".to_string());
                        let event = Event {
                            id: sub_id.clone(),
                            msg: EventMsg::Error(ErrorEvent {
                                message: format!(
                                    "Conversation is still above the token limit after automatic summarization (limit {limit_str}, current {current_tokens}). Please start a new session or trim your input."
                                ),
                            }),
                        };
                        sess.send_event(event).await;
                        break;
                    }
                    auto_compact_recently_attempted = true;
                    compact::run_inline_auto_compact_task(sess.clone(), turn_context.clone()).await;
                    continue;
                }

                auto_compact_recently_attempted = false;

                if responses.is_empty() {
                    last_agent_message = get_last_assistant_message_from_turn(
                        &items_to_record_in_conversation_history,
                    );
                    sess.notifier()
                        .notify(&UserNotification::AgentTurnComplete {
                            turn_id: sub_id.clone(),
                            input_messages: turn_input_messages,
                            last_assistant_message: last_agent_message.clone(),
                        });
                    break;
                }
                continue;
            }
            Err(e) => {
                info!("Turn error: {e:#}");
                let event = Event {
                    id: sub_id.clone(),
                    msg: EventMsg::Error(ErrorEvent {
                        message: e.to_string(),
                    }),
                };
                sess.send_event(event).await;
                // let the user continue the conversation
                break;
            }
        }
    }

    // If this was a review thread and we have a final assistant message,
    // try to parse it as a ReviewOutput.
    //
    // If parsing fails, construct a minimal ReviewOutputEvent using the plain
    // text as the overall explanation. Else, just exit review mode with None.
    //
    // Emits an ExitedReviewMode event with the parsed review output.
    if turn_context.is_review_mode {
        exit_review_mode(
            sess.clone(),
            sub_id.clone(),
            last_agent_message.as_deref().map(parse_review_output_event),
        )
        .await;
    }

    last_agent_message
}

/// Parse the review output; when not valid JSON, build a structured
/// fallback that carries the plain text as the overall explanation.
///
/// Returns: a ReviewOutputEvent parsed from JSON or a fallback populated from text.
fn parse_review_output_event(text: &str) -> ReviewOutputEvent {
    // Try direct parse first
    if let Ok(ev) = serde_json::from_str::<ReviewOutputEvent>(text) {
        return ev;
    }
    // If wrapped in markdown fences or extra prose, attempt to extract the first JSON object
    if let (Some(start), Some(end)) = (text.find('{'), text.rfind('}'))
        && start < end
        && let Some(slice) = text.get(start..=end)
        && let Ok(ev) = serde_json::from_str::<ReviewOutputEvent>(slice)
    {
        return ev;
    }
    // Not JSON – return a structured ReviewOutputEvent that carries
    // the plain text as the overall explanation.
    ReviewOutputEvent {
        overall_explanation: text.to_string(),
        ..Default::default()
    }
}

async fn run_turn(
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
    turn_diff_tracker: SharedTurnDiffTracker,
    sub_id: String,
    input: Vec<ResponseItem>,
) -> CodexResult<TurnRunResult> {
    let mcp_tools = sess.services.mcp_connection_manager.list_all_tools();
    let router = Arc::new(ToolRouter::from_config(
        &turn_context.tools_config,
        Some(mcp_tools),
    ));

    let model_supports_parallel = turn_context
        .client
        .get_model_family()
        .supports_parallel_tool_calls;
    let parallel_tool_calls = model_supports_parallel;
    let prompt = Prompt {
        input,
        tools: router.specs(),
        parallel_tool_calls,
        base_instructions_override: turn_context.base_instructions.clone(),
        output_schema: turn_context.final_output_json_schema.clone(),
    };

    let mut retries = 0;
    loop {
        match try_run_turn(
            Arc::clone(&router),
            Arc::clone(&sess),
            Arc::clone(&turn_context),
            Arc::clone(&turn_diff_tracker),
            &sub_id,
            &prompt,
        )
        .await
        {
            Ok(output) => return Ok(output),
            Err(CodexErr::Interrupted) => return Err(CodexErr::Interrupted),
            Err(CodexErr::EnvVar(var)) => return Err(CodexErr::EnvVar(var)),
            Err(e @ CodexErr::Fatal(_)) => return Err(e),
            Err(e @ CodexErr::ContextWindowExceeded) => {
                sess.set_total_tokens_full(&sub_id, &turn_context).await;
                return Err(e);
            }
            Err(CodexErr::UsageLimitReached(e)) => {
                let rate_limits = e.rate_limits.clone();
                if let Some(rate_limits) = rate_limits {
                    sess.update_rate_limits(&sub_id, rate_limits).await;
                }
                return Err(CodexErr::UsageLimitReached(e));
            }
            Err(CodexErr::UsageNotIncluded) => return Err(CodexErr::UsageNotIncluded),
            Err(e) => {
                // Use the configured provider-specific stream retry budget.
                let max_retries = turn_context.client.get_provider().stream_max_retries();
                if retries < max_retries {
                    retries += 1;
                    let delay = match e {
                        CodexErr::Stream(_, Some(delay)) => delay,
                        _ => backoff(retries),
                    };
                    warn!(
                        "stream disconnected - retrying turn ({retries}/{max_retries} in {delay:?})...",
                    );

                    // Surface retry information to any UI/front‑end so the
                    // user understands what is happening instead of staring
                    // at a seemingly frozen screen.
                    sess.notify_stream_error(
                        &sub_id,
                        format!("Re-connecting... {retries}/{max_retries}"),
                    )
                    .await;

                    tokio::time::sleep(delay).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

/// When the model is prompted, it returns a stream of events. Some of these
/// events map to a `ResponseItem`. A `ResponseItem` may need to be
/// "handled" such that it produces a `ResponseInputItem` that needs to be
/// sent back to the model on the next turn.
#[derive(Debug)]
pub(crate) struct ProcessedResponseItem {
    pub(crate) item: ResponseItem,
    pub(crate) response: Option<ResponseInputItem>,
}

#[derive(Debug)]
struct TurnRunResult {
    processed_items: Vec<ProcessedResponseItem>,
    total_token_usage: Option<TokenUsage>,
}

async fn try_run_turn(
    router: Arc<ToolRouter>,
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
    turn_diff_tracker: SharedTurnDiffTracker,
    sub_id: &str,
    prompt: &Prompt,
) -> CodexResult<TurnRunResult> {
    // call_ids that are part of this response.
    let completed_call_ids = prompt
        .input
        .iter()
        .filter_map(|ri| match ri {
            ResponseItem::FunctionCallOutput { call_id, .. } => Some(call_id),
            ResponseItem::LocalShellCall {
                call_id: Some(call_id),
                ..
            } => Some(call_id),
            ResponseItem::CustomToolCallOutput { call_id, .. } => Some(call_id),
            _ => None,
        })
        .collect::<Vec<_>>();

    // call_ids that were pending but are not part of this response.
    // This usually happens because the user interrupted the model before we responded to one of its tool calls
    // and then the user sent a follow-up message.
    let missing_calls = {
        prompt
            .input
            .iter()
            .filter_map(|ri| match ri {
                ResponseItem::FunctionCall { call_id, .. } => Some(call_id),
                ResponseItem::LocalShellCall {
                    call_id: Some(call_id),
                    ..
                } => Some(call_id),
                ResponseItem::CustomToolCall { call_id, .. } => Some(call_id),
                _ => None,
            })
            .filter_map(|call_id| {
                if completed_call_ids.contains(&call_id) {
                    None
                } else {
                    Some(call_id.clone())
                }
            })
            .map(|call_id| ResponseItem::CustomToolCallOutput {
                call_id,
                output: "aborted".to_string(),
            })
            .collect::<Vec<_>>()
    };
    let prompt: Cow<Prompt> = if missing_calls.is_empty() {
        Cow::Borrowed(prompt)
    } else {
        // Add the synthetic aborted missing calls to the beginning of the input to ensure all call ids have responses.
        let input = [missing_calls, prompt.input.clone()].concat();
        Cow::Owned(Prompt {
            input,
            ..prompt.clone()
        })
    };

    let rollout_item = RolloutItem::TurnContext(TurnContextItem {
        cwd: turn_context.cwd.clone(),
        approval_policy: turn_context.approval_policy,
        sandbox_policy: turn_context.sandbox_policy.clone(),
        model: turn_context.client.get_model(),
        effort: turn_context.client.get_reasoning_effort(),
        summary: turn_context.client.get_reasoning_summary(),
    });
    sess.persist_rollout_items(&[rollout_item]).await;
    let mut stream = turn_context.client.clone().stream(&prompt).await?;

    let tool_runtime = ToolCallRuntime::new(
        Arc::clone(&router),
        Arc::clone(&sess),
        Arc::clone(&turn_context),
        Arc::clone(&turn_diff_tracker),
        sub_id.to_string(),
    );
    let mut output: FuturesOrdered<BoxFuture<CodexResult<ProcessedResponseItem>>> =
        FuturesOrdered::new();

    loop {
        // Poll the next item from the model stream. We must inspect *both* Ok and Err
        // cases so that transient stream failures (e.g., dropped SSE connection before
        // `response.completed`) bubble up and trigger the caller's retry logic.
        let event = stream.next().await;
        let event = match event {
            Some(res) => res?,
            None => {
                return Err(CodexErr::Stream(
                    "stream closed before response.completed".into(),
                    None,
                ));
            }
        };

        let add_completed = &mut |response_item: ProcessedResponseItem| {
            output.push_back(future::ready(Ok(response_item)).boxed());
        };

        match event {
            ResponseEvent::Created => {}
            ResponseEvent::OutputItemDone(item) => {
                match ToolRouter::build_tool_call(sess.as_ref(), item.clone()) {
                    Ok(Some(call)) => {
                        let payload_preview = call.payload.log_payload().into_owned();
                        tracing::info!("ToolCall: {} {}", call.tool_name, payload_preview);

                        let response = tool_runtime.handle_tool_call(call);

                        output.push_back(
                            async move {
                                Ok(ProcessedResponseItem {
                                    item,
                                    response: Some(response.await?),
                                })
                            }
                            .boxed(),
                        );
                    }
                    Ok(None) => {
                        let response = handle_non_tool_response_item(
                            Arc::clone(&sess),
                            Arc::clone(&turn_context),
                            sub_id,
                            item.clone(),
                        )
                        .await?;
                        add_completed(ProcessedResponseItem { item, response });
                    }
                    Err(FunctionCallError::MissingLocalShellCallId) => {
                        let msg = "LocalShellCall without call_id or id";
                        turn_context
                            .client
                            .get_otel_event_manager()
                            .log_tool_failed("local_shell", msg);
                        error!(msg);

                        let response = ResponseInputItem::FunctionCallOutput {
                            call_id: String::new(),
                            output: FunctionCallOutputPayload {
                                content: msg.to_string(),
                                success: None,
                            },
                        };
                        add_completed(ProcessedResponseItem {
                            item,
                            response: Some(response),
                        });
                    }
                    Err(FunctionCallError::RespondToModel(message)) => {
                        let response = ResponseInputItem::FunctionCallOutput {
                            call_id: String::new(),
                            output: FunctionCallOutputPayload {
                                content: message,
                                success: None,
                            },
                        };
                        add_completed(ProcessedResponseItem {
                            item,
                            response: Some(response),
                        });
                    }
                    Err(FunctionCallError::Fatal(message)) => {
                        return Err(CodexErr::Fatal(message));
                    }
                }
            }
            ResponseEvent::WebSearchCallBegin { call_id } => {
                let _ = sess
                    .tx_event
                    .send(Event {
                        id: sub_id.to_string(),
                        msg: EventMsg::WebSearchBegin(WebSearchBeginEvent { call_id }),
                    })
                    .await;
            }
            ResponseEvent::RateLimits(snapshot) => {
                // Update internal state with latest rate limits, but defer sending until
                // token usage is available to avoid duplicate TokenCount events.
                sess.update_rate_limits(sub_id, snapshot).await;
            }
            ResponseEvent::Completed {
                response_id: _,
                token_usage,
            } => {
                sess.update_token_usage_info(sub_id, turn_context.as_ref(), token_usage.as_ref())
                    .await;

                let processed_items: Vec<ProcessedResponseItem> = output.try_collect().await?;

                let unified_diff = {
                    let mut tracker = turn_diff_tracker.lock().await;
                    tracker.get_unified_diff()
                };
                if let Ok(Some(unified_diff)) = unified_diff {
                    let msg = EventMsg::TurnDiff(TurnDiffEvent { unified_diff });
                    let event = Event {
                        id: sub_id.to_string(),
                        msg,
                    };
                    sess.send_event(event).await;
                }

                let result = TurnRunResult {
                    processed_items,
                    total_token_usage: token_usage.clone(),
                };

                return Ok(result);
            }
            ResponseEvent::OutputTextDelta(delta) => {
                // In review child threads, suppress assistant text deltas; the
                // UI will show a selection popup from the final ReviewOutput.
                if !turn_context.is_review_mode {
                    let event = Event {
                        id: sub_id.to_string(),
                        msg: EventMsg::AgentMessageDelta(AgentMessageDeltaEvent { delta }),
                    };
                    sess.send_event(event).await;
                } else {
                    trace!("suppressing OutputTextDelta in review mode");
                }
            }
            ResponseEvent::ReasoningSummaryDelta(delta) => {
                let event = Event {
                    id: sub_id.to_string(),
                    msg: EventMsg::AgentReasoningDelta(AgentReasoningDeltaEvent { delta }),
                };
                sess.send_event(event).await;
            }
            ResponseEvent::ReasoningSummaryPartAdded => {
                let event = Event {
                    id: sub_id.to_string(),
                    msg: EventMsg::AgentReasoningSectionBreak(AgentReasoningSectionBreakEvent {}),
                };
                sess.send_event(event).await;
            }
            ResponseEvent::Heartbeat(liveness) => {
                if let Some(snapshot) = sess.record_mailbox_liveness(liveness.transport_lag).await {
                    let transport_lag_ms =
                        snapshot.transport_lag.as_millis().min(u128::from(u64::MAX)) as u64;
                    let queue_depth = snapshot.queue_depth.min(u32::MAX as usize) as u32;

                    let event = Event {
                        id: sub_id.to_string(),
                        msg: EventMsg::Heartbeat(HeartbeatEvent {
                            observed_at: snapshot.observed_at,
                            transport_lag_ms: Some(transport_lag_ms),
                            queue_depth: Some(queue_depth),
                            liveness: Some(snapshot.state),
                        }),
                    };
                    sess.send_event(event).await;
                }
            }
            ResponseEvent::ReasoningContentDelta(delta) => {
                if sess.show_raw_agent_reasoning() {
                    let event = Event {
                        id: sub_id.to_string(),
                        msg: EventMsg::AgentReasoningRawContentDelta(
                            AgentReasoningRawContentDeltaEvent { delta },
                        ),
                    };
                    sess.send_event(event).await;
                }
            }
        }
    }
}

async fn handle_non_tool_response_item(
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
    sub_id: &str,
    item: ResponseItem,
) -> CodexResult<Option<ResponseInputItem>> {
    debug!(?item, "Output item");

    match &item {
        ResponseItem::Message { .. }
        | ResponseItem::Reasoning { .. }
        | ResponseItem::WebSearchCall { .. } => {
            let msgs = match &item {
                ResponseItem::Message { .. } if turn_context.is_review_mode => {
                    trace!("suppressing assistant Message in review mode");
                    Vec::new()
                }
                _ => map_response_item_to_event_messages(&item, sess.show_raw_agent_reasoning()),
            };
            for msg in msgs {
                let event = Event {
                    id: sub_id.to_string(),
                    msg,
                };
                sess.send_event(event).await;
            }
        }
        ResponseItem::FunctionCallOutput { .. } | ResponseItem::CustomToolCallOutput { .. } => {
            debug!("unexpected tool output from stream");
        }
        _ => {}
    }

    Ok(None)
}

pub(super) fn get_last_assistant_message_from_turn(responses: &[ResponseItem]) -> Option<String> {
    responses.iter().rev().find_map(|item| {
        if let ResponseItem::Message { role, content, .. } = item {
            if role == "assistant" {
                content.iter().rev().find_map(|ci| {
                    if let ContentItem::OutputText { text } = ci {
                        Some(text.clone())
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        } else {
            None
        }
    })
}
fn convert_call_tool_result_to_function_call_output_payload(
    call_tool_result: &CallToolResult,
) -> FunctionCallOutputPayload {
    let CallToolResult {
        content,
        is_error,
        structured_content,
    } = call_tool_result;

    // In terms of what to send back to the model, we prefer structured_content,
    // if available, and fallback to content, otherwise.
    let mut is_success = is_error != &Some(true);
    let content = if let Some(structured_content) = structured_content
        && structured_content != &serde_json::Value::Null
        && let Ok(serialized_structured_content) = serde_json::to_string(&structured_content)
    {
        serialized_structured_content
    } else {
        match serde_json::to_string(&content) {
            Ok(serialized_content) => serialized_content,
            Err(err) => {
                // If we could not serialize either content or structured_content to
                // JSON, flag this as an error.
                is_success = false;
                err.to_string()
            }
        }
    };

    FunctionCallOutputPayload {
        content,
        success: Some(is_success),
    }
}

/// Emits an ExitedReviewMode Event with optional ReviewOutput,
/// and records a developer message with the review output.
pub(crate) async fn exit_review_mode(
    session: Arc<Session>,
    task_sub_id: String,
    review_output: Option<ReviewOutputEvent>,
) {
    let event = Event {
        id: task_sub_id,
        msg: EventMsg::ExitedReviewMode(ExitedReviewModeEvent {
            review_output: review_output.clone(),
        }),
    };
    session.send_event(event).await;

    let mut user_message = String::new();
    if let Some(out) = review_output {
        let mut findings_str = String::new();
        let text = out.overall_explanation.trim();
        if !text.is_empty() {
            findings_str.push_str(text);
        }
        if !out.findings.is_empty() {
            let block = format_review_findings_block(&out.findings, None);
            findings_str.push_str(&format!("\n{block}"));
        }
        user_message.push_str(&format!(
            r#"<user_action>
  <context>User initiated a review task. Here's the full review output from reviewer model. User may select one or more comments to resolve.</context>
  <action>review</action>
  <results>
  {findings_str}
  </results>
</user_action>
"#));
    } else {
        user_message.push_str(r#"<user_action>
  <context>User initiated a review task, but was interrupted. If user asks about this, tell them to re-initiate a review with `/review` and wait for it to complete.</context>
  <action>review</action>
  <results>
  None.
  </results>
</user_action>
"#);
    }

    session
        .record_conversation_items(&[ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText { text: user_message }],
        }])
        .await;
}

use crate::executor::errors::ExecError;
use crate::executor::linkers::PreparedExec;
use crate::tools::context::ApplyPatchCommandContext;
use crate::tools::context::ExecCommandContext;
#[cfg(test)]
pub(crate) use tests::make_session_and_context;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::ConfigOverrides;
    use crate::config::ConfigToml;

    use crate::protocol::CompactedItem;
    use crate::protocol::InitialHistory;
    use crate::protocol::ResumedHistory;
    use crate::state::TaskKind;
    use crate::tasks::SessionTask;
    use crate::tasks::SessionTaskContext;
    use crate::tools::MODEL_FORMAT_HEAD_LINES;
    use crate::tools::MODEL_FORMAT_MAX_BYTES;
    use crate::tools::MODEL_FORMAT_MAX_LINES;
    use crate::tools::MODEL_FORMAT_TAIL_LINES;
    use crate::tools::ToolRouter;
    use crate::tools::handle_container_exec_with_params;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use codex_app_server_protocol::AuthMode;
    use codex_protocol::models::ContentItem;
    use codex_protocol::models::ResponseItem;

    use anyhow::Context as _;
    use mcp_types::ContentBlock;
    use mcp_types::TextContent;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use serde_json::json;
    use tokio::task::JoinHandle;
    use tokio::task::yield_now;

    pub(crate) struct MailboxTestGuard {
        _env_guard: MailboxEnvGuard,
        _home: tempfile::TempDir,
        _cwd: tempfile::TempDir,
    }

    struct MailboxEnvGuard {
        keys: [&'static str; 2],
    }

    impl MailboxEnvGuard {
        fn new() -> Self {
            // SAFETY: test harness serializes access to this env var.
            unsafe {
                std::env::set_var("CODEX_MAILBOX_OOB_FORCE", "1");
                std::env::set_var("CODEX_MAILBOX_OOB", "1");
            }
            Self {
                keys: ["CODEX_MAILBOX_OOB_FORCE", "CODEX_MAILBOX_OOB"],
            }
        }
    }

    impl Drop for MailboxEnvGuard {
        fn drop(&mut self) {
            // SAFETY: test harness serializes access to this env var.
            unsafe {
                for key in self.keys {
                    std::env::remove_var(key);
                }
            }
        }
    }

    struct EnvVarOverride {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarOverride {
        fn set(key: &'static str, value: impl AsRef<str>) -> Self {
            let previous = std::env::var(key).ok();
            // SAFETY: tests serialize access to these vars.
            unsafe {
                std::env::set_var(key, value.as_ref());
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarOverride {
        fn drop(&mut self) {
            // SAFETY: tests serialize access to these vars.
            unsafe {
                if let Some(prev) = &self.previous {
                    std::env::set_var(self.key, prev);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    pub(crate) async fn spawn_test_mailbox_session() -> anyhow::Result<(
        MailboxTestGuard,
        Arc<Session>,
        Arc<TurnContext>,
        JoinHandle<()>,
    )> {
        let env_guard = MailboxEnvGuard::new();
        let home = tempfile::tempdir().context("create codex home")?;
        let cwd = tempfile::tempdir().context("create cwd")?;

        let mut config = Config::load_from_base_config_with_overrides(
            ConfigToml::default(),
            ConfigOverrides::default(),
            home.path().to_path_buf(),
        )
        .context("load default config")?;
        config.cwd = cwd.path().to_path_buf();

        let config_arc = Arc::new(config.clone());
        let auth_manager = AuthManager::shared(config.codex_home.clone(), true);
        let (tx_event, _rx_event) = async_channel::unbounded();
        let (mailbox_tx, mailbox_rx) = mailbox_channel(MAILBOX_QUEUE_CAPACITY);

        let configure_session = ConfigureSession {
            provider: config.model_provider.clone(),
            model: config.model.clone(),
            model_reasoning_effort: config.model_reasoning_effort,
            model_reasoning_summary: config.model_reasoning_summary,
            user_instructions: config.user_instructions.clone(),
            base_instructions: config.base_instructions.clone(),
            approval_policy: config.approval_policy,
            sandbox_policy: config.sandbox_policy.clone(),
            notify: UserNotifier::default(),
            cwd: config.cwd.clone(),
        };

        let (session, turn_context) = Session::new(
            configure_session,
            Arc::clone(&config_arc),
            auth_manager,
            tx_event,
            InitialHistory::New,
            SessionSource::Exec,
            mailbox_tx.clone(),
        )
        .await
        .context("failed to initialize session")?;

        let include_vm_pty_tool =
            config_arc.include_vm_pty_tool && session.services.vm_pty_client.is_some();
        let include_vm_pty_open_tool =
            config_arc.include_vm_pty_open_tool && include_vm_pty_tool;
        let handler_turn_context = Arc::new(TurnContext {
            client: turn_context.client.clone(),
            cwd: turn_context.cwd.clone(),
            base_instructions: turn_context.base_instructions.clone(),
            user_instructions: turn_context.user_instructions.clone(),
            approval_policy: turn_context.approval_policy,
            sandbox_policy: turn_context.sandbox_policy.clone(),
            shell_environment_policy: turn_context.shell_environment_policy.clone(),
            tools_config: ToolsConfig::new(&ToolsConfigParams {
                model_family: &config_arc.model_family,
                include_plan_tool: config_arc.include_plan_tool,
                include_apply_patch_tool: config_arc.include_apply_patch_tool,
                include_web_search_request: config_arc.tools_web_search_request,
                use_streamable_shell_tool: config_arc.use_experimental_streamable_shell_tool,
                include_shell_tool: config_arc.include_shell_tool,
                include_view_image_tool: config_arc.include_view_image_tool,
                include_vm_pty_tool,
                include_vm_pty_open_tool,
                route_shell_via_pty: false,
                experimental_unified_exec_tool: config_arc.use_experimental_unified_exec_tool,
                debug_tools: config_arc.debug_tools,
            }),
            vm_pty_default_vm_id: config_arc.vm_pty_default_vm_id.clone(),
            vm_pty_open_timeout: config_arc.vm_pty_open_timeout,
            vm_pty_open_blocking: config_arc.vm_pty_open_blocking,
            vm_pty_max_concurrent_per_worker: config_arc.vm_pty_max_concurrent_per_worker,
            is_review_mode: false,
            final_output_json_schema: None,
        });

        let (tx_sub, rx_sub) = async_channel::bounded(SUBMISSION_CHANNEL_CAPACITY);
        drop(tx_sub);

        let submission_task = tokio::spawn(submission_loop(
            Arc::clone(&session),
            turn_context,
            Arc::clone(&config_arc),
            rx_sub,
            mailbox_tx,
            mailbox_rx,
            None,
        ));

        let guard = MailboxTestGuard {
            _env_guard: env_guard,
            _home: home,
            _cwd: cwd,
        };

        Ok((guard, session, handler_turn_context, submission_task))
    }
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
    use tokio::time::Duration;
    use tokio::time::sleep;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatcher_falls_back_to_local_delivery_when_unavailable() -> anyhow::Result<()> {
        let (_guard, session, _turn_context, submission_task) =
            spawn_test_mailbox_session().await.expect("spawn session");

        let temp_dir = tempfile::TempDir::new().context("temp dir")?;
        let endpoint = temp_dir.path().join("dispatcher.sock");

        let _force = EnvVarOverride::set("CODEX_MAIL_SERVER_FORCE", "1");
        let _endpoint = EnvVarOverride::set(
            "CODEX_MAIL_SERVER_ENDPOINT",
            endpoint.to_string_lossy().to_string(),
        );
        let _disable = EnvVarOverride::set("CODEX_MAIL_SERVER_DISABLE", "0");

        let client =
            MailboxDispatcherClient::from_env().expect("dispatcher client should be constructed");
        let client = Arc::new(client);

        let target_id = Uuid::now_v7();
        let message_id = Uuid::now_v7();
        let message = serde_json::from_value::<MailboxMessage>(json!({
            "message_id": message_id,
            "priority": "normal",
            "sender": { "id": "system.test", "role": "system" },
            "body": { "subject": "Fallback check", "content": "hello", "content_type": "text/plain" },
            "audit": { "request_id": "REQ-fallback" },
            "audience": { "conversation_id": target_id }
        }))
        .expect("valid mailbox message");

        let envelope = MailboxEnvelope::new("sub-fallback".to_string(), message.clone());

        let receiver = session
            .register_mailbox_delivery_listener(message.message_id)
            .await;

        let (mailbox_tx, _mailbox_rx) = mailbox_channel(MAILBOX_QUEUE_CAPACITY);

        dispatch_mailbox_envelope(client, Arc::clone(&session), mailbox_tx, envelope).await;

        let delivery = receiver.await.expect("delivery event");
        assert_eq!(delivery.state, MailboxDeliveryState::Delivered);

        submission_task.abort();
        Ok(())
    }

    #[test]
    fn reconstruct_history_matches_live_compactions() {
        let (session, turn_context) = make_session_and_context();
        let (rollout_items, expected) = sample_rollout(&session, &turn_context);

        let reconstructed = session.reconstruct_history_from_rollout(&turn_context, &rollout_items);

        assert_eq!(expected, reconstructed);
    }

    #[test]
    fn record_initial_history_reconstructs_resumed_transcript() {
        let (session, turn_context) = make_session_and_context();
        let (rollout_items, expected) = sample_rollout(&session, &turn_context);

        tokio_test::block_on(session.record_initial_history(
            &turn_context,
            InitialHistory::Resumed(ResumedHistory {
                conversation_id: ConversationId::default(),
                history: rollout_items,
                rollout_path: PathBuf::from("/tmp/resume.jsonl"),
            }),
        ));

        let actual = tokio_test::block_on(async { session.state.lock().await.history_snapshot() });
        assert_eq!(expected, actual);
    }

    #[test]
    fn record_initial_history_reconstructs_forked_transcript() {
        let (session, turn_context) = make_session_and_context();
        let (rollout_items, expected) = sample_rollout(&session, &turn_context);

        tokio_test::block_on(
            session.record_initial_history(&turn_context, InitialHistory::Forked(rollout_items)),
        );

        let actual = tokio_test::block_on(async { session.state.lock().await.history_snapshot() });
        assert_eq!(expected, actual);
    }

    #[test]
    fn prefers_structured_content_when_present() {
        let ctr = CallToolResult {
            // Content present but should be ignored because structured_content is set.
            content: vec![text_block("ignored")],
            is_error: None,
            structured_content: Some(json!({
                "ok": true,
                "value": 42
            })),
        };

        let got = convert_call_tool_result_to_function_call_output_payload(&ctr);
        let expected = FunctionCallOutputPayload {
            content: serde_json::to_string(&json!({
                "ok": true,
                "value": 42
            }))
            .unwrap(),
            success: Some(true),
        };

        assert_eq!(expected, got);
    }

    #[test]
    fn model_truncation_head_tail_by_lines() {
        // Build 400 short lines so line-count limit, not byte budget, triggers truncation
        let lines: Vec<String> = (1..=400).map(|i| format!("line{i}")).collect();
        let full = lines.join("\n");

        let exec = ExecToolCallOutput {
            exit_code: 0,
            stdout: StreamOutput::new(String::new()),
            stderr: StreamOutput::new(String::new()),
            aggregated_output: StreamOutput::new(full),
            duration: StdDuration::from_secs(1),
            timed_out: false,
        };

        let out = format_exec_output_str(&exec);

        // Strip truncation header if present for subsequent assertions
        let body = out
            .strip_prefix("Total output lines: ")
            .and_then(|rest| rest.split_once("\n\n").map(|x| x.1))
            .unwrap_or(out.as_str());

        // Expect elision marker with correct counts
        let omitted = 400 - MODEL_FORMAT_MAX_LINES; // 144
        let marker = format!("\n[... omitted {omitted} of 400 lines ...]\n\n");
        assert!(out.contains(&marker), "missing marker: {out}");

        // Validate head and tail
        let parts: Vec<&str> = body.split(&marker).collect();
        assert_eq!(parts.len(), 2, "expected one marker split");
        let head = parts[0];
        let tail = parts[1];

        let expected_head: String = (1..=MODEL_FORMAT_HEAD_LINES)
            .map(|i| format!("line{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(head.starts_with(&expected_head), "head mismatch");

        let expected_tail: String = ((400 - MODEL_FORMAT_TAIL_LINES + 1)..=400)
            .map(|i| format!("line{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(tail.ends_with(&expected_tail), "tail mismatch");
    }

    #[test]
    fn model_truncation_respects_byte_budget() {
        // Construct a large output (about 100kB) so byte budget dominates
        let big_line = "x".repeat(100);
        let full = std::iter::repeat_n(big_line, 1000)
            .collect::<Vec<_>>()
            .join("\n");

        let exec = ExecToolCallOutput {
            exit_code: 0,
            stdout: StreamOutput::new(String::new()),
            stderr: StreamOutput::new(String::new()),
            aggregated_output: StreamOutput::new(full.clone()),
            duration: StdDuration::from_secs(1),
            timed_out: false,
        };

        let out = format_exec_output_str(&exec);
        // Keep strict budget on the truncated body (excluding header)
        let body = out
            .strip_prefix("Total output lines: ")
            .and_then(|rest| rest.split_once("\n\n").map(|x| x.1))
            .unwrap_or(out.as_str());
        assert!(body.len() <= MODEL_FORMAT_MAX_BYTES, "exceeds byte budget");
        assert!(out.contains("omitted"), "should contain elision marker");

        // Ensure head and tail are drawn from the original
        assert!(full.starts_with(body.chars().take(8).collect::<String>().as_str()));
        assert!(
            full.ends_with(
                body.chars()
                    .rev()
                    .take(8)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect::<String>()
                    .as_str()
            )
        );
    }

    #[test]
    fn includes_timed_out_message() {
        let exec = ExecToolCallOutput {
            exit_code: 0,
            stdout: StreamOutput::new(String::new()),
            stderr: StreamOutput::new(String::new()),
            aggregated_output: StreamOutput::new("Command output".to_string()),
            duration: StdDuration::from_secs(1),
            timed_out: true,
        };

        let out = format_exec_output_str(&exec);

        assert_eq!(
            out,
            "command timed out after 1000 milliseconds\nCommand output"
        );
    }

    #[test]
    fn falls_back_to_content_when_structured_is_null() {
        let ctr = CallToolResult {
            content: vec![text_block("hello"), text_block("world")],
            is_error: None,
            structured_content: Some(serde_json::Value::Null),
        };

        let got = convert_call_tool_result_to_function_call_output_payload(&ctr);
        let expected = FunctionCallOutputPayload {
            content: serde_json::to_string(&vec![text_block("hello"), text_block("world")])
                .unwrap(),
            success: Some(true),
        };

        assert_eq!(expected, got);
    }

    #[test]
    fn success_flag_reflects_is_error_true() {
        let ctr = CallToolResult {
            content: vec![text_block("unused")],
            is_error: Some(true),
            structured_content: Some(json!({ "message": "bad" })),
        };

        let got = convert_call_tool_result_to_function_call_output_payload(&ctr);
        let expected = FunctionCallOutputPayload {
            content: serde_json::to_string(&json!({ "message": "bad" })).unwrap(),
            success: Some(false),
        };

        assert_eq!(expected, got);
    }

    #[test]
    fn success_flag_true_with_no_error_and_content_used() {
        let ctr = CallToolResult {
            content: vec![text_block("alpha")],
            is_error: Some(false),
            structured_content: None,
        };

        let got = convert_call_tool_result_to_function_call_output_payload(&ctr);
        let expected = FunctionCallOutputPayload {
            content: serde_json::to_string(&vec![text_block("alpha")]).unwrap(),
            success: Some(true),
        };

        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn wait_trigger_completion_injects_and_emits_mailbox() {
        let _env_guard = MailboxEnvGuard::new();
        assert!(crate::mailbox::mailbox_feature_enabled());
        let (session, _tc, _rx) = make_session_and_context_with_rx();
        {
            let mut active = session.active_turn.lock().await;
            *active = Some(ActiveTurn::default());
        }

        let handle = session
            .schedule_wait_trigger("sub-mail", "call-mail", WaitTriggerSpec::new("timer"))
            .await
            .expect("schedule wait trigger");

        let completion = WaitTriggerCompletion::default().with_inputs(vec![
            ResponseInputItem::FunctionCallOutput {
                call_id: "call-mail".to_string(),
                output: FunctionCallOutputPayload {
                    content: "completed".to_string(),
                    success: Some(true),
                },
            },
        ]);

        handle.complete(completion).await.expect("complete");

        yield_now().await;
        sleep(StdDuration::from_millis(20)).await;

        #[cfg(test)]
        {
            let events = session.wait_trigger_mailbox_events.lock().await;
            let failures = session.wait_trigger_mailbox_failures.lock().await;
            assert!(events.len() + failures.len() <= 1);
            if let Some(event) = events.first() {
                assert_eq!(event.message.sender.role, MailboxSenderRole::Automation);
            }
            if let Some(err) = failures.first() {
                assert!(matches!(
                    err,
                    MailboxEnqueueError::Closed
                        | MailboxEnqueueError::Disabled
                        | MailboxEnqueueError::Full { .. }
                ));
            }
        }

        let pending = session.get_pending_input().await;
        assert_eq!(pending.len(), 1);
        match &pending[0] {
            ResponseInputItem::FunctionCallOutput { call_id, output } => {
                assert_eq!(call_id, "call-mail");
                assert_eq!(output.content, "completed");
            }
            other => panic!("unexpected pending input: {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_trigger_cancellation_clears_state() {
        let (session, _tc, _rx) = make_session_and_context_with_rx();
        {
            let mut active = session.active_turn.lock().await;
            *active = Some(ActiveTurn::default());
        }

        let handle = session
            .schedule_wait_trigger("sub-cancel", "call-cancel", WaitTriggerSpec::new("fs"))
            .await
            .expect("schedule wait trigger");

        handle.cancel().await.expect("cancel wait trigger");
        yield_now().await;

        {
            let guard = session.wait_triggers.lock().await;
            assert!(guard.is_empty());
        }

        let active = session.active_turn.lock().await;
        if let Some(at) = active.as_ref() {
            let ts = at.turn_state.lock().await;
            assert_eq!(ts.trigger_count(), 0);
        }
    }

    #[tokio::test]
    async fn wait_trigger_quota_is_enforced() {
        let (session, _tc, _rx) = make_session_and_context_with_rx();
        {
            let mut active = session.active_turn.lock().await;
            *active = Some(ActiveTurn::default());
        }

        let mut handles = Vec::new();
        for idx in 0..MAX_WAIT_TRIGGERS_PER_TURN {
            let spec = WaitTriggerSpec::new(format!("timer-{idx}"));
            let handle = session
                .schedule_wait_trigger("sub-quota", &format!("call-{idx}"), spec)
                .await
                .expect("within quota");
            handles.push(handle);
        }

        let result = session
            .schedule_wait_trigger("sub-quota", "call-over", WaitTriggerSpec::new("overflow"))
            .await;
        let err = match result {
            Err(err) => err,
            Ok(handle) => {
                let _ = handle.cancel().await;
                panic!("quota should be exceeded");
            }
        };
        match err {
            WaitTriggerError::QuotaExceeded { limit } => {
                assert_eq!(limit, MAX_WAIT_TRIGGERS_PER_TURN);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        for handle in handles {
            let _ = handle.cancel().await;
        }
    }

    #[tokio::test]
    async fn wait_trigger_persists_until_completion() {
        let (session, _tc, _rx) = make_session_and_context_with_rx();
        {
            let mut active = session.active_turn.lock().await;
            *active = Some(ActiveTurn::default());
        }

        let handle = session
            .schedule_wait_trigger("sub-persist", "call-persist", WaitTriggerSpec::new("shell"))
            .await
            .expect("schedule wait trigger");

        let pending = session.get_pending_input().await;
        assert!(pending.is_empty());

        {
            let active = session.active_turn.lock().await;
            let at = active.as_ref().expect("active turn");
            let ts = at.turn_state.lock().await;
            assert_eq!(ts.trigger_count(), 1);
        }

        let completion = WaitTriggerCompletion::default().with_inputs(vec![
            ResponseInputItem::FunctionCallOutput {
                call_id: "call-persist".to_string(),
                output: FunctionCallOutputPayload {
                    content: "persist".to_string(),
                    success: Some(true),
                },
            },
        ]);

        handle.complete(completion).await.expect("complete");
        yield_now().await;

        {
            let active = session.active_turn.lock().await;
            let at = active.as_ref().expect("active turn");
            let ts = at.turn_state.lock().await;
            assert_eq!(ts.trigger_count(), 0);
        }
    }

    fn text_block(s: &str) -> ContentBlock {
        ContentBlock::TextContent(TextContent {
            annotations: None,
            text: s.to_string(),
            r#type: "text".to_string(),
        })
    }

    fn otel_event_manager(conversation_id: ConversationId, config: &Config) -> OtelEventManager {
        OtelEventManager::new(
            conversation_id,
            config.model.as_str(),
            config.model_family.slug.as_str(),
            None,
            Some(AuthMode::ChatGPT),
            false,
            "test".to_string(),
        )
    }

    pub(crate) fn make_session_and_context() -> (Session, TurnContext) {
        let (tx_event, _rx_event) = async_channel::unbounded();
        let codex_home = tempfile::tempdir().expect("create temp dir");
        let config = Config::load_from_base_config_with_overrides(
            ConfigToml::default(),
            ConfigOverrides::default(),
            codex_home.path().to_path_buf(),
        )
        .expect("load default test config");
        let config = Arc::new(config);
        let conversation_id = ConversationId::default();
        let otel_event_manager = otel_event_manager(conversation_id, config.as_ref());
        let client = ModelClient::new(
            config.clone(),
            None,
            otel_event_manager,
            config.model_provider.clone(),
            config.model_reasoning_effort,
            config.model_reasoning_summary,
            conversation_id,
        );
        let tools_config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &config.model_family,
            include_plan_tool: config.include_plan_tool,
            include_apply_patch_tool: config.include_apply_patch_tool,
            include_web_search_request: config.tools_web_search_request,
            use_streamable_shell_tool: config.use_experimental_streamable_shell_tool,
            include_shell_tool: config.include_shell_tool,
            include_view_image_tool: config.include_view_image_tool,
            experimental_unified_exec_tool: config.use_experimental_unified_exec_tool,
            include_vm_pty_tool: config.include_vm_pty_tool,
            include_vm_pty_open_tool: config.include_vm_pty_open_tool,
            route_shell_via_pty: false,
            debug_tools: config.debug_tools,
        });
        let turn_context = TurnContext {
            client,
            cwd: config.cwd.clone(),
            base_instructions: config.base_instructions.clone(),
            user_instructions: config.user_instructions.clone(),
            approval_policy: config.approval_policy,
            sandbox_policy: config.sandbox_policy.clone(),
            shell_environment_policy: config.shell_environment_policy.clone(),
            tools_config,
            vm_pty_default_vm_id: config.vm_pty_default_vm_id.clone(),
            vm_pty_open_timeout: config.vm_pty_open_timeout,
            vm_pty_open_blocking: config.vm_pty_open_blocking,
            vm_pty_max_concurrent_per_worker: config.vm_pty_max_concurrent_per_worker,
            is_review_mode: false,
            final_output_json_schema: None,
        };
        let services = SessionServices {
            mcp_connection_manager: McpConnectionManager::default(),
            session_manager: ExecSessionManager::default(),
            unified_exec_manager: UnifiedExecSessionManager::default(),
            notifier: UserNotifier::default(),
            rollout: Mutex::new(None),
            user_shell: shell::Shell::Unknown,
            show_raw_agent_reasoning: config.show_raw_agent_reasoning,
            executor: Executor::new(ExecutorConfig::new(
                turn_context.sandbox_policy.clone(),
                turn_context.cwd.clone(),
                None,
                false,
            )),
            summaries: Mutex::new(None),
            vm_pty_client: None,
            pty_sessions: Mutex::new(Default::default()),
        };
        let (mailbox_tx, _) = mailbox_channel(1);
        let session = Session {
            conversation_id,
            tx_event,
            state: Mutex::new(SessionState::new()),
            active_turn: Mutex::new(None),
            services,
            next_internal_sub_id: AtomicU64::new(0),
            mailbox_tx,
            #[cfg(test)]
            wait_trigger_mailbox_events: Mutex::new(Vec::new()),
            #[cfg(test)]
            wait_trigger_mailbox_failures: Mutex::new(Vec::new()),
            wait_triggers: Mutex::new(HashMap::new()),
            mailbox_waiters: Mutex::new(HashMap::new()),
            mailbox_predicate_waiters: Mutex::new(Vec::new()),
            session_source: SessionSource::Cli,
            mailbox_metrics: Mutex::new(MailboxDeliveryTelemetry::new()),
            mailbox_liveness: None,
        };
        (session, turn_context)
    }

    // Like make_session_and_context, but returns Arc<Session> and the event receiver
    // so tests can assert on emitted events.
    fn make_session_and_context_with_rx() -> (
        Arc<Session>,
        Arc<TurnContext>,
        async_channel::Receiver<Event>,
    ) {
        let (tx_event, rx_event) = async_channel::unbounded();
        let codex_home = tempfile::tempdir().expect("create temp dir");
        let config = Config::load_from_base_config_with_overrides(
            ConfigToml::default(),
            ConfigOverrides::default(),
            codex_home.path().to_path_buf(),
        )
        .expect("load default test config");
        let config = Arc::new(config);
        let conversation_id = ConversationId::default();
        let otel_event_manager = otel_event_manager(conversation_id, config.as_ref());
        let client = ModelClient::new(
            config.clone(),
            None,
            otel_event_manager,
            config.model_provider.clone(),
            config.model_reasoning_effort,
            config.model_reasoning_summary,
            conversation_id,
        );
        let tools_config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &config.model_family,
            include_plan_tool: config.include_plan_tool,
            include_apply_patch_tool: config.include_apply_patch_tool,
            include_web_search_request: config.tools_web_search_request,
            use_streamable_shell_tool: config.use_experimental_streamable_shell_tool,
            include_shell_tool: config.include_shell_tool,
            include_view_image_tool: config.include_view_image_tool,
            experimental_unified_exec_tool: config.use_experimental_unified_exec_tool,
            include_vm_pty_tool: config.include_vm_pty_tool,
            include_vm_pty_open_tool: config.include_vm_pty_open_tool,
            route_shell_via_pty: false,
            debug_tools: config.debug_tools,
        });
        let turn_context = Arc::new(TurnContext {
            client,
            cwd: config.cwd.clone(),
            base_instructions: config.base_instructions.clone(),
            user_instructions: config.user_instructions.clone(),
            approval_policy: config.approval_policy,
            sandbox_policy: config.sandbox_policy.clone(),
            shell_environment_policy: config.shell_environment_policy.clone(),
            tools_config,
            vm_pty_default_vm_id: config.vm_pty_default_vm_id.clone(),
            vm_pty_open_timeout: config.vm_pty_open_timeout,
            vm_pty_open_blocking: config.vm_pty_open_blocking,
            vm_pty_max_concurrent_per_worker: config.vm_pty_max_concurrent_per_worker,
            is_review_mode: false,
            final_output_json_schema: None,
        });
        let services = SessionServices {
            mcp_connection_manager: McpConnectionManager::default(),
            session_manager: ExecSessionManager::default(),
            unified_exec_manager: UnifiedExecSessionManager::default(),
            notifier: UserNotifier::default(),
            rollout: Mutex::new(None),
            user_shell: shell::Shell::Unknown,
            show_raw_agent_reasoning: config.show_raw_agent_reasoning,
            executor: Executor::new(ExecutorConfig::new(
                config.sandbox_policy.clone(),
                config.cwd.clone(),
                None,
                false,
            )),
            summaries: Mutex::new(None),
            vm_pty_client: None,
            pty_sessions: Mutex::new(Default::default()),
        };
        let (mailbox_tx, _) = mailbox_channel(1);
        let session = Arc::new(Session {
            conversation_id,
            tx_event,
            state: Mutex::new(SessionState::new()),
            active_turn: Mutex::new(None),
            services,
            next_internal_sub_id: AtomicU64::new(0),
            mailbox_tx,
            #[cfg(test)]
            wait_trigger_mailbox_events: Mutex::new(Vec::new()),
            #[cfg(test)]
            wait_trigger_mailbox_failures: Mutex::new(Vec::new()),
            wait_triggers: Mutex::new(HashMap::new()),
            mailbox_waiters: Mutex::new(HashMap::new()),
            mailbox_predicate_waiters: Mutex::new(Vec::new()),
            session_source: SessionSource::Cli,
            mailbox_metrics: Mutex::new(MailboxDeliveryTelemetry::new()),
            mailbox_liveness: None,
        });
        (session, turn_context, rx_event)
    }

    #[derive(Clone, Copy)]
    struct NeverEndingTask(TaskKind);

    #[async_trait::async_trait]
    impl SessionTask for NeverEndingTask {
        fn kind(&self) -> TaskKind {
            self.0
        }

        async fn run(
            self: Arc<Self>,
            _session: Arc<SessionTaskContext>,
            _ctx: Arc<TurnContext>,
            _sub_id: String,
            _input: Vec<InputItem>,
        ) -> Option<String> {
            loop {
                sleep(Duration::from_secs(60)).await;
            }
        }

        async fn abort(&self, session: Arc<SessionTaskContext>, sub_id: &str) {
            if let TaskKind::Review = self.0 {
                exit_review_mode(session.clone_session(), sub_id.to_string(), None).await;
            }
        }
    }

    #[tokio::test]
    async fn abort_regular_task_emits_turn_aborted_only() {
        let (sess, tc, rx) = make_session_and_context_with_rx();
        let sub_id = "sub-regular".to_string();
        let input = vec![InputItem::Text {
            text: "hello".to_string(),
        }];
        sess.spawn_task(
            Arc::clone(&tc),
            sub_id.clone(),
            input,
            NeverEndingTask(TaskKind::Regular),
        )
        .await;

        sess.abort_all_tasks(TurnAbortReason::Interrupted).await;

        let evt = rx.recv().await.expect("event");
        match evt.msg {
            EventMsg::TurnAborted(e) => assert_eq!(TurnAbortReason::Interrupted, e.reason),
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn abort_review_task_emits_exited_then_aborted_and_records_history() {
        let (sess, tc, rx) = make_session_and_context_with_rx();
        let sub_id = "sub-review".to_string();
        let input = vec![InputItem::Text {
            text: "start review".to_string(),
        }];
        sess.spawn_task(
            Arc::clone(&tc),
            sub_id.clone(),
            input,
            NeverEndingTask(TaskKind::Review),
        )
        .await;

        sess.abort_all_tasks(TurnAbortReason::Interrupted).await;

        let first = rx.recv().await.expect("first event");
        match first.msg {
            EventMsg::ExitedReviewMode(ev) => assert!(ev.review_output.is_none()),
            other => panic!("unexpected first event: {other:?}"),
        }
        let second = rx.recv().await.expect("second event");
        match second.msg {
            EventMsg::TurnAborted(e) => assert_eq!(TurnAbortReason::Interrupted, e.reason),
            other => panic!("unexpected second event: {other:?}"),
        }

        let history = sess.history_snapshot().await;
        let found = history.iter().any(|item| match item {
            ResponseItem::Message { role, content, .. } if role == "user" => {
                content.iter().any(|ci| match ci {
                    ContentItem::InputText { text } => {
                        text.contains("<user_action>")
                            && text.contains("review")
                            && text.contains("interrupted")
                    }
                    _ => false,
                })
            }
            _ => false,
        });
        assert!(
            found,
            "synthetic review interruption not recorded in history"
        );
    }

    #[tokio::test]
    async fn fatal_tool_error_stops_turn_and_reports_error() {
        let (session, turn_context, _rx) = make_session_and_context_with_rx();
        let router = ToolRouter::from_config(
            &turn_context.tools_config,
            Some(session.services.mcp_connection_manager.list_all_tools()),
        );
        let item = ResponseItem::CustomToolCall {
            id: None,
            status: None,
            call_id: "call-1".to_string(),
            name: "shell".to_string(),
            input: "{}".to_string(),
        };

        let call = ToolRouter::build_tool_call(session.as_ref(), item.clone())
            .expect("build tool call")
            .expect("tool call present");
        let tracker = Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new()));
        let err = router
            .dispatch_tool_call(
                Arc::clone(&session),
                Arc::clone(&turn_context),
                tracker,
                "sub-id".to_string(),
                call,
            )
            .await
            .expect_err("expected fatal error");

        match err {
            FunctionCallError::Fatal(message) => {
                assert_eq!(message, "tool shell invoked with incompatible payload");
            }
            other => panic!("expected FunctionCallError::Fatal, got {other:?}"),
        }
    }

    fn sample_rollout(
        session: &Session,
        turn_context: &TurnContext,
    ) -> (Vec<RolloutItem>, Vec<ResponseItem>) {
        let mut rollout_items = Vec::new();
        let mut live_history = ConversationHistory::new();

        let initial_context = session.build_initial_context(turn_context);
        for item in &initial_context {
            rollout_items.push(RolloutItem::ResponseItem(item.clone()));
        }
        live_history.record_items(initial_context.iter());

        let user1 = ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "first user".to_string(),
            }],
        };
        live_history.record_items(std::iter::once(&user1));
        rollout_items.push(RolloutItem::ResponseItem(user1.clone()));

        let assistant1 = ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: "assistant reply one".to_string(),
            }],
        };
        live_history.record_items(std::iter::once(&assistant1));
        rollout_items.push(RolloutItem::ResponseItem(assistant1.clone()));

        let summary1 = "summary one";
        let snapshot1 = live_history.contents();
        let user_messages1 = collect_user_messages(&snapshot1);
        let rebuilt1 = build_compacted_history(
            session.build_initial_context(turn_context),
            &user_messages1,
            summary1,
        );
        live_history.replace(rebuilt1);
        rollout_items.push(RolloutItem::Compacted(CompactedItem {
            message: summary1.to_string(),
        }));

        let user2 = ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "second user".to_string(),
            }],
        };
        live_history.record_items(std::iter::once(&user2));
        rollout_items.push(RolloutItem::ResponseItem(user2.clone()));

        let assistant2 = ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: "assistant reply two".to_string(),
            }],
        };
        live_history.record_items(std::iter::once(&assistant2));
        rollout_items.push(RolloutItem::ResponseItem(assistant2.clone()));

        let summary2 = "summary two";
        let snapshot2 = live_history.contents();
        let user_messages2 = collect_user_messages(&snapshot2);
        let rebuilt2 = build_compacted_history(
            session.build_initial_context(turn_context),
            &user_messages2,
            summary2,
        );
        live_history.replace(rebuilt2);
        rollout_items.push(RolloutItem::Compacted(CompactedItem {
            message: summary2.to_string(),
        }));

        let user3 = ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "third user".to_string(),
            }],
        };
        live_history.record_items(std::iter::once(&user3));
        rollout_items.push(RolloutItem::ResponseItem(user3.clone()));

        let assistant3 = ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: "assistant reply three".to_string(),
            }],
        };
        live_history.record_items(std::iter::once(&assistant3));
        rollout_items.push(RolloutItem::ResponseItem(assistant3.clone()));

        (rollout_items, live_history.contents())
    }

    #[tokio::test]
    async fn rejects_escalated_permissions_when_policy_not_on_request() {
        use crate::exec::ExecParams;
        use crate::protocol::AskForApproval;
        use crate::protocol::SandboxPolicy;
        use crate::turn_diff_tracker::TurnDiffTracker;
        use std::collections::HashMap;

        let (session, mut turn_context_raw) = make_session_and_context();
        // Ensure policy is NOT OnRequest so the early rejection path triggers
        turn_context_raw.approval_policy = AskForApproval::OnFailure;
        let session = Arc::new(session);
        let mut turn_context = Arc::new(turn_context_raw);

        let params = ExecParams {
            command: if cfg!(windows) {
                vec![
                    "cmd.exe".to_string(),
                    "/C".to_string(),
                    "echo hi".to_string(),
                ]
            } else {
                vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "echo hi".to_string(),
                ]
            },
            cwd: turn_context.cwd.clone(),
            timeout_ms: Some(1000),
            env: HashMap::new(),
            with_escalated_permissions: Some(true),
            justification: Some("test".to_string()),
        };

        let params2 = ExecParams {
            with_escalated_permissions: Some(false),
            ..params.clone()
        };

        let turn_diff_tracker = Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new()));

        let tool_name = "shell";
        let sub_id = "test-sub".to_string();
        let call_id = "test-call".to_string();

        let resp = handle_container_exec_with_params(
            tool_name,
            params,
            Arc::clone(&session),
            Arc::clone(&turn_context),
            Arc::clone(&turn_diff_tracker),
            sub_id,
            call_id,
        )
        .await;

        let Err(FunctionCallError::RespondToModel(output)) = resp else {
            panic!("expected error result");
        };

        let expected = format!(
            "approval policy is {policy:?}; reject command — you should not ask for escalated permissions if the approval policy is {policy:?}",
            policy = turn_context.approval_policy
        );

        pretty_assertions::assert_eq!(output, expected);

        // Now retry the same command WITHOUT escalated permissions; should succeed.
        // Force DangerFullAccess to avoid platform sandbox dependencies in tests.
        Arc::get_mut(&mut turn_context)
            .expect("unique turn context Arc")
            .sandbox_policy = SandboxPolicy::DangerFullAccess;

        let resp2 = handle_container_exec_with_params(
            tool_name,
            params2,
            Arc::clone(&session),
            Arc::clone(&turn_context),
            Arc::clone(&turn_diff_tracker),
            "test-sub".to_string(),
            "test-call-2".to_string(),
        )
        .await;

        let output = resp2.expect("expected Ok result");

        #[derive(Deserialize, PartialEq, Eq, Debug)]
        struct ResponseExecMetadata {
            exit_code: i32,
        }

        #[derive(Deserialize)]
        struct ResponseExecOutput {
            output: String,
            metadata: ResponseExecMetadata,
        }

        let exec_output: ResponseExecOutput =
            serde_json::from_str(&output).expect("valid exec output json");

        pretty_assertions::assert_eq!(exec_output.metadata, ResponseExecMetadata { exit_code: 0 });
        assert!(exec_output.output.contains("hi"));
    }
}
