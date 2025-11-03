use crate::config_loader::LoadedConfigLayers;
pub use crate::config_loader::load_config_as_toml;
use crate::config_loader::load_config_layers_with_overrides;
use crate::config_loader::merge_toml_values;
use crate::config_profile::ConfigProfile;
use crate::config_types::DEFAULT_OTEL_ENVIRONMENT;
use crate::config_types::History;
use crate::config_types::McpServerConfig;
use crate::config_types::McpServerTransportConfig;
use crate::config_types::Notifications;
use crate::config_types::OtelConfig;
use crate::config_types::OtelConfigToml;
use crate::config_types::OtelExporterKind;
use crate::config_types::ReasoningSummaryFormat;
use crate::config_types::SandboxWorkspaceWrite;
use crate::config_types::ShellEnvironmentPolicy;
use crate::config_types::ShellEnvironmentPolicyToml;
use crate::config_types::Tui;
use crate::config_types::UriBasedFileOpener;
use crate::git_info::resolve_root_git_project_for_trust;
use crate::model_family::ModelFamily;
use crate::model_family::derive_default_model_family;
use crate::model_family::find_family_for_model;
use crate::model_provider_info::ModelProviderInfo;
use crate::model_provider_info::built_in_model_providers;
use crate::openai_model_info::get_model_info;
use crate::protocol::AskForApproval;
use crate::protocol::SandboxPolicy;
use crate::vm_pty;
use codex_app_server_protocol::Tools;
use codex_app_server_protocol::UserSavedConfig;
use codex_protocol::config_types::ReasoningEffort;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::SandboxMode;
use codex_protocol::config_types::Verbosity;
use codex_rmcp_client::OAuthCredentialsStoreMode;
use dirs::home_dir;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::Duration;
use tracing::warn;

use crate::config_edit::CONFIG_KEY_EFFORT;
use crate::config_edit::CONFIG_KEY_MODEL;
use crate::config_edit::persist_overrides_and_clear_if_none;
use tempfile::NamedTempFile;
use toml::Value as TomlValue;
use toml_edit::Array as TomlArray;
use toml_edit::DocumentMut;
use toml_edit::Item as TomlItem;
use toml_edit::Table as TomlTable;

#[cfg(target_os = "windows")]
pub const OPENAI_DEFAULT_MODEL: &str = "gpt-5";
#[cfg(not(target_os = "windows"))]
pub const OPENAI_DEFAULT_MODEL: &str = "gpt-5-codex";
const OPENAI_DEFAULT_REVIEW_MODEL: &str = "gpt-5-codex";
pub const GPT_5_CODEX_MEDIUM_MODEL: &str = "gpt-5-codex";

/// Maximum number of bytes of the documentation that will be embedded. Larger
/// files are *silently truncated* to this size so we do not take up too much of
/// the context window.
pub(crate) const PROJECT_DOC_MAX_BYTES: usize = 32 * 1024; // 32 KiB

const DEFAULT_MAILBOX_LIVENESS_IDLE_AFTER: Duration = Duration::from_secs(3);
const DEFAULT_MAILBOX_LIVENESS_STALLED_AFTER: Duration = Duration::from_secs(12);
const DEFAULT_MAILBOX_LIVENESS_EMIT_INTERVAL: Duration = Duration::from_secs(5);
const MIN_MAILBOX_LIVENESS_EMIT_INTERVAL: Duration = Duration::from_millis(500);

// Defaults mirrored from vm_pty client behaviour.
const DEFAULT_VM_PTY_CONNECT_TIMEOUT: Duration = Duration::from_millis(1_500);
const DEFAULT_VM_PTY_REQUEST_TIMEOUT: Duration = Duration::from_millis(5_000);
const DEFAULT_VM_PTY_OPEN_TIMEOUT: Duration = Duration::from_millis(90_000);

pub(crate) const CONFIG_TOML_FILE: &str = "config.toml";

/// Ensure the Codex home directory exists.
fn ensure_codex_home(codex_home: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(codex_home)
}

/// Atomically write `contents` to `config.toml` at `config_path`.
fn write_config_toml_atomic(config_path: &Path, contents: &str) -> std::io::Result<()> {
    let Some(parent) = config_path.parent() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "config path has no parent directory",
        ));
    };
    ensure_codex_home(parent)?;
    let tmp_file = NamedTempFile::new_in(parent)?;
    std::fs::write(tmp_file.path(), contents)?;
    tmp_file.persist(config_path).map_err(|err| err.error)?;
    Ok(())
}

/// Application configuration loaded from disk and merged with overrides.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// Optional override of model selection.
    pub model: String,

    /// Model used specifically for review sessions. Defaults to "gpt-5-codex".
    pub review_model: String,

    pub model_family: ModelFamily,

    /// Size of the context window for the model, in tokens.
    pub model_context_window: Option<u64>,

    /// Maximum number of output tokens.
    pub model_max_output_tokens: Option<u64>,

    /// Token usage threshold triggering auto-compaction of conversation history.
    pub model_auto_compact_token_limit: Option<i64>,

    /// Key into the model_providers map that specifies which provider to use.
    pub model_provider_id: String,

    /// Info needed to make an API request to the model.
    pub model_provider: ModelProviderInfo,

    /// Approval policy for executing commands.
    pub approval_policy: AskForApproval,

    pub sandbox_policy: SandboxPolicy,

    pub shell_environment_policy: ShellEnvironmentPolicy,

    /// When `true`, `AgentReasoning` events emitted by the backend will be
    /// suppressed from the frontend output. This can reduce visual noise when
    /// users are only interested in the final agent responses.
    pub hide_agent_reasoning: bool,

    /// When set to `true`, `AgentReasoningRawContentEvent` events will be shown in the UI/output.
    /// Defaults to `false`.
    pub show_raw_agent_reasoning: bool,

    /// User-provided instructions from AGENTS.md.
    pub user_instructions: Option<String>,

    /// Base instructions override.
    pub base_instructions: Option<String>,

    /// Optional external notifier command. When set, Codex will spawn this
    /// program after each completed *turn* (i.e. when the agent finishes
    /// processing a user submission). The value must be the full command
    /// broken into argv tokens **without** the trailing JSON argument - Codex
    /// appends one extra argument containing a JSON payload describing the
    /// event.
    ///
    /// Example `~/.codex/config.toml` snippet:
    ///
    /// ```toml
    /// notify = ["notify-send", "Codex"]
    /// ```
    ///
    /// which will be invoked as:
    ///
    /// ```shell
    /// notify-send Codex '{"type":"agent-turn-complete","turn-id":"12345"}'
    /// ```
    ///
    /// If unset the feature is disabled.
    pub notify: Option<Vec<String>>,

    /// TUI notifications preference. When set, the TUI will send OSC 9 notifications on approvals
    /// and turn completions when not focused.
    pub tui_notifications: Notifications,

    /// The directory that should be treated as the current working directory
    /// for the session. All relative paths inside the business-logic layer are
    /// resolved against this path.
    pub cwd: PathBuf,

    /// Definition for MCP servers that Codex can reach out to for tool calls.
    pub mcp_servers: HashMap<String, McpServerConfig>,

    /// Preferred store for MCP OAuth credentials.
    /// keyring: Use an OS-specific keyring service.
    ///          Credentials stored in the keyring will only be readable by Codex unless the user explicitly grants access via OS-level keyring access.
    ///          https://github.com/openai/codex/blob/main/codex-rs/rmcp-client/src/oauth.rs#L2
    /// file: CODEX_HOME/.credentials.json
    ///       This file will be readable to Codex and other applications running as the same user.
    /// auto (default): keyring if available, otherwise file.
    pub mcp_oauth_credentials_store_mode: OAuthCredentialsStoreMode,

    /// Combined provider map (defaults merged with user-defined overrides).
    pub model_providers: HashMap<String, ModelProviderInfo>,

    /// Maximum number of bytes to include from an AGENTS.md project doc file.
    pub project_doc_max_bytes: usize,

    /// Additional filenames to try when looking for project-level docs.
    pub project_doc_fallback_filenames: Vec<String>,

    /// Directory containing all Codex state (defaults to `~/.codex` but can be
    /// overridden by the `CODEX_HOME` environment variable).
    pub codex_home: PathBuf,

    /// Settings that govern if and what will be written to `~/.codex/history.jsonl`.
    pub history: History,

    /// Optional URI-based file opener. If set, citations to files in the model
    /// output will be hyperlinked using the specified URI scheme.
    pub file_opener: UriBasedFileOpener,

    /// Path to the `codex-linux-sandbox` executable. This must be set if
    /// [`crate::exec::SandboxType::LinuxSeccomp`] is used. Note that this
    /// cannot be set in the config file: it must be set in code via
    /// [`ConfigOverrides`].
    ///
    /// When this program is invoked, arg0 will be set to `codex-linux-sandbox`.
    pub codex_linux_sandbox_exe: Option<PathBuf>,

    /// Value to use for `reasoning.effort` when making a request using the
    /// Responses API.
    pub model_reasoning_effort: Option<ReasoningEffort>,

    /// If not "none", the value to use for `reasoning.summary` when making a
    /// request using the Responses API.
    pub model_reasoning_summary: ReasoningSummary,

    /// Optional verbosity control for GPT-5 models (Responses API `text.verbosity`).
    pub model_verbosity: Option<Verbosity>,

    /// Base URL for requests to ChatGPT (as opposed to the OpenAI API).
    pub chatgpt_base_url: String,

    /// Include an experimental plan tool that the model can use to update its current plan and status of each step.
    pub include_plan_tool: bool,

    /// Include the `apply_patch` tool for models that benefit from invoking
    /// file edits as a structured tool call. When unset, this falls back to the
    /// model family's default preference.
    pub include_apply_patch_tool: bool,

    pub tools_web_search_request: bool,

    pub use_experimental_streamable_shell_tool: bool,

    /// If set to `true`, used only the experimental unified exec tool.
    pub use_experimental_unified_exec_tool: bool,

    /// If set to `true`, use the experimental official Rust MCP client.
    /// https://github.com/modelcontextprotocol/rust-sdk
    pub use_experimental_use_rmcp_client: bool,

    /// Include the `view_image` tool that lets the agent attach a local image path to context.
    pub include_view_image_tool: bool,

    /// Whether to include host shell tools for this session.
    pub include_shell_tool: bool,

    /// Whether to attempt to include the VM-backed PTY tool for this session.
    pub include_vm_pty_tool: bool,

    /// Whether to expose the management-only pty_open tool.
    pub include_vm_pty_open_tool: bool,

    /// When true, print enabled tool list for debugging.
    pub debug_tools: bool,

    /// Resolved vm-pty socket endpoint, if configured.
    pub vm_pty_socket: Option<PathBuf>,
    /// Default vm id to use when a tool call does not specify one.
    pub vm_pty_default_vm_id: Option<String>,
    /// Timeout for connecting to the vm-pty daemon.
    pub vm_pty_connect_timeout: Duration,
    /// Timeout for individual vm-pty requests.
    pub vm_pty_request_timeout: Duration,
    /// Timeout to use specifically for the pty_open call (can be higher on cold boots).
    pub vm_pty_open_timeout: Duration,
    /// If true, request a blocking open; when false (default), request nonblocking.
    pub vm_pty_open_blocking: bool,

    /// The active profile name used to derive this `Config` (if any).
    pub active_profile: Option<String>,

    /// Tracks whether the Windows onboarding screen has been acknowledged.
    pub windows_wsl_setup_acknowledged: bool,

    /// When true, disables burst-paste detection for typed input entirely.
    /// All characters are inserted as they are received, and no buffering
    /// or placeholder replacement will occur for fast keypress bursts.
    pub disable_paste_burst: bool,

    /// OTEL configuration (exporter type, endpoint, headers, etc.).
    pub otel: crate::config_types::OtelConfig,

    /// Settings controlling how mailbox heartbeats are promoted to liveness telemetry.
    pub mailbox_liveness: MailboxLivenessSettings,

    /// Settings controlling background conversation summaries.
    pub summaries: SummariesSettings,

    /// Wait predicate policy enforcement knobs.
    pub wait: WaitPolicySettings,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MailboxLivenessSettings {
    pub enabled: bool,
    pub idle_after: Duration,
    pub stalled_after: Duration,
    pub emit_interval: Duration,
}

impl Default for MailboxLivenessSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            idle_after: DEFAULT_MAILBOX_LIVENESS_IDLE_AFTER,
            stalled_after: DEFAULT_MAILBOX_LIVENESS_STALLED_AFTER,
            emit_interval: DEFAULT_MAILBOX_LIVENESS_EMIT_INTERVAL,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummariesSettings {
    pub enabled: bool,
    pub emit_interval: Duration,
    pub initial_delay: Duration,
    /// Optional base directory override for summary checkpoints.
    pub base_dir: Option<PathBuf>,
}

const DEFAULT_SUMMARIES_EMIT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_SUMMARIES_INITIAL_DELAY: Duration = Duration::from_secs(10);
const MIN_SUMMARIES_EMIT_INTERVAL: Duration = Duration::from_millis(500);

impl Default for SummariesSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            emit_interval: DEFAULT_SUMMARIES_EMIT_INTERVAL,
            initial_delay: DEFAULT_SUMMARIES_INITIAL_DELAY,
            base_dir: None,
        }
    }
}

impl SummariesSettings {
    pub fn from_toml(toml: Option<SummariesToml>) -> Self {
        let raw = toml.unwrap_or_default();
        let mut s = Self::default();
        if let Some(enabled) = raw.enabled {
            s.enabled = enabled;
        }
        if let Some(secs) = raw.emit_interval_seconds {
            if let Ok(d) = Duration::try_from_secs_f64(secs) {
                s.emit_interval = d;
            }
        }
        if let Some(secs) = raw.initial_delay_seconds {
            if let Ok(d) = Duration::try_from_secs_f64(secs) {
                s.initial_delay = d;
            }
        }
        if let Some(base) = raw.base_dir {
            s.base_dir = Some(base);
        }
        s.normalize();
        s
    }

    fn normalize(&mut self) {
        if self.emit_interval < MIN_SUMMARIES_EMIT_INTERVAL {
            self.emit_interval = MIN_SUMMARIES_EMIT_INTERVAL;
        }
    }
}

const DEFAULT_WAIT_MAX_DURATION: Duration = Duration::from_secs(3_600);
const MAX_WAIT_DURATION_LIMIT: Duration = Duration::from_secs(86_400);
const DEFAULT_MAX_WAITS_PER_TURN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WaitPredicateKind {
    Timer,
    Filesystem,
    Shell,
    Mailbox,
}

impl WaitPredicateKind {
    fn all() -> [Self; 4] {
        [Self::Timer, Self::Filesystem, Self::Shell, Self::Mailbox]
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Timer => "timer",
            Self::Filesystem => "filesystem",
            Self::Shell => "shell",
            Self::Mailbox => "mailbox",
        }
    }
}

impl fmt::Display for WaitPredicateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WaitPredicateKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "timer" => Ok(Self::Timer),
            "filesystem" => Ok(Self::Filesystem),
            "shell" => Ok(Self::Shell),
            "mailbox" => Ok(Self::Mailbox),
            other => Err(format!("invalid wait predicate `{other}`")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WaitPolicySettings {
    pub allowed_predicates: BTreeSet<WaitPredicateKind>,
    pub max_duration: Duration,
    pub max_waits_per_turn: usize,
    pub require_shell_approval: bool,
}

impl Default for WaitPolicySettings {
    fn default() -> Self {
        Self {
            allowed_predicates: WaitPredicateKind::all().into_iter().collect(),
            max_duration: DEFAULT_WAIT_MAX_DURATION,
            max_waits_per_turn: DEFAULT_MAX_WAITS_PER_TURN,
            require_shell_approval: false,
        }
    }
}

impl WaitPolicySettings {
    fn parse_allowed(predicates: &[String]) -> Result<BTreeSet<WaitPredicateKind>, String> {
        let mut set = BTreeSet::new();
        for raw in predicates {
            let parsed = WaitPredicateKind::from_str(raw)?;
            set.insert(parsed);
        }
        Ok(set)
    }

    pub fn from_toml(toml: Option<&WaitPolicyToml>) -> Result<Self, String> {
        let mut settings = Self::default();
        if let Some(config) = toml {
            settings = settings.apply_toml(config)?;
        }
        settings.normalize();
        Ok(settings)
    }

    fn apply_toml(mut self, toml: &WaitPolicyToml) -> Result<Self, String> {
        if let Some(predicates) = &toml.allowed_predicates {
            let parsed = Self::parse_allowed(predicates)?;
            self.allowed_predicates = parsed;
        }
        if let Some(seconds) = toml.max_duration_seconds {
            if seconds == 0 {
                return Err("wait.max_duration_seconds must be positive".to_string());
            }
            self.max_duration = Duration::from_secs(seconds);
        }
        if let Some(limit) = toml.max_waits_per_turn {
            if limit == 0 {
                return Err("wait.max_waits_per_turn must be >= 1".to_string());
            }
            self.max_waits_per_turn = limit;
        }
        if let Some(require) = toml.require_shell_approval {
            self.require_shell_approval = require;
        }
        Ok(self)
    }

    pub fn apply_override(&mut self, overrides: &WaitPolicyOverrides) -> Result<(), String> {
        if let Some(predicates) = &overrides.allowed_predicates {
            self.allowed_predicates = predicates.iter().copied().collect();
        }
        if let Some(seconds) = overrides.max_duration_seconds {
            if seconds == 0 {
                return Err(
                    "wait policy override max_duration_seconds must be positive".to_string()
                );
            }
            self.max_duration = Duration::from_secs(seconds);
        }
        if let Some(limit) = overrides.max_waits_per_turn {
            if limit == 0 {
                return Err("wait policy override max_waits_per_turn must be >= 1".to_string());
            }
            self.max_waits_per_turn = limit;
        }
        if let Some(require) = overrides.require_shell_approval {
            self.require_shell_approval = require;
        }
        self.normalize();
        Ok(())
    }

    fn normalize(&mut self) {
        if self.max_duration.is_zero() {
            self.max_duration = DEFAULT_WAIT_MAX_DURATION;
        }

        if self.max_duration > MAX_WAIT_DURATION_LIMIT {
            self.max_duration = MAX_WAIT_DURATION_LIMIT;
        }

        if self.max_waits_per_turn == 0 {
            self.max_waits_per_turn = 1;
        }
    }

    pub fn is_allowed(&self, kind: WaitPredicateKind) -> bool {
        self.allowed_predicates.contains(&kind)
    }

    pub fn max_duration(&self) -> Duration {
        self.max_duration
    }

    pub fn max_waits_per_turn(&self) -> usize {
        self.max_waits_per_turn
    }

    pub fn require_shell_approval(&self) -> bool {
        self.require_shell_approval
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct WaitPolicyToml {
    pub allowed_predicates: Option<Vec<String>>,
    #[serde(rename = "max_duration_seconds")]
    pub max_duration_seconds: Option<u64>,
    pub max_waits_per_turn: Option<usize>,
    pub require_shell_approval: Option<bool>,
}

#[derive(Default, Debug, Clone)]
pub struct WaitPolicyOverrides {
    pub allowed_predicates: Option<Vec<WaitPredicateKind>>,
    pub max_duration_seconds: Option<u64>,
    pub max_waits_per_turn: Option<usize>,
    pub require_shell_approval: Option<bool>,
}

static WAIT_POLICY_GLOBAL: OnceLock<RwLock<WaitPolicySettings>> = OnceLock::new();

fn wait_policy_slot() -> &'static RwLock<WaitPolicySettings> {
    WAIT_POLICY_GLOBAL.get_or_init(|| RwLock::new(WaitPolicySettings::default()))
}

pub fn wait_policy_settings() -> WaitPolicySettings {
    wait_policy_slot()
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| WaitPolicySettings::default())
}

fn update_wait_policy(settings: &WaitPolicySettings) {
    if let Ok(mut guard) = wait_policy_slot().write() {
        *guard = settings.clone();
    }
}

impl MailboxLivenessSettings {
    pub fn from_toml(toml: Option<MailboxLivenessToml>) -> Self {
        let raw = toml.unwrap_or_default();
        let mut settings = Self::default();
        if let Some(enabled) = raw.enabled {
            settings.enabled = enabled;
        }
        if let Some(idle_after) = raw.idle_after_seconds {
            if let Ok(duration) = Duration::try_from_secs_f64(idle_after) {
                settings.idle_after = duration;
            }
        }
        if let Some(stalled_after) = raw.stalled_after_seconds {
            if let Ok(duration) = Duration::try_from_secs_f64(stalled_after) {
                settings.stalled_after = duration;
            }
        }
        if let Some(emit_interval) = raw.emit_interval_seconds {
            if let Ok(duration) = Duration::try_from_secs_f64(emit_interval) {
                settings.emit_interval = duration;
            }
        }
        settings.normalize();
        settings
    }

    pub fn normalize(&mut self) {
        if self.idle_after.is_zero() {
            self.idle_after = DEFAULT_MAILBOX_LIVENESS_IDLE_AFTER;
        }

        if self.stalled_after <= self.idle_after {
            self.stalled_after = self.idle_after + Duration::from_secs(1);
        }

        if self.emit_interval < MIN_MAILBOX_LIVENESS_EMIT_INTERVAL {
            self.emit_interval = MIN_MAILBOX_LIVENESS_EMIT_INTERVAL;
        }
    }
}

impl Config {
    pub async fn load_with_cli_overrides(
        cli_overrides: Vec<(String, TomlValue)>,
        overrides: ConfigOverrides,
    ) -> std::io::Result<Self> {
        let codex_home = find_codex_home()?;
        // Capture CLI overrides for post-profile precedence on select keys.
        let overrides_for_resolution = cli_overrides.clone();

        let root_value = load_resolved_config(
            &codex_home,
            cli_overrides,
            crate::config_loader::LoaderOverrides::default(),
        )
        .await?;

        let cfg: ConfigToml = root_value.try_into().map_err(|e| {
            tracing::error!("Failed to deserialize overridden config: {e}");
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let mut overrides = overrides;
        overrides.cli_resolved_overrides = extract_cli_resolved_overrides(&overrides_for_resolution);

        Self::load_from_base_config_with_overrides(cfg, overrides, codex_home)
    }
}

fn extract_cli_resolved_overrides(
    cli_overrides: &[(String, TomlValue)],
) -> Option<CliResolvedOverrides> {
    let mut out = CliResolvedOverrides::default();
    let mut any = false;

    for (path, value) in cli_overrides.iter() {
        match path.as_str() {
            "tools.vm_pty.socket" => {
                if let Some(s) = value.as_str() {
                    out.vm_pty_socket = Some(PathBuf::from(s));
                    any = true;
                }
            }
            "vm_pty.default_vm_id" => {
                if let Some(s) = value.as_str() {
                    out.vm_pty_default_vm_id = Some(s.to_string());
                    any = true;
                }
            }
            "vm_pty.timeouts.connect_ms" => {
                if let Some(v) = value.as_integer() {
                    out.vm_pty_timeouts.connect_ms = Some(v as u64);
                    any = true;
                } else if let Some(s) = value.as_str() {
                    if let Ok(parsed) = s.trim().parse::<u64>() {
                        out.vm_pty_timeouts.connect_ms = Some(parsed);
                        any = true;
                    }
                }
            }
            "vm_pty.timeouts.request_ms" => {
                if let Some(v) = value.as_integer() {
                    out.vm_pty_timeouts.request_ms = Some(v as u64);
                    any = true;
                } else if let Some(s) = value.as_str() {
                    if let Ok(parsed) = s.trim().parse::<u64>() {
                        out.vm_pty_timeouts.request_ms = Some(parsed);
                        any = true;
                    }
                }
            }
            "vm_pty.timeouts.open_ms" => {
                if let Some(v) = value.as_integer() {
                    out.vm_pty_timeouts.open_ms = Some(v as u64);
                    any = true;
                } else if let Some(s) = value.as_str() {
                    if let Ok(parsed) = s.trim().parse::<u64>() {
                        out.vm_pty_timeouts.open_ms = Some(parsed);
                        any = true;
                    }
                }
            }
            "vm_pty.open_blocking" => {
                if let Some(b) = value.as_bool() {
                    out.vm_pty_open_blocking = Some(b);
                    any = true;
                } else if let Some(s) = value.as_str() {
                    let sl = s.trim().to_ascii_lowercase();
                    if matches!(sl.as_str(), "1" | "true" | "yes") {
                        out.vm_pty_open_blocking = Some(true);
                        any = true;
                    } else if matches!(sl.as_str(), "0" | "false" | "no") {
                        out.vm_pty_open_blocking = Some(false);
                        any = true;
                    }
                }
            }
            "debug.tools" => {
                if let Some(b) = value.as_bool() {
                    out.debug_tools = Some(b);
                    any = true;
                } else if let Some(s) = value.as_str() {
                    let sl = s.trim().to_ascii_lowercase();
                    if matches!(sl.as_str(), "1" | "true" | "yes") {
                        out.debug_tools = Some(true);
                        any = true;
                    } else if matches!(sl.as_str(), "0" | "false" | "no") {
                        out.debug_tools = Some(false);
                        any = true;
                    }
                }
            }
            _ => {}
        }
    }

    if any { Some(out) } else { None }
}

pub async fn load_config_as_toml_with_cli_overrides(
    codex_home: &Path,
    cli_overrides: Vec<(String, TomlValue)>,
) -> std::io::Result<ConfigToml> {
    let root_value = load_resolved_config(
        codex_home,
        cli_overrides,
        crate::config_loader::LoaderOverrides::default(),
    )
    .await?;

    let cfg: ConfigToml = root_value.try_into().map_err(|e| {
        tracing::error!("Failed to deserialize overridden config: {e}");
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;

    Ok(cfg)
}

async fn load_resolved_config(
    codex_home: &Path,
    cli_overrides: Vec<(String, TomlValue)>,
    overrides: crate::config_loader::LoaderOverrides,
) -> std::io::Result<TomlValue> {
    let layers = load_config_layers_with_overrides(codex_home, overrides).await?;
    Ok(apply_overlays(layers, cli_overrides))
}

fn apply_overlays(
    layers: LoadedConfigLayers,
    cli_overrides: Vec<(String, TomlValue)>,
) -> TomlValue {
    let LoadedConfigLayers {
        mut base,
        managed_config,
        managed_preferences,
    } = layers;

    for (path, value) in cli_overrides.into_iter() {
        apply_toml_override(&mut base, &path, value);
    }

    for overlay in [managed_config, managed_preferences].into_iter().flatten() {
        merge_toml_values(&mut base, &overlay);
    }

    base
}

pub async fn load_global_mcp_servers(
    codex_home: &Path,
) -> std::io::Result<BTreeMap<String, McpServerConfig>> {
    let root_value = load_config_as_toml(codex_home).await?;
    let Some(servers_value) = root_value.get("mcp_servers") else {
        return Ok(BTreeMap::new());
    };

    ensure_no_inline_bearer_tokens(servers_value)?;

    servers_value
        .clone()
        .try_into()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// We briefly allowed plain text bearer_token fields in MCP server configs.
/// We want to warn people who recently added these fields but can remove this after a few months.
fn ensure_no_inline_bearer_tokens(value: &TomlValue) -> std::io::Result<()> {
    let Some(servers_table) = value.as_table() else {
        return Ok(());
    };

    for (server_name, server_value) in servers_table {
        if let Some(server_table) = server_value.as_table()
            && server_table.contains_key("bearer_token")
        {
            let message = format!(
                "mcp_servers.{server_name} uses unsupported `bearer_token`; set `bearer_token_env_var`."
            );
            return Err(std::io::Error::new(ErrorKind::InvalidData, message));
        }
    }

    Ok(())
}

pub fn write_global_mcp_servers(
    codex_home: &Path,
    servers: &BTreeMap<String, McpServerConfig>,
) -> std::io::Result<()> {
    let config_path = codex_home.join(CONFIG_TOML_FILE);
    let mut doc = match std::fs::read_to_string(&config_path) {
        Ok(contents) => contents
            .parse::<DocumentMut>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DocumentMut::new(),
        Err(e) => return Err(e),
    };

    doc.as_table_mut().remove("mcp_servers");

    if !servers.is_empty() {
        let mut table = TomlTable::new();
        table.set_implicit(true);
        doc["mcp_servers"] = TomlItem::Table(table);

        for (name, config) in servers {
            let mut entry = TomlTable::new();
            entry.set_implicit(false);
            match &config.transport {
                McpServerTransportConfig::Stdio { command, args, env } => {
                    entry["command"] = toml_edit::value(command.clone());

                    if !args.is_empty() {
                        let mut args_array = TomlArray::new();
                        for arg in args {
                            args_array.push(arg.clone());
                        }
                        entry["args"] = TomlItem::Value(args_array.into());
                    }

                    if let Some(env) = env
                        && !env.is_empty()
                    {
                        let mut env_table = TomlTable::new();
                        env_table.set_implicit(false);
                        let mut pairs: Vec<_> = env.iter().collect();
                        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
                        for (key, value) in pairs {
                            env_table.insert(key, toml_edit::value(value.clone()));
                        }
                        entry["env"] = TomlItem::Table(env_table);
                    }
                }
                McpServerTransportConfig::StreamableHttp {
                    url,
                    bearer_token_env_var,
                } => {
                    entry["url"] = toml_edit::value(url.clone());
                    if let Some(env_var) = bearer_token_env_var {
                        entry["bearer_token_env_var"] = toml_edit::value(env_var.clone());
                    }
                }
            }

            if !config.enabled {
                entry["enabled"] = toml_edit::value(false);
            }

            if let Some(timeout) = config.startup_timeout_sec {
                entry["startup_timeout_sec"] = toml_edit::value(timeout.as_secs_f64());
            }

            if let Some(timeout) = config.tool_timeout_sec {
                entry["tool_timeout_sec"] = toml_edit::value(timeout.as_secs_f64());
            }

            doc["mcp_servers"][name.as_str()] = TomlItem::Table(entry);
        }
    }

    write_config_toml_atomic(&config_path, &doc.to_string())
}

fn set_project_trusted_inner(doc: &mut DocumentMut, project_path: &Path) -> anyhow::Result<()> {
    // Ensure we render a human-friendly structure:
    //
    // [projects]
    // [projects."/path/to/project"]
    // trust_level = "trusted"
    //
    // rather than inline tables like:
    //
    // [projects]
    // "/path/to/project" = { trust_level = "trusted" }
    let project_key = project_path.to_string_lossy().to_string();

    // Ensure top-level `projects` exists as a non-inline, explicit table. If it
    // exists but was previously represented as a non-table (e.g., inline),
    // replace it with an explicit table.
    {
        let root = doc.as_table_mut();
        // If `projects` exists but isn't a standard table (e.g., it's an inline table),
        // convert it to an explicit table while preserving existing entries.
        let existing_projects = root.get("projects").cloned();
        if existing_projects.as_ref().is_none_or(|i| !i.is_table()) {
            let mut projects_tbl = toml_edit::Table::new();
            projects_tbl.set_implicit(true);

            // If there was an existing inline table, migrate its entries to explicit tables.
            if let Some(inline_tbl) = existing_projects.as_ref().and_then(|i| i.as_inline_table()) {
                for (k, v) in inline_tbl.iter() {
                    if let Some(inner_tbl) = v.as_inline_table() {
                        let new_tbl = inner_tbl.clone().into_table();
                        projects_tbl.insert(k, toml_edit::Item::Table(new_tbl));
                    }
                }
            }

            root.insert("projects", toml_edit::Item::Table(projects_tbl));
        }
    }
    let Some(projects_tbl) = doc["projects"].as_table_mut() else {
        return Err(anyhow::anyhow!(
            "projects table missing after initialization"
        ));
    };

    // Ensure the per-project entry is its own explicit table. If it exists but
    // is not a table (e.g., an inline table), replace it with an explicit table.
    let needs_proj_table = !projects_tbl.contains_key(project_key.as_str())
        || projects_tbl
            .get(project_key.as_str())
            .and_then(|i| i.as_table())
            .is_none();
    if needs_proj_table {
        projects_tbl.insert(project_key.as_str(), toml_edit::table());
    }
    let Some(proj_tbl) = projects_tbl
        .get_mut(project_key.as_str())
        .and_then(|i| i.as_table_mut())
    else {
        return Err(anyhow::anyhow!("project table missing for {project_key}"));
    };
    proj_tbl.set_implicit(false);
    proj_tbl["trust_level"] = toml_edit::value("trusted");
    Ok(())
}

/// Patch `CODEX_HOME/config.toml` project state.
/// Use with caution.
pub fn set_project_trusted(codex_home: &Path, project_path: &Path) -> anyhow::Result<()> {
    let config_path = codex_home.join(CONFIG_TOML_FILE);
    // Parse existing config if present; otherwise start a new document.
    let mut doc = match std::fs::read_to_string(config_path.clone()) {
        Ok(s) => s.parse::<DocumentMut>()?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DocumentMut::new(),
        Err(e) => return Err(e.into()),
    };

    set_project_trusted_inner(&mut doc, project_path)?;
    write_config_toml_atomic(&config_path, &doc.to_string())?;
    Ok(())
}

/// Persist the acknowledgement flag for the Windows onboarding screen.
pub fn set_windows_wsl_setup_acknowledged(
    codex_home: &Path,
    acknowledged: bool,
) -> anyhow::Result<()> {
    let config_path = codex_home.join(CONFIG_TOML_FILE);
    let mut doc = match std::fs::read_to_string(config_path.clone()) {
        Ok(s) => s.parse::<DocumentMut>()?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DocumentMut::new(),
        Err(e) => return Err(e.into()),
    };

    doc["windows_wsl_setup_acknowledged"] = toml_edit::value(acknowledged);

    write_config_toml_atomic(&config_path, &doc.to_string())?;
    Ok(())
}

fn ensure_profile_table<'a>(
    doc: &'a mut DocumentMut,
    profile_name: &str,
) -> anyhow::Result<&'a mut toml_edit::Table> {
    let mut created_profiles_table = false;
    {
        let root = doc.as_table_mut();
        let needs_table = !root.contains_key("profiles")
            || root
                .get("profiles")
                .and_then(|item| item.as_table())
                .is_none();
        if needs_table {
            root.insert("profiles", toml_edit::table());
            created_profiles_table = true;
        }
    }

    let Some(profiles_table) = doc["profiles"].as_table_mut() else {
        return Err(anyhow::anyhow!(
            "profiles table missing after initialization"
        ));
    };

    if created_profiles_table {
        profiles_table.set_implicit(true);
    }

    let needs_profile_table = !profiles_table.contains_key(profile_name)
        || profiles_table
            .get(profile_name)
            .and_then(|item| item.as_table())
            .is_none();
    if needs_profile_table {
        profiles_table.insert(profile_name, toml_edit::table());
    }

    let Some(profile_table) = profiles_table
        .get_mut(profile_name)
        .and_then(|item| item.as_table_mut())
    else {
        return Err(anyhow::anyhow!(format!(
            "profile table missing for {profile_name}"
        )));
    };

    profile_table.set_implicit(false);
    Ok(profile_table)
}

// TODO(jif) refactor config persistence.
pub async fn persist_model_selection(
    codex_home: &Path,
    active_profile: Option<&str>,
    model: &str,
    effort: Option<ReasoningEffort>,
) -> anyhow::Result<()> {
    let effort_string = effort.as_ref().map(|e| e.to_string());
    let effort_ref = effort_string.as_deref();

    persist_overrides_and_clear_if_none(
        codex_home,
        active_profile,
        &[
            (&[CONFIG_KEY_MODEL], Some(model)),
            (&[CONFIG_KEY_EFFORT], effort_ref),
        ],
    )
    .await
}

/// Apply a single dotted-path override onto a TOML value.
fn apply_toml_override(root: &mut TomlValue, path: &str, value: TomlValue) {
    use toml::value::Table;

    let segments: Vec<&str> = path.split('.').collect();
    let mut current = root;

    for (idx, segment) in segments.iter().enumerate() {
        let is_last = idx == segments.len() - 1;

        if is_last {
            match current {
                TomlValue::Table(table) => {
                    table.insert(segment.to_string(), value);
                }
                _ => {
                    let mut table = Table::new();
                    table.insert(segment.to_string(), value);
                    *current = TomlValue::Table(table);
                }
            }
            return;
        }

        // Traverse or create intermediate object.
        match current {
            TomlValue::Table(table) => {
                current = table
                    .entry(segment.to_string())
                    .or_insert_with(|| TomlValue::Table(Table::new()));
            }
            _ => {
                *current = TomlValue::Table(Table::new());
                if let TomlValue::Table(tbl) = current {
                    current = tbl
                        .entry(segment.to_string())
                        .or_insert_with(|| TomlValue::Table(Table::new()));
                }
            }
        }
    }
}

/// Base config deserialized from ~/.codex/config.toml.
#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
pub struct ConfigToml {
    /// Optional override of model selection.
    pub model: Option<String>,
    /// Review model override used by the `/review` feature.
    pub review_model: Option<String>,

    /// Provider to use from the model_providers map.
    pub model_provider: Option<String>,

    /// Size of the context window for the model, in tokens.
    pub model_context_window: Option<u64>,

    /// Maximum number of output tokens.
    pub model_max_output_tokens: Option<u64>,

    /// Token usage threshold triggering auto-compaction of conversation history.
    pub model_auto_compact_token_limit: Option<i64>,

    /// Default approval policy for executing commands.
    pub approval_policy: Option<AskForApproval>,

    #[serde(default)]
    pub shell_environment_policy: ShellEnvironmentPolicyToml,

    /// Sandbox mode to use.
    pub sandbox_mode: Option<SandboxMode>,

    /// Sandbox configuration to apply if `sandbox` is `WorkspaceWrite`.
    pub sandbox_workspace_write: Option<SandboxWorkspaceWrite>,

    /// Optional external command to spawn for end-user notifications.
    #[serde(default)]
    pub notify: Option<Vec<String>>,

    /// System instructions.
    pub instructions: Option<String>,

    /// Definition for MCP servers that Codex can reach out to for tool calls.
    #[serde(default)]
    pub mcp_servers: HashMap<String, McpServerConfig>,

    /// Preferred backend for storing MCP OAuth credentials.
    /// keyring: Use an OS-specific keyring service.
    ///          https://github.com/openai/codex/blob/main/codex-rs/rmcp-client/src/oauth.rs#L2
    /// file: Use a file in the Codex home directory.
    /// auto (default): Use the OS-specific keyring service if available, otherwise use a file.
    #[serde(default)]
    pub mcp_oauth_credentials_store: Option<OAuthCredentialsStoreMode>,

    /// User-defined provider entries that extend/override the built-in list.
    #[serde(default)]
    pub model_providers: HashMap<String, ModelProviderInfo>,

    /// Maximum number of bytes to include from an AGENTS.md project doc file.
    pub project_doc_max_bytes: Option<usize>,

    /// Ordered list of fallback filenames to look for when AGENTS.md is missing.
    pub project_doc_fallback_filenames: Option<Vec<String>>,

    /// Profile to use from the `profiles` map.
    pub profile: Option<String>,

    /// Named profiles to facilitate switching between different configurations.
    #[serde(default)]
    pub profiles: HashMap<String, ConfigProfile>,

    /// Settings that govern if and what will be written to `~/.codex/history.jsonl`.
    #[serde(default)]
    pub history: Option<History>,

    /// Optional URI-based file opener. If set, citations to files in the model
    /// output will be hyperlinked using the specified URI scheme.
    pub file_opener: Option<UriBasedFileOpener>,

    /// Collection of settings that are specific to the TUI.
    pub tui: Option<Tui>,

    #[serde(default)]
    pub mailbox_liveness: Option<MailboxLivenessToml>,

    #[serde(default)]
    pub wait: Option<WaitPolicyToml>,

    #[serde(default)]
    pub wait_profiles: HashMap<String, WaitPolicyToml>,

    /// When set to `true`, `AgentReasoning` events will be hidden from the
    /// UI/output. Defaults to `false`.
    pub hide_agent_reasoning: Option<bool>,

    /// When set to `true`, `AgentReasoningRawContentEvent` events will be shown in the UI/output.
    /// Defaults to `false`.
    pub show_raw_agent_reasoning: Option<bool>,

    pub model_reasoning_effort: Option<ReasoningEffort>,
    pub model_reasoning_summary: Option<ReasoningSummary>,
    /// Optional verbosity control for GPT-5 models (Responses API `text.verbosity`).
    pub model_verbosity: Option<Verbosity>,

    /// Override to force-enable reasoning summaries for the configured model.
    pub model_supports_reasoning_summaries: Option<bool>,

    /// Override to force reasoning summary format for the configured model.
    pub model_reasoning_summary_format: Option<ReasoningSummaryFormat>,

    /// Base URL for requests to ChatGPT (as opposed to the OpenAI API).
    pub chatgpt_base_url: Option<String>,

    /// Experimental path to a file whose contents replace the built-in BASE_INSTRUCTIONS.
    pub experimental_instructions_file: Option<PathBuf>,

    pub experimental_use_exec_command_tool: Option<bool>,
    pub experimental_use_unified_exec_tool: Option<bool>,
    pub experimental_use_rmcp_client: Option<bool>,
    pub experimental_use_freeform_apply_patch: Option<bool>,

    pub projects: Option<HashMap<String, ProjectConfig>>,

    /// Nested tools section for feature toggles
    pub tools: Option<ToolsToml>,

    /// VM-backed PTY feature flag configuration.
    #[serde(default)]
    pub vm_pty: VmPtyToml,

    /// Debug toggles.
    #[serde(default)]
    pub debug: Option<DebugToml>,

    /// When true, disables burst-paste detection for typed input entirely.
    /// All characters are inserted as they are received, and no buffering
    /// or placeholder replacement will occur for fast keypress bursts.
    pub disable_paste_burst: Option<bool>,

    /// OTEL configuration.
    pub otel: Option<crate::config_types::OtelConfigToml>,

    /// Background conversation summaries configuration.
    #[serde(default)]
    pub summaries: Option<SummariesToml>,

    /// Tracks whether the Windows onboarding screen has been acknowledged.
    pub windows_wsl_setup_acknowledged: Option<bool>,
}

impl From<ConfigToml> for UserSavedConfig {
    fn from(config_toml: ConfigToml) -> Self {
        let profiles = config_toml
            .profiles
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();

        Self {
            approval_policy: config_toml.approval_policy,
            sandbox_mode: config_toml.sandbox_mode,
            sandbox_settings: config_toml.sandbox_workspace_write.map(From::from),
            model: config_toml.model,
            model_reasoning_effort: config_toml.model_reasoning_effort,
            model_reasoning_summary: config_toml.model_reasoning_summary,
            model_verbosity: config_toml.model_verbosity,
            tools: config_toml.tools.map(From::from),
            profile: config_toml.profile,
            profiles,
        }
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectConfig {
    pub trust_level: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
pub struct ToolsToml {
    #[serde(default, alias = "web_search_request")]
    pub web_search: Option<bool>,

    /// Enable the `view_image` tool that lets the agent attach local images.
    #[serde(default)]
    pub view_image: Option<bool>,

    /// Shell tool configuration and profile gating.
    #[serde(default)]
    pub shell: Option<ShellToolsToml>,

    /// Experimental VM-backed PTY lane configuration.
    #[serde(default)]
    pub vm_pty: VmPtyToml,

    /// Explicit toggle for the unified_exec tool. When absent, defaults to false.
    /// This allows enabling the VM PTY lane without automatically exposing a
    /// general-purpose exec tool to the agent.
    #[serde(default)]
    pub unified_exec_enabled: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct DebugToml {
    /// When true, print the enabled tool list at startup.
    pub tools: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct VmPtyToml {
    /// Global feature flag for the VM-backed PTY lane.
    pub enabled: Option<bool>,
    /// Active profile names that are allowed to use the PTY lane.
    #[serde(default)]
    pub allow_profiles: Vec<String>,
    /// Optional vm-pty daemon socket path.
    pub socket: Option<PathBuf>,
    /// Default VM identifier to target when omitted by tool params.
    pub default_vm_id: Option<String>,
    /// Millisecond timeout knobs for vm-pty.
    #[serde(default)]
    pub timeouts: VmPtyTimeoutsToml,
    /// Request a blocking open; defaults to nonblocking when unset.
    pub open_blocking: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct VmPtyTimeoutsToml {
    pub connect_ms: Option<u64>,
    pub request_ms: Option<u64>,
    pub open_ms: Option<u64>,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct ShellToolsToml {
    /// Enable the `shell`/`container_exec` tools (default: true).
    pub enabled: Option<bool>,
    /// Profiles explicitly allowed to use the shell tool. Empty → allow all.
    #[serde(default)]
    pub allow_profiles: Vec<String>,
    /// Profiles explicitly disallowed from using the shell tool.
    #[serde(default)]
    pub disallow_profiles: Vec<String>,
}

impl From<ToolsToml> for Tools {
    fn from(tools_toml: ToolsToml) -> Self {
        Self {
            web_search: tools_toml.web_search,
            view_image: tools_toml.view_image,
            vm_pty_lane: tools_toml.vm_pty.enabled,
        }
    }
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct MailboxLivenessToml {
    pub enabled: Option<bool>,
    #[serde(rename = "idle_after_seconds")]
    pub idle_after_seconds: Option<f64>,
    #[serde(rename = "stalled_after_seconds")]
    pub stalled_after_seconds: Option<f64>,
    #[serde(rename = "emit_interval_seconds")]
    pub emit_interval_seconds: Option<f64>,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct SummariesToml {
    pub enabled: Option<bool>,
    /// Interval between background summary updates, in seconds.
    #[serde(rename = "emit_interval_seconds")]
    pub emit_interval_seconds: Option<f64>,
    /// Initial startup delay before emitting the first summary, in seconds.
    #[serde(rename = "initial_delay_seconds")]
    pub initial_delay_seconds: Option<f64>,
    /// Optional base directory for summary checkpoints.
    pub base_dir: Option<PathBuf>,
}

/// Return a configured base directory for summaries if overridden by
/// environment or config. Precedence:
/// 1) CODEX_SUMMARIES_BASE_DIR env (non-empty)
/// 2) CODEX_HOME/config.toml [summaries].base_dir
pub fn summaries_base_dir_override() -> Option<PathBuf> {
    if let Ok(val) = std::env::var("CODEX_SUMMARIES_BASE_DIR") {
        let trimmed = val.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }

    let codex_home = match find_codex_home() {
        Ok(p) => p,
        Err(_) => return None,
    };
    let config_path = codex_home.join(CONFIG_TOML_FILE);
    let contents = match std::fs::read_to_string(&config_path) {
        Ok(s) => s,
        Err(_) => return None,
    };

    #[derive(Deserialize)]
    struct PartialConfig {
        summaries: Option<SummariesToml>,
    }

    let cfg: PartialConfig = match toml::from_str(&contents) {
        Ok(v) => v,
        Err(_) => return None,
    };
    cfg.summaries.and_then(|s| s.base_dir)
}

impl ConfigToml {
    /// Derive the effective sandbox policy from the configuration.
    fn derive_sandbox_policy(&self, sandbox_mode_override: Option<SandboxMode>) -> SandboxPolicy {
        let resolved_sandbox_mode = sandbox_mode_override
            .or(self.sandbox_mode)
            .unwrap_or_default();
        match resolved_sandbox_mode {
            SandboxMode::ReadOnly => SandboxPolicy::new_read_only_policy(),
            SandboxMode::WorkspaceWrite => match self.sandbox_workspace_write.as_ref() {
                Some(SandboxWorkspaceWrite {
                    writable_roots,
                    network_access,
                    exclude_tmpdir_env_var,
                    exclude_slash_tmp,
                }) => SandboxPolicy::WorkspaceWrite {
                    writable_roots: writable_roots.clone(),
                    network_access: *network_access,
                    exclude_tmpdir_env_var: *exclude_tmpdir_env_var,
                    exclude_slash_tmp: *exclude_slash_tmp,
                },
                None => SandboxPolicy::new_workspace_write_policy(),
            },
            SandboxMode::DangerFullAccess => SandboxPolicy::DangerFullAccess,
        }
    }

    pub fn is_cwd_trusted(&self, resolved_cwd: &Path) -> bool {
        let projects = self.projects.clone().unwrap_or_default();

        let is_path_trusted = |path: &Path| {
            let path_str = path.to_string_lossy().to_string();
            projects
                .get(&path_str)
                .map(|p| p.trust_level.as_deref() == Some("trusted"))
                .unwrap_or(false)
        };

        // Fast path: exact cwd match
        if is_path_trusted(resolved_cwd) {
            return true;
        }

        // If cwd lives inside a git worktree, check whether the root git project
        // (the primary repository working directory) is trusted. This lets
        // worktrees inherit trust from the main project.
        if let Some(root_project) = resolve_root_git_project_for_trust(resolved_cwd) {
            return is_path_trusted(&root_project);
        }

        false
    }

    pub fn get_config_profile(
        &self,
        override_profile: Option<String>,
    ) -> Result<ConfigProfile, std::io::Error> {
        let profile = override_profile.or_else(|| self.profile.clone());

        match profile {
            Some(key) => {
                if let Some(profile) = self.profiles.get(key.as_str()) {
                    return Ok(profile.clone());
                }

                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("config profile `{key}` not found"),
                ))
            }
            None => Ok(ConfigProfile::default()),
        }
    }
}

/// Optional overrides for user configuration (e.g., from CLI flags).
#[derive(Default, Debug, Clone)]
pub struct ConfigOverrides {
    pub model: Option<String>,
    pub review_model: Option<String>,
    pub cwd: Option<PathBuf>,
    pub approval_policy: Option<AskForApproval>,
    pub sandbox_mode: Option<SandboxMode>,
    pub model_provider: Option<String>,
    pub config_profile: Option<String>,
    pub codex_linux_sandbox_exe: Option<PathBuf>,
    pub base_instructions: Option<String>,
    pub include_plan_tool: Option<bool>,
    pub include_apply_patch_tool: Option<bool>,
    pub include_view_image_tool: Option<bool>,
    pub show_raw_agent_reasoning: Option<bool>,
    pub tools_web_search_request: Option<bool>,
    pub wait_policy: Option<WaitPolicyOverrides>,
    /// High-precedence resolved overrides derived from `-c key=value` CLI flags.
    /// These apply after profile selection to enforce CLI > profile > project > env > defaults.
    pub cli_resolved_overrides: Option<CliResolvedOverrides>,
}

#[derive(Default, Debug, Clone)]
pub struct CliResolvedOverrides {
    pub vm_pty_socket: Option<PathBuf>,
    pub vm_pty_default_vm_id: Option<String>,
    pub vm_pty_timeouts: CliVmPtyTimeouts,
    pub vm_pty_open_blocking: Option<bool>,
    pub debug_tools: Option<bool>,
}

#[derive(Default, Debug, Clone)]
pub struct CliVmPtyTimeouts {
    pub connect_ms: Option<u64>,
    pub request_ms: Option<u64>,
    pub open_ms: Option<u64>,
}

impl Config {
    /// Meant to be used exclusively for tests: `load_with_overrides()` should
    /// be used in all other cases.
    pub fn load_from_base_config_with_overrides(
        cfg: ConfigToml,
        overrides: ConfigOverrides,
        codex_home: PathBuf,
    ) -> std::io::Result<Self> {
        let user_instructions = Self::load_instructions(Some(&codex_home));

        // Destructure ConfigOverrides fully to ensure all overrides are applied.
        let ConfigOverrides {
            model,
            review_model: override_review_model,
            cwd,
            approval_policy,
            sandbox_mode,
            model_provider,
            config_profile: config_profile_key,
            codex_linux_sandbox_exe,
            base_instructions,
            include_plan_tool,
            include_apply_patch_tool,
            include_view_image_tool,
            show_raw_agent_reasoning,
            tools_web_search_request: override_tools_web_search_request,
            wait_policy: wait_policy_override,
            cli_resolved_overrides,
        } = overrides;

        let active_profile_name = config_profile_key
            .as_ref()
            .or(cfg.profile.as_ref())
            .cloned();
        let config_profile = match active_profile_name.as_ref() {
            Some(key) => cfg
                .profiles
                .get(key)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("config profile `{key}` not found"),
                    )
                })?
                .clone(),
            None => ConfigProfile::default(),
        };

        let sandbox_policy = cfg.derive_sandbox_policy(sandbox_mode);

        let mut model_providers = built_in_model_providers();
        // Merge user-defined providers into the built-in list.
        for (key, provider) in cfg.model_providers.into_iter() {
            model_providers.entry(key).or_insert(provider);
        }

        let model_provider_id = model_provider
            .or(config_profile.model_provider)
            .or(cfg.model_provider)
            .unwrap_or_else(|| "openai".to_string());
        let model_provider = model_providers
            .get(&model_provider_id)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Model provider `{model_provider_id}` not found"),
                )
            })?
            .clone();

        let shell_environment_policy = cfg.shell_environment_policy.into();

        let resolved_cwd = {
            use std::env;

            match cwd {
                None => {
                    tracing::info!("cwd not set, using current dir");
                    env::current_dir()?
                }
                Some(p) if p.is_absolute() => p,
                Some(p) => {
                    // Resolve relative path against the current working directory.
                    tracing::info!("cwd is relative, resolving against current dir");
                    let mut current = env::current_dir()?;
                    current.push(p);
                    current
                }
            }
        };

        let history = cfg.history.unwrap_or_default();

        let tools_web_search_request = override_tools_web_search_request
            .or(cfg.tools.as_ref().and_then(|t| t.web_search))
            .unwrap_or(false);

        let include_view_image_tool = include_view_image_tool
            .or(cfg.tools.as_ref().and_then(|t| t.view_image))
            .unwrap_or(true);

        let shell_settings = cfg
            .tools
            .as_ref()
            .and_then(|t| t.shell.clone())
            .unwrap_or_default();
        let shell_enabled = shell_settings.enabled.unwrap_or(true);
        let normalize_profiles = |profiles: Vec<String>| {
            profiles
                .into_iter()
                .map(|entry| entry.trim().to_ascii_lowercase())
                .filter(|entry| !entry.is_empty())
                .collect::<Vec<_>>()
        };
        let shell_allow = normalize_profiles(shell_settings.allow_profiles);
        let shell_deny = normalize_profiles(shell_settings.disallow_profiles);
        let include_shell_tool = if !shell_enabled {
            false
        } else if let Some(profile) = active_profile_name.as_ref() {
            let profile = profile.trim().to_ascii_lowercase();
            let allowed = if shell_allow.is_empty() {
                true
            } else {
                shell_allow
                    .iter()
                    .any(|entry| entry == "*" || entry == &profile)
            };
            let denied = shell_deny
                .iter()
                .any(|entry| entry == "*" || entry == &profile);
            allowed && !denied
        } else {
            let allowed = shell_allow.is_empty()
                || shell_allow.iter().any(|entry| entry == "*");
            let denied = shell_deny.iter().any(|entry| entry == "*");
            allowed && !denied
        };

        // Resolve vm-pty settings with precedence: profile > project config > env > defaults.
        let legacy_vm_pty_enabled = cfg.experimental_use_unified_exec_tool.unwrap_or(false);
        // Prefer nested tools.vm_pty if provided; fall back to top-level vm_pty for backward-compat.
        let base_vm_pty_cfg = if let Some(t) = &cfg.tools {
            let mut nested = t.vm_pty.clone();
            if nested.enabled.is_none() && nested.allow_profiles.is_empty() {
                cfg.vm_pty.clone()
            } else {
                nested
            }
        } else {
            cfg.vm_pty.clone()
        };
        let profile_vm_pty_cfg = config_profile
            .tools
            .as_ref()
            .map(|t| t.vm_pty.clone())
            .unwrap_or_default();
        let vm_pty_enabled = profile_vm_pty_cfg
            .enabled
            .or(base_vm_pty_cfg.enabled)
            .unwrap_or(legacy_vm_pty_enabled);
        let mut vm_pty_allowed_profiles: Vec<String> = {
            let from_profile = if !profile_vm_pty_cfg.allow_profiles.is_empty() {
                profile_vm_pty_cfg.allow_profiles.clone()
            } else {
                base_vm_pty_cfg.allow_profiles.clone()
            };
            from_profile
                .into_iter()
                .map(|profile| profile.trim().to_ascii_lowercase())
                .filter(|profile| !profile.is_empty())
                .collect()
        };
        vm_pty_allowed_profiles.sort_unstable();
        vm_pty_allowed_profiles.dedup();

        // Socket resolution: CLI > profile > project config > env.
        let cli_vm_pty_socket = cli_resolved_overrides
            .as_ref()
            .and_then(|o| o.vm_pty_socket.clone());
        let resolved_vm_pty_socket: Option<PathBuf> = cli_vm_pty_socket
            .or(profile_vm_pty_cfg.socket)
            .or(base_vm_pty_cfg.socket.clone())
            .or_else(|| {
                if let Ok(v) = std::env::var("CODEX_VM_PTY_SOCKET") {
                    let trimmed = v.trim();
                    if !trimmed.is_empty() {
                        warn!(target: "codex::config", "Using CODEX_VM_PTY_SOCKET from environment; prefer -c tools.vm_pty.socket or config.toml");
                        return Some(PathBuf::from(trimmed));
                    }
                }
                None
            });

        let vm_pty_flag_enabled = if vm_pty_enabled && resolved_vm_pty_socket.is_none() {
            warn!(
                target: "codex::config",
                "vm-pty lane enabled but tools.vm_pty.socket is not configured; disabling pty_open tool"
            );
            false
        } else {
            vm_pty_enabled
        };
        let vm_pty_profile_allowlisted = if vm_pty_flag_enabled {
            match active_profile_name.as_ref() {
                Some(profile) => {
                    let normalized = profile.to_ascii_lowercase();
                    vm_pty_allowed_profiles
                        .iter()
                        .any(|allowed| allowed == "*" || allowed == &normalized)
                }
                None => false,
            }
        } else {
            false
        };
        let include_vm_pty_tool = vm_pty_flag_enabled && vm_pty_profile_allowlisted;
        // Expose pty_open for any profile allow‑listed via tools.vm_pty.allow_profiles.
        // This removes hard‑coded profile names in favor of config.
        let include_vm_pty_open_tool = include_vm_pty_tool;
        let exec_explicit_enabled = cfg
            .tools
            .as_ref()
            .and_then(|t| t.unified_exec_enabled)
            .unwrap_or(false);
        let use_experimental_unified_exec_tool = include_vm_pty_tool && exec_explicit_enabled;

        let model = model
            .or(config_profile.model)
            .or(cfg.model)
            .unwrap_or_else(default_model);

        let mut wait_settings = WaitPolicySettings::from_toml(cfg.wait.as_ref())
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;

        if let Some(profile_name) = active_profile_name.as_ref() {
            if let Some(profile_wait) = cfg.wait_profiles.get(profile_name) {
                wait_settings = wait_settings
                    .apply_toml(profile_wait)
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
                wait_settings.normalize();
            }
        }

        if let Some(overrides) = wait_policy_override.as_ref() {
            wait_settings
                .apply_override(overrides)
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        }

        // Resolve vm-pty timeouts with precedence: CLI > profile > project > env > default.
        let cli_timeouts = cli_resolved_overrides.as_ref().map(|o| &o.vm_pty_timeouts);
        let resolved_connect_timeout = cli_timeouts
            .and_then(|t| t.connect_ms.map(Duration::from_millis))
            .or(profile_vm_pty_cfg
                .timeouts
                .connect_ms
                .map(Duration::from_millis))
            .or(base_vm_pty_cfg
                .timeouts
                .connect_ms
                .map(Duration::from_millis))
            .or_else(|| {
                std::env::var("CODEX_VM_PTY_CONNECT_TIMEOUT_MS")
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(Duration::from_millis)
            })
            .unwrap_or(DEFAULT_VM_PTY_CONNECT_TIMEOUT);

        let resolved_request_timeout = cli_timeouts
            .and_then(|t| t.request_ms.map(Duration::from_millis))
            .or(profile_vm_pty_cfg
                .timeouts
                .request_ms
                .map(Duration::from_millis))
            .or(base_vm_pty_cfg
                .timeouts
                .request_ms
                .map(Duration::from_millis))
            .or_else(|| {
                std::env::var("CODEX_VM_PTY_REQUEST_TIMEOUT_MS")
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(Duration::from_millis)
            })
            .unwrap_or(DEFAULT_VM_PTY_REQUEST_TIMEOUT);

        let resolved_open_timeout = cli_timeouts
            .and_then(|t| t.open_ms.map(Duration::from_millis))
            .or(profile_vm_pty_cfg
                .timeouts
                .open_ms
                .map(Duration::from_millis))
            .or(base_vm_pty_cfg
                .timeouts
                .open_ms
                .map(Duration::from_millis))
            .or_else(|| {
                std::env::var("CODEX_VM_PTY_OPEN_TIMEOUT_MS")
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(Duration::from_millis)
            })
            .unwrap_or(DEFAULT_VM_PTY_OPEN_TIMEOUT);

        let resolved_open_blocking = cli_resolved_overrides
            .as_ref()
            .and_then(|o| o.vm_pty_open_blocking)
            .or(profile_vm_pty_cfg.open_blocking)
            .or(base_vm_pty_cfg.open_blocking)
            .or_else(|| {
                let v = std::env::var("CODEX_VM_PTY_OPEN_BLOCKING").unwrap_or_default();
                let b = matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes");
                Some(b)
            })
            .unwrap_or(false);

        // Resolve default vm id for pty_open/attach.
        let resolved_default_vm_id = cli_resolved_overrides
            .as_ref()
            .and_then(|o| o.vm_pty_default_vm_id.clone())
            .or(profile_vm_pty_cfg.default_vm_id)
            .or(base_vm_pty_cfg.default_vm_id)
            .or_else(|| std::env::var("CODEX_VM_PTY_VM_ID").ok());

        // Debug tools gate precedence: CLI > profile > project > env > default(false)
        let debug_tools = cli_resolved_overrides
            .as_ref()
            .and_then(|o| o.debug_tools)
            .or(config_profile
                .debug
                .as_ref()
                .and_then(|d| d.tools))
            .or(cfg.debug.as_ref().and_then(|d| d.tools))
            .or_else(|| (std::env::var("CODEX_DEBUG_TOOLS").as_deref() == Ok("1")).then_some(true))
            .unwrap_or(false);

        let mut model_family =
            find_family_for_model(&model).unwrap_or_else(|| derive_default_model_family(&model));

        if let Some(supports_reasoning_summaries) = cfg.model_supports_reasoning_summaries {
            model_family.supports_reasoning_summaries = supports_reasoning_summaries;
        }
        if let Some(model_reasoning_summary_format) = cfg.model_reasoning_summary_format {
            model_family.reasoning_summary_format = model_reasoning_summary_format;
        }

        let openai_model_info = get_model_info(&model_family);
        let model_context_window = cfg
            .model_context_window
            .or_else(|| openai_model_info.as_ref().map(|info| info.context_window));
        let model_max_output_tokens = cfg.model_max_output_tokens.or_else(|| {
            openai_model_info
                .as_ref()
                .map(|info| info.max_output_tokens)
        });
        let model_auto_compact_token_limit = cfg.model_auto_compact_token_limit.or_else(|| {
            openai_model_info
                .as_ref()
                .and_then(|info| info.auto_compact_token_limit)
        });

        // Load base instructions override from a file if specified. If the
        // path is relative, resolve it against the effective cwd so the
        // behaviour matches other path-like config values.
        let experimental_instructions_path = config_profile
            .experimental_instructions_file
            .as_ref()
            .or(cfg.experimental_instructions_file.as_ref());
        let file_base_instructions =
            Self::get_base_instructions(experimental_instructions_path, &resolved_cwd)?;
        let base_instructions = base_instructions.or(file_base_instructions);

        // Default review model when not set in config; allow CLI override to take precedence.
        let review_model = override_review_model
            .or(cfg.review_model)
            .unwrap_or_else(default_review_model);

        let mailbox_liveness = MailboxLivenessSettings::from_toml(cfg.mailbox_liveness.clone());

        let config = Self {
            model,
            review_model,
            model_family,
            model_context_window,
            model_max_output_tokens,
            model_auto_compact_token_limit,
            model_provider_id,
            model_provider,
            cwd: resolved_cwd,
            approval_policy: approval_policy
                .or(config_profile.approval_policy)
                .or(cfg.approval_policy)
                .unwrap_or_else(AskForApproval::default),
            sandbox_policy,
            shell_environment_policy,
            notify: cfg.notify,
            user_instructions,
            base_instructions,
            mcp_servers: cfg.mcp_servers,
            // The config.toml omits "_mode" because it's a config file. However, "_mode"
            // is important in code to differentiate the mode from the store implementation.
            mcp_oauth_credentials_store_mode: cfg.mcp_oauth_credentials_store.unwrap_or_default(),
            model_providers,
            project_doc_max_bytes: cfg.project_doc_max_bytes.unwrap_or(PROJECT_DOC_MAX_BYTES),
            project_doc_fallback_filenames: cfg
                .project_doc_fallback_filenames
                .unwrap_or_default()
                .into_iter()
                .filter_map(|name| {
                    let trimmed = name.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .collect(),
            codex_home,
            history,
            file_opener: cfg.file_opener.unwrap_or(UriBasedFileOpener::VsCode),
            codex_linux_sandbox_exe,

            hide_agent_reasoning: cfg.hide_agent_reasoning.unwrap_or(false),
            show_raw_agent_reasoning: cfg
                .show_raw_agent_reasoning
                .or(show_raw_agent_reasoning)
                .unwrap_or(false),
            model_reasoning_effort: config_profile
                .model_reasoning_effort
                .or(cfg.model_reasoning_effort),
            model_reasoning_summary: config_profile
                .model_reasoning_summary
                .or(cfg.model_reasoning_summary)
                .unwrap_or_default(),
            model_verbosity: config_profile.model_verbosity.or(cfg.model_verbosity),
            chatgpt_base_url: config_profile
                .chatgpt_base_url
                .or(cfg.chatgpt_base_url)
                .unwrap_or("https://chatgpt.com/backend-api/".to_string()),
            include_plan_tool: include_plan_tool.unwrap_or(false),
            include_apply_patch_tool: include_apply_patch_tool
                .or(cfg.experimental_use_freeform_apply_patch)
                .unwrap_or(false),
            tools_web_search_request,
            use_experimental_streamable_shell_tool: cfg
                .experimental_use_exec_command_tool
                .unwrap_or(false),
            use_experimental_unified_exec_tool,
            use_experimental_use_rmcp_client: cfg.experimental_use_rmcp_client.unwrap_or(false),
            include_view_image_tool,
            include_shell_tool,
            include_vm_pty_tool,
            include_vm_pty_open_tool,
            debug_tools,
            vm_pty_socket: resolved_vm_pty_socket,
            vm_pty_default_vm_id: resolved_default_vm_id,
            vm_pty_connect_timeout: resolved_connect_timeout,
            vm_pty_request_timeout: resolved_request_timeout,
            vm_pty_open_timeout: resolved_open_timeout,
            vm_pty_open_blocking: resolved_open_blocking,
            active_profile: active_profile_name,
            windows_wsl_setup_acknowledged: cfg.windows_wsl_setup_acknowledged.unwrap_or(false),
            disable_paste_burst: cfg.disable_paste_burst.unwrap_or(false),
            tui_notifications: cfg
                .tui
                .as_ref()
                .map(|t| t.notifications.clone())
                .unwrap_or_default(),
            otel: {
                let t: OtelConfigToml = cfg.otel.unwrap_or_default();
                let log_user_prompt = t.log_user_prompt.unwrap_or(false);
                let environment = t
                    .environment
                    .unwrap_or(DEFAULT_OTEL_ENVIRONMENT.to_string());
                let exporter = t.exporter.unwrap_or(OtelExporterKind::None);
                OtelConfig {
                    log_user_prompt,
                    environment,
                    exporter,
                }
            },
            summaries: SummariesSettings::from_toml(cfg.summaries.clone()),
            mailbox_liveness,
            wait: wait_settings.clone(),
        };
        update_wait_policy(&config.wait);
        Ok(config)
    }

    fn load_instructions(codex_dir: Option<&Path>) -> Option<String> {
        let mut p = match codex_dir {
            Some(p) => p.to_path_buf(),
            None => return None,
        };

        p.push("AGENTS.md");
        std::fs::read_to_string(&p).ok().and_then(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
    }

    fn get_base_instructions(
        path: Option<&PathBuf>,
        cwd: &Path,
    ) -> std::io::Result<Option<String>> {
        let p = match path.as_ref() {
            None => return Ok(None),
            Some(p) => p,
        };

        // Resolve relative paths against the provided cwd to make CLI
        // overrides consistent regardless of where the process was launched
        // from.
        let full_path = if p.is_relative() {
            cwd.join(p)
        } else {
            p.to_path_buf()
        };

        let contents = std::fs::read_to_string(&full_path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "failed to read experimental instructions file {}: {e}",
                    full_path.display()
                ),
            )
        })?;

        let s = contents.trim().to_string();
        if s.is_empty() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "experimental instructions file is empty: {}",
                    full_path.display()
                ),
            ))
        } else {
            Ok(Some(s))
        }
    }
}

fn default_model() -> String {
    OPENAI_DEFAULT_MODEL.to_string()
}

fn default_review_model() -> String {
    OPENAI_DEFAULT_REVIEW_MODEL.to_string()
}

/// Returns the path to the Codex configuration directory, which can be
/// specified by the `CODEX_HOME` environment variable. If not set, defaults to
/// `~/.codex`.
///
/// This helper ensures the directory exists before returning. When
/// `CODEX_HOME` is provided, the directory will be created on-demand (to
/// restore the historical behaviour where new installs worked with no manual
/// setup) and the resulting path is canonicalized when possible.
pub fn find_codex_home() -> std::io::Result<PathBuf> {
    // Honor the `CODEX_HOME` environment variable when it is set to allow users
    // (and tests) to override the default location.
    if let Ok(val) = std::env::var("CODEX_HOME")
        && !val.is_empty()
    {
        let path = PathBuf::from(val);
        std::fs::create_dir_all(&path)?;
        return path.canonicalize().or_else(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Ok(path)
            } else {
                Err(err)
            }
        });
    }

    let mut p = home_dir().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not find home directory",
        )
    })?;
    p.push(".codex");
    std::fs::create_dir_all(&p)?;
    Ok(p)
}

/// Returns the path to the folder where Codex logs are stored. Does not verify
/// that the directory exists.
pub fn log_dir(cfg: &Config) -> std::io::Result<PathBuf> {
    let mut p = cfg.codex_home.clone();
    p.push("log");
    Ok(p)
}

#[cfg(test)]
mod tests {
    use crate::config_types::HistoryPersistence;
    use crate::config_types::Notifications;

    use super::*;
    use pretty_assertions::assert_eq;

    use serial_test::serial;
    use std::ffi::OsString;
    use std::time::Duration;
    use tempfile::TempDir;

    struct EnvVarGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set_os(key: &'static str, value: &std::ffi::OsStr) -> Self {
            let original = std::env::var_os(key);
            // SAFETY: tests serialize access to these env vars via the
            // serial_test attribute, so we can mutate process-global state.
            unsafe { std::env::set_var(key, value) };
            Self { key, original }
        }

        fn unset(key: &'static str) -> Self {
            let original = std::env::var_os(key);
            // SAFETY: see comment in set_os.
            unsafe { std::env::remove_var(key) };
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    #[test]
    #[serial]
    fn find_codex_home_creates_default_directory() {
        let temp_home = TempDir::new().expect("tempdir");
        let _home_guard = EnvVarGuard::set_os("HOME", temp_home.path().as_os_str());
        let _codex_guard = EnvVarGuard::unset("CODEX_HOME");

        let codex_home = find_codex_home().expect("find codex home");
        assert_eq!(codex_home, temp_home.path().join(".codex"));
        assert!(codex_home.is_dir());
    }

    #[test]
    #[serial]
    fn find_codex_home_creates_env_override_directory() {
        let temp_home = TempDir::new().expect("tempdir");
        let override_dir = temp_home.path().join("custom-codex-home");
        let _home_guard = EnvVarGuard::set_os("HOME", temp_home.path().as_os_str());
        let _codex_guard = EnvVarGuard::set_os("CODEX_HOME", override_dir.as_os_str());

        let codex_home = find_codex_home().expect("find codex home");
        assert_eq!(
            codex_home,
            override_dir
                .canonicalize()
                .expect("canonical override exists"),
        );
        assert!(codex_home.is_dir());
    }

    #[test]
    fn test_toml_parsing() {
        let history_with_persistence = r#"
[history]
persistence = "save-all"
"#;
        let history_with_persistence_cfg = toml::from_str::<ConfigToml>(history_with_persistence)
            .expect("TOML deserialization should succeed");
        assert_eq!(
            Some(History {
                persistence: HistoryPersistence::SaveAll,
                max_bytes: None,
            }),
            history_with_persistence_cfg.history
        );

        let history_no_persistence = r#"
[history]
persistence = "none"
"#;

        let history_no_persistence_cfg = toml::from_str::<ConfigToml>(history_no_persistence)
            .expect("TOML deserialization should succeed");
        assert_eq!(
            Some(History {
                persistence: HistoryPersistence::None,
                max_bytes: None,
            }),
            history_no_persistence_cfg.history
        );
    }

    #[test]
    fn tui_config_missing_notifications_field_defaults_to_disabled() {
        let cfg = r#"
[tui]
"#;

        let parsed = toml::from_str::<ConfigToml>(cfg)
            .expect("TUI config without notifications should succeed");
        let tui = parsed.tui.expect("config should include tui section");

        assert_eq!(tui.notifications, Notifications::Enabled(false));
    }

    #[test]
    fn wait_policy_defaults_apply() {
        let settings = WaitPolicySettings::from_toml(None).expect("defaults");
        assert!(settings.is_allowed(WaitPredicateKind::Timer));
        assert!(settings.is_allowed(WaitPredicateKind::Filesystem));
        assert!(settings.is_allowed(WaitPredicateKind::Shell));
        assert_eq!(settings.max_waits_per_turn(), DEFAULT_MAX_WAITS_PER_TURN);
        assert_eq!(settings.max_duration(), DEFAULT_WAIT_MAX_DURATION);
        assert!(!settings.require_shell_approval());
    }

    #[test]
    fn wait_policy_allows_disabling_all_predicates_via_config() {
        let settings = WaitPolicySettings::from_toml(Some(&WaitPolicyToml {
            allowed_predicates: Some(vec![]),
            ..Default::default()
        }))
        .expect("empty allow-list should parse");

        assert!(!settings.is_allowed(WaitPredicateKind::Timer));
        assert!(!settings.is_allowed(WaitPredicateKind::Filesystem));
        assert!(!settings.is_allowed(WaitPredicateKind::Shell));
    }

    #[test]
    fn wait_policy_rejects_invalid_predicates() {
        let result = WaitPolicySettings::from_toml(Some(&WaitPolicyToml {
            allowed_predicates: Some(vec!["unknown".to_string()]),
            ..Default::default()
        }));
        assert!(result.is_err());
    }

    #[test]
    fn wait_policy_profile_override_applies() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut cfg = ConfigToml::default();
        cfg.profiles
            .insert("dev".to_string(), ConfigProfile::default());
        cfg.profile = Some("dev".to_string());
        cfg.wait = Some(WaitPolicyToml {
            allowed_predicates: Some(vec!["timer".to_string()]),
            ..Default::default()
        });
        cfg.wait_profiles.insert(
            "dev".to_string(),
            WaitPolicyToml {
                allowed_predicates: Some(vec!["shell".to_string()]),
                max_waits_per_turn: Some(2),
                require_shell_approval: Some(true),
                ..Default::default()
            },
        );

        let overrides = ConfigOverrides {
            config_profile: Some("dev".to_string()),
            ..Default::default()
        };

        let config =
            Config::load_from_base_config_with_overrides(cfg, overrides, temp.path().into())
                .expect("config");

        assert!(config.wait.is_allowed(WaitPredicateKind::Shell));
        assert!(!config.wait.is_allowed(WaitPredicateKind::Timer));
        assert_eq!(config.wait.max_waits_per_turn(), 2);
        assert!(config.wait.require_shell_approval());
    }

    #[test]
    fn wait_policy_cli_override_wins() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut cfg = ConfigToml::default();
        cfg.wait = Some(WaitPolicyToml {
            max_duration_seconds: Some(120),
            ..Default::default()
        });

        let overrides = ConfigOverrides {
            wait_policy: Some(WaitPolicyOverrides {
                max_duration_seconds: Some(10),
                allowed_predicates: Some(vec![WaitPredicateKind::Filesystem]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let config =
            Config::load_from_base_config_with_overrides(cfg, overrides, temp.path().into())
                .expect("config");

        assert_eq!(config.wait.max_duration(), Duration::from_secs(10));
        assert!(config.wait.is_allowed(WaitPredicateKind::Filesystem));
        assert!(!config.wait.is_allowed(WaitPredicateKind::Timer));
    }

    #[test]
    fn wait_policy_cli_override_can_disable_all_predicates() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cfg = ConfigToml::default();

        let overrides = ConfigOverrides {
            wait_policy: Some(WaitPolicyOverrides {
                allowed_predicates: Some(Vec::<WaitPredicateKind>::new()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let config =
            Config::load_from_base_config_with_overrides(cfg, overrides, temp.path().into())
                .expect("config");

        assert!(!config.wait.is_allowed(WaitPredicateKind::Timer));
        assert!(!config.wait.is_allowed(WaitPredicateKind::Filesystem));
        assert!(!config.wait.is_allowed(WaitPredicateKind::Shell));
    }

    #[test]
    fn vm_pty_disabled_by_default() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let cwd = tempfile::tempdir().expect("cwd");
        let mut overrides = ConfigOverrides::default();
        overrides.cwd = Some(cwd.path().to_path_buf());

        let config = Config::load_from_base_config_with_overrides(
            ConfigToml::default(),
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");

        assert!(
            !config.use_experimental_unified_exec_tool,
            "unified_exec should be disabled unless the VM PTY feature flag and allowlist are set"
        );
        assert!(
            !config.include_vm_pty_tool,
            "vm-pty tool must be disabled unless the feature flag, allowlist, and socket are configured"
        );
    }

    #[test]
    fn vm_pty_requires_allowlisted_profile() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let cwd = tempfile::tempdir().expect("cwd");
        let socket_dir = tempfile::tempdir().expect("socket_dir");
        let socket_path = socket_dir.path().join("pty.sock");
        let mut cfg = ConfigToml::default();
        cfg.vm_pty.enabled = Some(true);
        cfg.vm_pty.allow_profiles = vec!["management".to_string()];
        cfg.tools = Some(ToolsToml {
            vm_pty: VmPtyToml { socket: Some(socket_path.clone()), ..Default::default() },
            ..Default::default()
        });
        cfg.profiles
            .insert("workers".to_string(), ConfigProfile::default());
        cfg.profiles
            .insert("management".to_string(), ConfigProfile::default());

        let mut overrides = ConfigOverrides::default();
        overrides.cwd = Some(cwd.path().to_path_buf());
        overrides.config_profile = Some("workers".to_string());

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");

        assert!(
            !config.use_experimental_unified_exec_tool,
            "non-allowlisted profiles must not receive the unified_exec tool"
        );
        assert!(
            !config.include_vm_pty_tool,
            "non-allowlisted profiles must not receive the vm-pty tool"
        );
    }

    #[test]
    fn vm_pty_enabled_for_management_profile() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let cwd = tempfile::tempdir().expect("cwd");
        let socket_dir = tempfile::tempdir().expect("socket_dir");
        let socket_path = socket_dir.path().join("pty.sock");
        let mut cfg = ConfigToml::default();
        cfg.vm_pty.enabled = Some(true);
        cfg.vm_pty.allow_profiles = vec!["management".to_string()];
        cfg.tools = Some(ToolsToml {
            vm_pty: VmPtyToml { socket: Some(socket_path.clone()), ..Default::default() },
            ..Default::default()
        });
        cfg.profiles
            .insert("management".to_string(), ConfigProfile::default());

        let mut overrides = ConfigOverrides::default();
        overrides.cwd = Some(cwd.path().to_path_buf());
        overrides.config_profile = Some("management".to_string());

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");

        assert!(
            config.use_experimental_unified_exec_tool,
            "allowlisted Management profile should receive the unified_exec tool"
        );
        assert!(
            config.include_vm_pty_tool,
            "allowlisted Management profile should receive the vm-pty tool"
        );
    }

    #[test]
    fn test_sandbox_config_parsing() {
        let sandbox_full_access = r#"
sandbox_mode = "danger-full-access"

[sandbox_workspace_write]
network_access = false  # This should be ignored.
"#;
        let sandbox_full_access_cfg = toml::from_str::<ConfigToml>(sandbox_full_access)
            .expect("TOML deserialization should succeed");
        let sandbox_mode_override = None;
        assert_eq!(
            SandboxPolicy::DangerFullAccess,
            sandbox_full_access_cfg.derive_sandbox_policy(sandbox_mode_override)
        );

        let sandbox_read_only = r#"
sandbox_mode = "read-only"

[sandbox_workspace_write]
network_access = true  # This should be ignored.
"#;

        let sandbox_read_only_cfg = toml::from_str::<ConfigToml>(sandbox_read_only)
            .expect("TOML deserialization should succeed");
        let sandbox_mode_override = None;
        assert_eq!(
            SandboxPolicy::ReadOnly,
            sandbox_read_only_cfg.derive_sandbox_policy(sandbox_mode_override)
        );

        let sandbox_workspace_write = r#"
sandbox_mode = "workspace-write"

[sandbox_workspace_write]
writable_roots = [
    "/my/workspace",
]
exclude_tmpdir_env_var = true
exclude_slash_tmp = true
"#;

        let sandbox_workspace_write_cfg = toml::from_str::<ConfigToml>(sandbox_workspace_write)
            .expect("TOML deserialization should succeed");
        let sandbox_mode_override = None;
        assert_eq!(
            SandboxPolicy::WorkspaceWrite {
                writable_roots: vec![PathBuf::from("/my/workspace")],
                network_access: false,
                exclude_tmpdir_env_var: true,
                exclude_slash_tmp: true,
            },
            sandbox_workspace_write_cfg.derive_sandbox_policy(sandbox_mode_override)
        );
    }

    #[test]
    fn config_defaults_to_auto_oauth_store_mode() -> std::io::Result<()> {
        let codex_home = TempDir::new()?;
        let cfg = ConfigToml::default();

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            ConfigOverrides::default(),
            codex_home.path().to_path_buf(),
        )?;

        assert_eq!(
            config.mcp_oauth_credentials_store_mode,
            OAuthCredentialsStoreMode::Auto,
        );

        Ok(())
    }

    #[test]
    fn config_honors_explicit_file_oauth_store_mode() -> std::io::Result<()> {
        let codex_home = TempDir::new()?;
        let cfg = ConfigToml {
            mcp_oauth_credentials_store: Some(OAuthCredentialsStoreMode::File),
            ..Default::default()
        };

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            ConfigOverrides::default(),
            codex_home.path().to_path_buf(),
        )?;

        assert_eq!(
            config.mcp_oauth_credentials_store_mode,
            OAuthCredentialsStoreMode::File,
        );

        Ok(())
    }

    #[tokio::test]
    async fn managed_config_overrides_oauth_store_mode() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let managed_path = codex_home.path().join("managed_config.toml");
        let config_path = codex_home.path().join(CONFIG_TOML_FILE);

        std::fs::write(&config_path, "mcp_oauth_credentials_store = \"file\"\n")?;
        std::fs::write(&managed_path, "mcp_oauth_credentials_store = \"keyring\"\n")?;

        let overrides = crate::config_loader::LoaderOverrides {
            managed_config_path: Some(managed_path.clone()),
            #[cfg(target_os = "macos")]
            managed_preferences_base64: None,
        };

        let root_value = load_resolved_config(codex_home.path(), Vec::new(), overrides).await?;
        let cfg: ConfigToml = root_value.try_into().map_err(|e| {
            tracing::error!("Failed to deserialize overridden config: {e}");
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        assert_eq!(
            cfg.mcp_oauth_credentials_store,
            Some(OAuthCredentialsStoreMode::Keyring),
        );

        let final_config = Config::load_from_base_config_with_overrides(
            cfg,
            ConfigOverrides::default(),
            codex_home.path().to_path_buf(),
        )?;
        assert_eq!(
            final_config.mcp_oauth_credentials_store_mode,
            OAuthCredentialsStoreMode::Keyring,
        );

        Ok(())
    }

    #[tokio::test]
    async fn load_global_mcp_servers_returns_empty_if_missing() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        let servers = load_global_mcp_servers(codex_home.path()).await?;
        assert!(servers.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn write_global_mcp_servers_round_trips_entries() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        let mut servers = BTreeMap::new();
        servers.insert(
            "docs".to_string(),
            McpServerConfig {
                transport: McpServerTransportConfig::Stdio {
                    command: "echo".to_string(),
                    args: vec!["hello".to_string()],
                    env: None,
                },
                enabled: true,
                startup_timeout_sec: Some(Duration::from_secs(3)),
                tool_timeout_sec: Some(Duration::from_secs(5)),
                keepalive_interval_sec: None,
            },
        );

        write_global_mcp_servers(codex_home.path(), &servers)?;

        let loaded = load_global_mcp_servers(codex_home.path()).await?;
        assert_eq!(loaded.len(), 1);
        let docs = loaded.get("docs").expect("docs entry");
        match &docs.transport {
            McpServerTransportConfig::Stdio { command, args, env } => {
                assert_eq!(command, "echo");
                assert_eq!(args, &vec!["hello".to_string()]);
                assert!(env.is_none());
            }
            other => panic!("unexpected transport {other:?}"),
        }
        assert_eq!(docs.startup_timeout_sec, Some(Duration::from_secs(3)));
        assert_eq!(docs.tool_timeout_sec, Some(Duration::from_secs(5)));
        assert!(docs.enabled);

        let empty = BTreeMap::new();
        write_global_mcp_servers(codex_home.path(), &empty)?;
        let loaded = load_global_mcp_servers(codex_home.path()).await?;
        assert!(loaded.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn managed_config_wins_over_cli_overrides() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let managed_path = codex_home.path().join("managed_config.toml");

        std::fs::write(
            codex_home.path().join(CONFIG_TOML_FILE),
            "model = \"base\"\n",
        )?;
        std::fs::write(&managed_path, "model = \"managed_config\"\n")?;

        let overrides = crate::config_loader::LoaderOverrides {
            managed_config_path: Some(managed_path),
            #[cfg(target_os = "macos")]
            managed_preferences_base64: None,
        };

        let root_value = load_resolved_config(
            codex_home.path(),
            vec![("model".to_string(), TomlValue::String("cli".to_string()))],
            overrides,
        )
        .await?;

        let cfg: ConfigToml = root_value.try_into().map_err(|e| {
            tracing::error!("Failed to deserialize overridden config: {e}");
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        assert_eq!(cfg.model.as_deref(), Some("managed_config"));
        Ok(())
    }

    #[tokio::test]
    async fn load_global_mcp_servers_accepts_legacy_ms_field() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let config_path = codex_home.path().join(CONFIG_TOML_FILE);

        std::fs::write(
            &config_path,
            r#"
[mcp_servers]
[mcp_servers.docs]
command = "echo"
startup_timeout_ms = 2500
"#,
        )?;

        let servers = load_global_mcp_servers(codex_home.path()).await?;
        let docs = servers.get("docs").expect("docs entry");
        assert_eq!(docs.startup_timeout_sec, Some(Duration::from_millis(2500)));

        Ok(())
    }

    #[tokio::test]
    async fn load_global_mcp_servers_rejects_inline_bearer_token() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let config_path = codex_home.path().join(CONFIG_TOML_FILE);

        std::fs::write(
            &config_path,
            r#"
[mcp_servers.docs]
url = "https://example.com/mcp"
bearer_token = "secret"
"#,
        )?;

        let err = load_global_mcp_servers(codex_home.path())
            .await
            .expect_err("bearer_token entries should be rejected");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("bearer_token"));
        assert!(err.to_string().contains("bearer_token_env_var"));

        Ok(())
    }

    #[tokio::test]
    async fn write_global_mcp_servers_serializes_env_sorted() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        let servers = BTreeMap::from([(
            "docs".to_string(),
            McpServerConfig {
                transport: McpServerTransportConfig::Stdio {
                    command: "docs-server".to_string(),
                    args: vec!["--verbose".to_string()],
                    env: Some(HashMap::from([
                        ("ZIG_VAR".to_string(), "3".to_string()),
                        ("ALPHA_VAR".to_string(), "1".to_string()),
                    ])),
                },
                enabled: true,
                startup_timeout_sec: None,
                tool_timeout_sec: None,
                keepalive_interval_sec: None,
            },
        )]);

        write_global_mcp_servers(codex_home.path(), &servers)?;

        let config_path = codex_home.path().join(CONFIG_TOML_FILE);
        let serialized = std::fs::read_to_string(&config_path)?;
        assert_eq!(
            serialized,
            r#"[mcp_servers.docs]
command = "docs-server"
args = ["--verbose"]

[mcp_servers.docs.env]
ALPHA_VAR = "1"
ZIG_VAR = "3"
"#
        );

        let loaded = load_global_mcp_servers(codex_home.path()).await?;
        let docs = loaded.get("docs").expect("docs entry");
        match &docs.transport {
            McpServerTransportConfig::Stdio { command, args, env } => {
                assert_eq!(command, "docs-server");
                assert_eq!(args, &vec!["--verbose".to_string()]);
                let env = env
                    .as_ref()
                    .expect("env should be preserved for stdio transport");
                assert_eq!(env.get("ALPHA_VAR"), Some(&"1".to_string()));
                assert_eq!(env.get("ZIG_VAR"), Some(&"3".to_string()));
            }
            other => panic!("unexpected transport {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn write_global_mcp_servers_serializes_streamable_http() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        let mut servers = BTreeMap::from([(
            "docs".to_string(),
            McpServerConfig {
                transport: McpServerTransportConfig::StreamableHttp {
                    url: "https://example.com/mcp".to_string(),
                    bearer_token_env_var: Some("MCP_TOKEN".to_string()),
                },
                enabled: true,
                startup_timeout_sec: Some(Duration::from_secs(2)),
                tool_timeout_sec: None,
                keepalive_interval_sec: None,
            },
        )]);

        write_global_mcp_servers(codex_home.path(), &servers)?;

        let config_path = codex_home.path().join(CONFIG_TOML_FILE);
        let serialized = std::fs::read_to_string(&config_path)?;
        assert_eq!(
            serialized,
            r#"[mcp_servers.docs]
url = "https://example.com/mcp"
bearer_token_env_var = "MCP_TOKEN"
startup_timeout_sec = 2.0
"#
        );

        let loaded = load_global_mcp_servers(codex_home.path()).await?;
        let docs = loaded.get("docs").expect("docs entry");
        match &docs.transport {
            McpServerTransportConfig::StreamableHttp {
                url,
                bearer_token_env_var,
            } => {
                assert_eq!(url, "https://example.com/mcp");
                assert_eq!(bearer_token_env_var.as_deref(), Some("MCP_TOKEN"));
            }
            other => panic!("unexpected transport {other:?}"),
        }
        assert_eq!(docs.startup_timeout_sec, Some(Duration::from_secs(2)));

        servers.insert(
            "docs".to_string(),
            McpServerConfig {
                transport: McpServerTransportConfig::StreamableHttp {
                    url: "https://example.com/mcp".to_string(),
                    bearer_token_env_var: None,
                },
                enabled: true,
                startup_timeout_sec: None,
                tool_timeout_sec: None,
                keepalive_interval_sec: None,
            },
        );
        write_global_mcp_servers(codex_home.path(), &servers)?;

        let serialized = std::fs::read_to_string(&config_path)?;
        assert_eq!(
            serialized,
            r#"[mcp_servers.docs]
url = "https://example.com/mcp"
"#
        );

        let loaded = load_global_mcp_servers(codex_home.path()).await?;
        let docs = loaded.get("docs").expect("docs entry");
        match &docs.transport {
            McpServerTransportConfig::StreamableHttp {
                url,
                bearer_token_env_var,
            } => {
                assert_eq!(url, "https://example.com/mcp");
                assert!(bearer_token_env_var.is_none());
            }
            other => panic!("unexpected transport {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn write_global_mcp_servers_serializes_disabled_flag() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        let servers = BTreeMap::from([(
            "docs".to_string(),
            McpServerConfig {
                transport: McpServerTransportConfig::Stdio {
                    command: "docs-server".to_string(),
                    args: Vec::new(),
                    env: None,
                },
                enabled: false,
                startup_timeout_sec: None,
                tool_timeout_sec: None,
                keepalive_interval_sec: None,
            },
        )]);

        write_global_mcp_servers(codex_home.path(), &servers)?;

        let config_path = codex_home.path().join(CONFIG_TOML_FILE);
        let serialized = std::fs::read_to_string(&config_path)?;
        assert!(
            serialized.contains("enabled = false"),
            "serialized config missing disabled flag:\n{serialized}"
        );

        let loaded = load_global_mcp_servers(codex_home.path()).await?;
        let docs = loaded.get("docs").expect("docs entry");
        assert!(!docs.enabled);

        Ok(())
    }

    #[tokio::test]
    async fn persist_model_selection_updates_defaults() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        persist_model_selection(
            codex_home.path(),
            None,
            "gpt-5-codex",
            Some(ReasoningEffort::High),
        )
        .await?;

        let serialized =
            tokio::fs::read_to_string(codex_home.path().join(CONFIG_TOML_FILE)).await?;
        let parsed: ConfigToml = toml::from_str(&serialized)?;

        assert_eq!(parsed.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(parsed.model_reasoning_effort, Some(ReasoningEffort::High));

        Ok(())
    }

    #[tokio::test]
    async fn persist_model_selection_overwrites_existing_model() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let config_path = codex_home.path().join(CONFIG_TOML_FILE);

        tokio::fs::write(
            &config_path,
            r#"
model = "gpt-5-codex"
model_reasoning_effort = "medium"

[profiles.dev]
model = "gpt-4.1"
"#,
        )
        .await?;

        persist_model_selection(
            codex_home.path(),
            None,
            "o4-mini",
            Some(ReasoningEffort::High),
        )
        .await?;

        let serialized = tokio::fs::read_to_string(config_path).await?;
        let parsed: ConfigToml = toml::from_str(&serialized)?;

        assert_eq!(parsed.model.as_deref(), Some("o4-mini"));
        assert_eq!(parsed.model_reasoning_effort, Some(ReasoningEffort::High));
        assert_eq!(
            parsed
                .profiles
                .get("dev")
                .and_then(|profile| profile.model.as_deref()),
            Some("gpt-4.1"),
        );

        Ok(())
    }

    #[tokio::test]
    async fn persist_model_selection_updates_profile() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;

        persist_model_selection(
            codex_home.path(),
            Some("dev"),
            "gpt-5-codex",
            Some(ReasoningEffort::Medium),
        )
        .await?;

        let serialized =
            tokio::fs::read_to_string(codex_home.path().join(CONFIG_TOML_FILE)).await?;
        let parsed: ConfigToml = toml::from_str(&serialized)?;
        let profile = parsed
            .profiles
            .get("dev")
            .expect("profile should be created");

        assert_eq!(profile.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(
            profile.model_reasoning_effort,
            Some(ReasoningEffort::Medium)
        );

        Ok(())
    }

    #[tokio::test]
    async fn persist_model_selection_updates_existing_profile() -> anyhow::Result<()> {
        let codex_home = TempDir::new()?;
        let config_path = codex_home.path().join(CONFIG_TOML_FILE);

        tokio::fs::write(
            &config_path,
            r#"
[profiles.dev]
model = "gpt-4"
model_reasoning_effort = "medium"

[profiles.prod]
model = "gpt-5-codex"
"#,
        )
        .await?;

        persist_model_selection(
            codex_home.path(),
            Some("dev"),
            "o4-high",
            Some(ReasoningEffort::Medium),
        )
        .await?;

        let serialized = tokio::fs::read_to_string(config_path).await?;
        let parsed: ConfigToml = toml::from_str(&serialized)?;

        let dev_profile = parsed
            .profiles
            .get("dev")
            .expect("dev profile should survive updates");
        assert_eq!(dev_profile.model.as_deref(), Some("o4-high"));
        assert_eq!(
            dev_profile.model_reasoning_effort,
            Some(ReasoningEffort::Medium)
        );

        assert_eq!(
            parsed
                .profiles
                .get("prod")
                .and_then(|profile| profile.model.as_deref()),
            Some("gpt-5-codex"),
        );

        Ok(())
    }

    struct PrecedenceTestFixture {
        cwd: TempDir,
        codex_home: TempDir,
        cfg: ConfigToml,
        model_provider_map: HashMap<String, ModelProviderInfo>,
        openai_provider: ModelProviderInfo,
        openai_chat_completions_provider: ModelProviderInfo,
    }

    impl PrecedenceTestFixture {
        fn cwd(&self) -> PathBuf {
            self.cwd.path().to_path_buf()
        }

        fn codex_home(&self) -> PathBuf {
            self.codex_home.path().to_path_buf()
        }
    }

    fn create_test_fixture() -> std::io::Result<PrecedenceTestFixture> {
        let toml = r#"
model = "o3"
approval_policy = "untrusted"

# Can be used to determine which profile to use if not specified by
# `ConfigOverrides`.
profile = "gpt3"

[model_providers.openai-chat-completions]
name = "OpenAI using Chat Completions"
base_url = "https://api.openai.com/v1"
env_key = "OPENAI_API_KEY"
wire_api = "chat"
request_max_retries = 4            # retry failed HTTP requests
stream_max_retries = 10            # retry dropped SSE streams
stream_idle_timeout_ms = 300000    # 5m idle timeout

[profiles.o3]
model = "o3"
model_provider = "openai"
approval_policy = "never"
model_reasoning_effort = "high"
model_reasoning_summary = "detailed"

[profiles.gpt3]
model = "gpt-3.5-turbo"
model_provider = "openai-chat-completions"

[profiles.zdr]
model = "o3"
model_provider = "openai"
approval_policy = "on-failure"

[profiles.gpt5]
model = "gpt-5"
model_provider = "openai"
approval_policy = "on-failure"
model_reasoning_effort = "high"
model_reasoning_summary = "detailed"
model_verbosity = "high"
"#;

        let cfg: ConfigToml = toml::from_str(toml).expect("TOML deserialization should succeed");

        // Use a temporary directory for the cwd so it does not contain an
        // AGENTS.md file.
        let cwd_temp_dir = TempDir::new().unwrap();
        let cwd = cwd_temp_dir.path().to_path_buf();
        // Make it look like a Git repo so it does not search for AGENTS.md in
        // a parent folder, either.
        std::fs::write(cwd.join(".git"), "gitdir: nowhere")?;

        let codex_home_temp_dir = TempDir::new().unwrap();

        let openai_chat_completions_provider = ModelProviderInfo {
            name: "OpenAI using Chat Completions".to_string(),
            base_url: Some("https://api.openai.com/v1".to_string()),
            env_key: Some("OPENAI_API_KEY".to_string()),
            wire_api: crate::WireApi::Chat,
            env_key_instructions: None,
            query_params: None,
            http_headers: None,
            env_http_headers: None,
            request_max_retries: Some(4),
            stream_max_retries: Some(10),
            stream_idle_timeout_ms: Some(300_000),
            stream_heartbeat_interval_ms: None,
            requires_openai_auth: false,
        };
        let model_provider_map = {
            let mut model_provider_map = built_in_model_providers();
            model_provider_map.insert(
                "openai-chat-completions".to_string(),
                openai_chat_completions_provider.clone(),
            );
            model_provider_map
        };

        let openai_provider = model_provider_map
            .get("openai")
            .expect("openai provider should exist")
            .clone();

        Ok(PrecedenceTestFixture {
            cwd: cwd_temp_dir,
            codex_home: codex_home_temp_dir,
            cfg,
            model_provider_map,
            openai_provider,
            openai_chat_completions_provider,
        })
    }

    /// Users can specify config values at multiple levels that have the
    /// following precedence:
    ///
    /// 1. custom command-line argument, e.g. `--model o3`
    /// 2. as part of a profile, where the `--profile` is specified via a CLI
    ///    (or in the config file itself)
    /// 3. as an entry in `config.toml`, e.g. `model = "o3"`
    /// 4. the default value for a required field defined in code, e.g.,
    ///    `crate::flags::OPENAI_DEFAULT_MODEL`
    ///
    /// Note that profiles are the recommended way to specify a group of
    /// configuration options together.
    #[test]
    fn test_precedence_fixture_with_o3_profile() -> std::io::Result<()> {
        let fixture = create_test_fixture()?;

        let o3_profile_overrides = ConfigOverrides {
            config_profile: Some("o3".to_string()),
            cwd: Some(fixture.cwd()),
            ..Default::default()
        };
        let o3_profile_config: Config = Config::load_from_base_config_with_overrides(
            fixture.cfg.clone(),
            o3_profile_overrides,
            fixture.codex_home(),
        )?;
        assert_eq!(
            Config {
                model: "o3".to_string(),
                review_model: OPENAI_DEFAULT_REVIEW_MODEL.to_string(),
                model_family: find_family_for_model("o3").expect("known model slug"),
                model_context_window: Some(200_000),
                model_max_output_tokens: Some(100_000),
                model_auto_compact_token_limit: None,
                model_provider_id: "openai".to_string(),
                model_provider: fixture.openai_provider.clone(),
                approval_policy: AskForApproval::Never,
                sandbox_policy: SandboxPolicy::new_read_only_policy(),
                shell_environment_policy: ShellEnvironmentPolicy::default(),
                user_instructions: None,
                notify: None,
                cwd: fixture.cwd(),
                mcp_servers: HashMap::new(),
                mcp_oauth_credentials_store_mode: Default::default(),
                model_providers: fixture.model_provider_map.clone(),
                project_doc_max_bytes: PROJECT_DOC_MAX_BYTES,
                project_doc_fallback_filenames: Vec::new(),
                codex_home: fixture.codex_home(),
                history: History::default(),
                file_opener: UriBasedFileOpener::VsCode,
                codex_linux_sandbox_exe: None,
                mailbox_liveness: MailboxLivenessSettings::default(),
                hide_agent_reasoning: false,
                show_raw_agent_reasoning: false,
                model_reasoning_effort: Some(ReasoningEffort::High),
                model_reasoning_summary: ReasoningSummary::Detailed,
                model_verbosity: None,
                chatgpt_base_url: "https://chatgpt.com/backend-api/".to_string(),
                base_instructions: None,
                include_plan_tool: false,
                include_apply_patch_tool: false,
                tools_web_search_request: false,
                use_experimental_streamable_shell_tool: false,
                use_experimental_unified_exec_tool: false,
                use_experimental_use_rmcp_client: false,
                include_view_image_tool: true,
                include_shell_tool: true,
                include_vm_pty_tool: false,
                include_vm_pty_open_tool: false,
                debug_tools: false,
                vm_pty_socket: None,
                vm_pty_default_vm_id: None,
                vm_pty_connect_timeout: DEFAULT_VM_PTY_CONNECT_TIMEOUT,
                vm_pty_request_timeout: DEFAULT_VM_PTY_REQUEST_TIMEOUT,
                vm_pty_open_timeout: DEFAULT_VM_PTY_OPEN_TIMEOUT,
                vm_pty_open_blocking: false,
                active_profile: Some("o3".to_string()),
                windows_wsl_setup_acknowledged: false,
                disable_paste_burst: false,
                tui_notifications: Default::default(),
                summaries: SummariesSettings::default(),
                wait: WaitPolicySettings::default(),
                otel: OtelConfig::default(),
            },
            o3_profile_config
        );
        Ok(())
    }

    #[test]
    fn test_precedence_fixture_with_gpt3_profile() -> std::io::Result<()> {
        let fixture = create_test_fixture()?;

        let gpt3_profile_overrides = ConfigOverrides {
            config_profile: Some("gpt3".to_string()),
            cwd: Some(fixture.cwd()),
            ..Default::default()
        };
        let gpt3_profile_config = Config::load_from_base_config_with_overrides(
            fixture.cfg.clone(),
            gpt3_profile_overrides,
            fixture.codex_home(),
        )?;
        let expected_gpt3_profile_config = Config {
            model: "gpt-3.5-turbo".to_string(),
            review_model: OPENAI_DEFAULT_REVIEW_MODEL.to_string(),
            model_family: find_family_for_model("gpt-3.5-turbo").expect("known model slug"),
            model_context_window: Some(16_385),
            model_max_output_tokens: Some(4_096),
            model_auto_compact_token_limit: None,
            model_provider_id: "openai-chat-completions".to_string(),
            model_provider: fixture.openai_chat_completions_provider.clone(),
            approval_policy: AskForApproval::UnlessTrusted,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            shell_environment_policy: ShellEnvironmentPolicy::default(),
            user_instructions: None,
            notify: None,
            cwd: fixture.cwd(),
            mcp_servers: HashMap::new(),
            mcp_oauth_credentials_store_mode: Default::default(),
            model_providers: fixture.model_provider_map.clone(),
            project_doc_max_bytes: PROJECT_DOC_MAX_BYTES,
            project_doc_fallback_filenames: Vec::new(),
            codex_home: fixture.codex_home(),
            history: History::default(),
            file_opener: UriBasedFileOpener::VsCode,
            codex_linux_sandbox_exe: None,
            mailbox_liveness: MailboxLivenessSettings::default(),
            hide_agent_reasoning: false,
            show_raw_agent_reasoning: false,
            model_reasoning_effort: None,
            model_reasoning_summary: ReasoningSummary::default(),
            model_verbosity: None,
            chatgpt_base_url: "https://chatgpt.com/backend-api/".to_string(),
            base_instructions: None,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            tools_web_search_request: false,
            use_experimental_streamable_shell_tool: false,
            use_experimental_unified_exec_tool: false,
            use_experimental_use_rmcp_client: false,
            include_view_image_tool: true,
            include_shell_tool: true,
            include_vm_pty_tool: false,
            include_vm_pty_open_tool: false,
            debug_tools: false,
            vm_pty_socket: None,
            vm_pty_default_vm_id: None,
            vm_pty_connect_timeout: DEFAULT_VM_PTY_CONNECT_TIMEOUT,
            vm_pty_request_timeout: DEFAULT_VM_PTY_REQUEST_TIMEOUT,
            vm_pty_open_timeout: DEFAULT_VM_PTY_OPEN_TIMEOUT,
            vm_pty_open_blocking: false,
            active_profile: Some("gpt3".to_string()),
            windows_wsl_setup_acknowledged: false,
            disable_paste_burst: false,
            tui_notifications: Default::default(),
            summaries: SummariesSettings::default(),
            wait: WaitPolicySettings::default(),
            otel: OtelConfig::default(),
        };

        assert_eq!(expected_gpt3_profile_config, gpt3_profile_config);

        // Verify that loading without specifying a profile in ConfigOverrides
        // uses the default profile from the config file (which is "gpt3").
        let default_profile_overrides = ConfigOverrides {
            cwd: Some(fixture.cwd()),
            ..Default::default()
        };

        let default_profile_config = Config::load_from_base_config_with_overrides(
            fixture.cfg.clone(),
            default_profile_overrides,
            fixture.codex_home(),
        )?;

        assert_eq!(expected_gpt3_profile_config, default_profile_config);
        Ok(())
    }

    #[test]
    fn test_precedence_fixture_with_zdr_profile() -> std::io::Result<()> {
        let fixture = create_test_fixture()?;

        let zdr_profile_overrides = ConfigOverrides {
            config_profile: Some("zdr".to_string()),
            cwd: Some(fixture.cwd()),
            ..Default::default()
        };
        let zdr_profile_config = Config::load_from_base_config_with_overrides(
            fixture.cfg.clone(),
            zdr_profile_overrides,
            fixture.codex_home(),
        )?;
        let expected_zdr_profile_config = Config {
            model: "o3".to_string(),
            review_model: OPENAI_DEFAULT_REVIEW_MODEL.to_string(),
            model_family: find_family_for_model("o3").expect("known model slug"),
            model_context_window: Some(200_000),
            model_max_output_tokens: Some(100_000),
            model_auto_compact_token_limit: None,
            model_provider_id: "openai".to_string(),
            model_provider: fixture.openai_provider.clone(),
            approval_policy: AskForApproval::OnFailure,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            shell_environment_policy: ShellEnvironmentPolicy::default(),
            user_instructions: None,
            notify: None,
            cwd: fixture.cwd(),
            mcp_servers: HashMap::new(),
            mcp_oauth_credentials_store_mode: Default::default(),
            model_providers: fixture.model_provider_map.clone(),
            project_doc_max_bytes: PROJECT_DOC_MAX_BYTES,
            project_doc_fallback_filenames: Vec::new(),
            codex_home: fixture.codex_home(),
            history: History::default(),
            file_opener: UriBasedFileOpener::VsCode,
            codex_linux_sandbox_exe: None,
            mailbox_liveness: MailboxLivenessSettings::default(),
            hide_agent_reasoning: false,
            show_raw_agent_reasoning: false,
            model_reasoning_effort: None,
            model_reasoning_summary: ReasoningSummary::default(),
            model_verbosity: None,
            chatgpt_base_url: "https://chatgpt.com/backend-api/".to_string(),
            base_instructions: None,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            tools_web_search_request: false,
            use_experimental_streamable_shell_tool: false,
            use_experimental_unified_exec_tool: false,
            use_experimental_use_rmcp_client: false,
            include_view_image_tool: true,
            include_shell_tool: true,
            include_vm_pty_tool: false,
            include_vm_pty_open_tool: false,
            debug_tools: false,
            vm_pty_socket: None,
            vm_pty_default_vm_id: None,
            vm_pty_connect_timeout: DEFAULT_VM_PTY_CONNECT_TIMEOUT,
            vm_pty_request_timeout: DEFAULT_VM_PTY_REQUEST_TIMEOUT,
            vm_pty_open_timeout: DEFAULT_VM_PTY_OPEN_TIMEOUT,
            vm_pty_open_blocking: false,
            active_profile: Some("zdr".to_string()),
            windows_wsl_setup_acknowledged: false,
            disable_paste_burst: false,
            tui_notifications: Default::default(),
            summaries: SummariesSettings::default(),
            wait: WaitPolicySettings::default(),
            otel: OtelConfig::default(),
        };

        assert_eq!(expected_zdr_profile_config, zdr_profile_config);

        Ok(())
    }

    #[test]
    fn test_precedence_fixture_with_gpt5_profile() -> std::io::Result<()> {
        let fixture = create_test_fixture()?;

        let gpt5_profile_overrides = ConfigOverrides {
            config_profile: Some("gpt5".to_string()),
            cwd: Some(fixture.cwd()),
            ..Default::default()
        };
        let gpt5_profile_config = Config::load_from_base_config_with_overrides(
            fixture.cfg.clone(),
            gpt5_profile_overrides,
            fixture.codex_home(),
        )?;
        let expected_gpt5_profile_config = Config {
            model: "gpt-5".to_string(),
            review_model: OPENAI_DEFAULT_REVIEW_MODEL.to_string(),
            model_family: find_family_for_model("gpt-5").expect("known model slug"),
            model_context_window: Some(272_000),
            model_max_output_tokens: Some(128_000),
            model_auto_compact_token_limit: None,
            model_provider_id: "openai".to_string(),
            model_provider: fixture.openai_provider.clone(),
            approval_policy: AskForApproval::OnFailure,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            shell_environment_policy: ShellEnvironmentPolicy::default(),
            user_instructions: None,
            notify: None,
            cwd: fixture.cwd(),
            mcp_servers: HashMap::new(),
            mcp_oauth_credentials_store_mode: Default::default(),
            model_providers: fixture.model_provider_map.clone(),
            project_doc_max_bytes: PROJECT_DOC_MAX_BYTES,
            project_doc_fallback_filenames: Vec::new(),
            codex_home: fixture.codex_home(),
            history: History::default(),
            file_opener: UriBasedFileOpener::VsCode,
            codex_linux_sandbox_exe: None,
            mailbox_liveness: MailboxLivenessSettings::default(),
            hide_agent_reasoning: false,
            show_raw_agent_reasoning: false,
            model_reasoning_effort: Some(ReasoningEffort::High),
            model_reasoning_summary: ReasoningSummary::Detailed,
            model_verbosity: Some(Verbosity::High),
            chatgpt_base_url: "https://chatgpt.com/backend-api/".to_string(),
            base_instructions: None,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            tools_web_search_request: false,
            use_experimental_streamable_shell_tool: false,
            use_experimental_unified_exec_tool: false,
            use_experimental_use_rmcp_client: false,
            include_view_image_tool: true,
            include_shell_tool: true,
            include_vm_pty_tool: false,
            include_vm_pty_open_tool: false,
            debug_tools: false,
            vm_pty_socket: None,
            vm_pty_default_vm_id: None,
            vm_pty_connect_timeout: DEFAULT_VM_PTY_CONNECT_TIMEOUT,
            vm_pty_request_timeout: DEFAULT_VM_PTY_REQUEST_TIMEOUT,
            vm_pty_open_timeout: DEFAULT_VM_PTY_OPEN_TIMEOUT,
            vm_pty_open_blocking: false,
            active_profile: Some("gpt5".to_string()),
            windows_wsl_setup_acknowledged: false,
            disable_paste_burst: false,
            tui_notifications: Default::default(),
            summaries: SummariesSettings::default(),
            wait: WaitPolicySettings::default(),
            otel: OtelConfig::default(),
        };

        assert_eq!(expected_gpt5_profile_config, gpt5_profile_config);

        Ok(())
    }

    #[test]
    fn test_set_project_trusted_writes_explicit_tables() -> anyhow::Result<()> {
        let project_dir = Path::new("/some/path");
        let mut doc = DocumentMut::new();

        set_project_trusted_inner(&mut doc, project_dir)?;

        let contents = doc.to_string();

        let raw_path = project_dir.to_string_lossy();
        let path_str = if raw_path.contains('\\') {
            format!("'{raw_path}'")
        } else {
            format!("\"{raw_path}\"")
        };
        let expected = format!(
            r#"[projects.{path_str}]
trust_level = "trusted"
"#
        );
        assert_eq!(contents, expected);

        Ok(())
    }

    #[test]
    fn test_set_project_trusted_converts_inline_to_explicit() -> anyhow::Result<()> {
        let project_dir = Path::new("/some/path");

        // Seed config.toml with an inline project entry under [projects]
        let raw_path = project_dir.to_string_lossy();
        let path_str = if raw_path.contains('\\') {
            format!("'{raw_path}'")
        } else {
            format!("\"{raw_path}\"")
        };
        // Use a quoted key so backslashes don't require escaping on Windows
        let initial = format!(
            r#"[projects]
{path_str} = {{ trust_level = "untrusted" }}
"#
        );
        let mut doc = initial.parse::<DocumentMut>()?;

        // Run the function; it should convert to explicit tables and set trusted
        set_project_trusted_inner(&mut doc, project_dir)?;

        let contents = doc.to_string();

        // Assert exact output after conversion to explicit table
        let expected = format!(
            r#"[projects]

[projects.{path_str}]
trust_level = "trusted"
"#
        );
        assert_eq!(contents, expected);

        Ok(())
    }

    #[test]
    fn test_set_project_trusted_migrates_top_level_inline_projects_preserving_entries()
    -> anyhow::Result<()> {
        let initial = r#"toplevel = "baz"
projects = { "/Users/mbolin/code/codex4" = { trust_level = "trusted", foo = "bar" } , "/Users/mbolin/code/codex3" = { trust_level = "trusted" } }
model = "foo""#;
        let mut doc = initial.parse::<DocumentMut>()?;

        // Approve a new directory
        let new_project = Path::new("/Users/mbolin/code/codex2");
        set_project_trusted_inner(&mut doc, new_project)?;

        let contents = doc.to_string();

        // Since we created the [projects] table as part of migration, it is kept implicit.
        // Expect explicit per-project tables, preserving prior entries and appending the new one.
        let expected = r#"toplevel = "baz"
model = "foo"

[projects."/Users/mbolin/code/codex4"]
trust_level = "trusted"
foo = "bar"

[projects."/Users/mbolin/code/codex3"]
trust_level = "trusted"

[projects."/Users/mbolin/code/codex2"]
trust_level = "trusted"
"#;
        assert_eq!(contents, expected);

        Ok(())
    }
}

#[cfg(test)]
mod notifications_tests {
    use crate::config_types::Notifications;
    use assert_matches::assert_matches;
    use serde::Deserialize;

    #[derive(Deserialize, Debug, PartialEq)]
    struct TuiTomlTest {
        notifications: Notifications,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    struct RootTomlTest {
        tui: TuiTomlTest,
    }

    #[test]
    fn test_tui_notifications_true() {
        let toml = r#"
            [tui]
            notifications = true
        "#;
        let parsed: RootTomlTest = toml::from_str(toml).expect("deserialize notifications=true");
        assert_matches!(parsed.tui.notifications, Notifications::Enabled(true));
    }

    #[test]
    fn test_tui_notifications_custom_array() {
        let toml = r#"
            [tui]
            notifications = ["foo"]
        "#;
        let parsed: RootTomlTest =
            toml::from_str(toml).expect("deserialize notifications=[\"foo\"]");
        assert_matches!(
            parsed.tui.notifications,
            Notifications::Custom(ref v) if v == &vec!["foo".to_string()]
        );
    }
}

#[cfg(test)]
mod vm_pty_precedence_tests {
    use super::*;
    use serial_test::serial;

    struct LocalEnvGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl LocalEnvGuard {
        fn set_os(key: &'static str, value: &std::ffi::OsStr) -> Self {
            let original = std::env::var_os(key);
            unsafe { std::env::set_var(key, value) };
            Self { key, original }
        }
    }

    impl Drop for LocalEnvGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(val) => unsafe { std::env::set_var(self.key, val) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    #[test]
    #[serial]
    fn vm_pty_socket_env_fallback_when_unset_in_config() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let cwd = tempfile::tempdir().expect("cwd");
        let socket_dir = tempfile::tempdir().expect("socket_dir");
        let socket_path = socket_dir.path().join("env.sock");
        let _guard = LocalEnvGuard::set_os("CODEX_VM_PTY_SOCKET", socket_path.as_os_str());

        let mut cfg = ConfigToml::default();
        cfg.vm_pty.enabled = Some(true);
        cfg.vm_pty.allow_profiles = vec!["management".to_string()];
        cfg.profiles
            .insert("management".to_string(), ConfigProfile::default());

        let mut overrides = ConfigOverrides::default();
        overrides.cwd = Some(cwd.path().to_path_buf());
        overrides.config_profile = Some("management".to_string());

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");

        assert!(config.include_vm_pty_tool);
        assert_eq!(config.vm_pty_socket.as_deref(), Some(socket_path.as_path()));
    }

    #[test]
    fn vm_pty_socket_profile_beats_project() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let cwd = tempfile::tempdir().expect("cwd");
        let base_socket_dir = tempfile::tempdir().expect("base_socket_dir");
        let base_socket = base_socket_dir.path().join("base.sock");
        let profile_socket_dir = tempfile::tempdir().expect("profile_socket_dir");
        let profile_socket = profile_socket_dir.path().join("profile.sock");

        let mut cfg = ConfigToml::default();
        cfg.vm_pty.enabled = Some(true);
        cfg.vm_pty.allow_profiles = vec!["management".to_string()];
        cfg.tools = Some(ToolsToml {
            vm_pty: VmPtyToml { socket: Some(base_socket.clone()), ..Default::default() },
            ..Default::default()
        });
        let mut profile = ConfigProfile::default();
        profile.tools = Some(ToolsToml {
            vm_pty: VmPtyToml { socket: Some(profile_socket.clone()), ..Default::default() },
            ..Default::default()
        });
        cfg.profiles.insert("management".to_string(), profile);
        cfg.profile = Some("management".to_string());

        let mut overrides = ConfigOverrides::default();
        overrides.cwd = Some(cwd.path().to_path_buf());
        overrides.config_profile = Some("management".to_string());

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");

        assert_eq!(config.vm_pty_socket.as_deref(), Some(profile_socket.as_path()));
    }

    #[test]
    fn vm_pty_socket_cli_beats_profile_and_env() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let cwd = tempfile::tempdir().expect("cwd");
        let base_socket_dir = tempfile::tempdir().expect("base_socket_dir");
        let base_socket = base_socket_dir.path().join("base.sock");
        let profile_socket_dir = tempfile::tempdir().expect("profile_socket_dir");
        let profile_socket = profile_socket_dir.path().join("profile.sock");
        let cli_socket_dir = tempfile::tempdir().expect("cli_socket_dir");
        let cli_socket = cli_socket_dir.path().join("cli.sock");

        let mut cfg = ConfigToml::default();
        cfg.vm_pty.enabled = Some(true);
        cfg.vm_pty.allow_profiles = vec!["management".to_string()];
        cfg.tools = Some(ToolsToml {
            vm_pty: VmPtyToml { socket: Some(base_socket.clone()), ..Default::default() },
            ..Default::default()
        });
        let mut profile = ConfigProfile::default();
        profile.tools = Some(ToolsToml {
            vm_pty: VmPtyToml { socket: Some(profile_socket.clone()), ..Default::default() },
            ..Default::default()
        });
        cfg.profiles.insert("management".to_string(), profile);
        cfg.profile = Some("management".to_string());

        let mut overrides = ConfigOverrides::default();
        overrides.cwd = Some(cwd.path().to_path_buf());
        overrides.config_profile = Some("management".to_string());
        overrides.cli_resolved_overrides = Some(CliResolvedOverrides {
            vm_pty_socket: Some(cli_socket.clone()),
            vm_pty_default_vm_id: None,
            vm_pty_timeouts: CliVmPtyTimeouts::default(),
            vm_pty_open_blocking: None,
            debug_tools: None,
        });

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");

        assert_eq!(config.vm_pty_socket.as_deref(), Some(cli_socket.as_path()));
    }

    #[test]
    #[serial]
    fn debug_tools_env_vs_config_precedence() {
        let _guard = LocalEnvGuard::set_os("CODEX_DEBUG_TOOLS", std::ffi::OsStr::new("1"));
        let codex_home = tempfile::tempdir().expect("codex_home");

        // Config disables debug.tools; env is set to 1, but project config should win.
        let mut cfg = ConfigToml::default();
        cfg.debug = Some(DebugToml { tools: Some(false) });

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            ConfigOverrides::default(),
            codex_home.path().to_path_buf(),
        )
        .expect("config");
        assert_eq!(config.debug_tools, false);
    }

    #[test]
    fn debug_tools_cli_beats_project() {
        let codex_home = tempfile::tempdir().expect("codex_home");
        let mut cfg = ConfigToml::default();
        cfg.debug = Some(DebugToml { tools: Some(false) });

        let mut overrides = ConfigOverrides::default();
        overrides.cli_resolved_overrides = Some(CliResolvedOverrides {
            vm_pty_socket: None,
            vm_pty_default_vm_id: None,
            vm_pty_timeouts: CliVmPtyTimeouts::default(),
            vm_pty_open_blocking: None,
            debug_tools: Some(true),
        });

        let config = Config::load_from_base_config_with_overrides(
            cfg,
            overrides,
            codex_home.path().to_path_buf(),
        )
        .expect("config");
        assert_eq!(config.debug_tools, true);
    }
}
