use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::Read;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use clap::ArgAction;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use codex_common::CliConfigOverrides;
use codex_core::AuthManager;
use codex_core::ConversationManager;
use codex_core::DEFAULT_CRITICAL_MAILBOX_CAPACITY;
use codex_core::DEFAULT_CRITICAL_MAILBOX_INTERVAL_SECONDS;
use codex_core::NewConversation;
use codex_core::apply_mailbox_defaults;
use codex_core::config::Config;
use codex_core::config::ConfigOverrides;
use codex_core::protocol::AskForApproval;
use codex_core::protocol::Event;
use codex_core::protocol::EventMsg;
use codex_core::protocol::MailboxDeliveryState;
use codex_core::protocol::Op;
use codex_core::protocol::SessionSource;
use codex_core::validate_mailbox_message;
use codex_protocol::mailbox::MailboxAckMode;
use codex_protocol::mailbox::MailboxAckPolicy;
use codex_protocol::mailbox::MailboxAudience;
use codex_protocol::mailbox::MailboxAudienceScope;
use codex_protocol::mailbox::MailboxBody;
use codex_protocol::mailbox::MailboxContentType;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxPriority;
use codex_protocol::mailbox::MailboxRateLimitHint;
use codex_protocol::mailbox::MailboxRateLimitScope;
use codex_protocol::mailbox::MailboxSender;
use codex_protocol::mailbox::MailboxSenderRole;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use time::Duration as TimeDuration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use uuid::Uuid;

mod registry;

use self::registry::MailboxRegistry;

const EXIT_UNKNOWN_SESSION: i32 = 64;
const EXIT_QUEUE_FULL: i32 = 69;
const EXIT_IO_FAILURE: i32 = 70;
const CONNECT_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(50),
    Duration::from_millis(200),
    Duration::from_secs(1),
];

#[derive(Debug)]
struct MailboxCliError {
    exit_code: i32,
    message: String,
}

impl MailboxCliError {
    fn new(exit_code: i32, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
        }
    }

    fn unknown_session(message: impl Into<String>) -> Self {
        Self::new(EXIT_UNKNOWN_SESSION, message)
    }

    fn queue_full(message: impl Into<String>) -> Self {
        Self::new(EXIT_QUEUE_FULL, message)
    }

    fn io_failure(message: impl Into<String>) -> Self {
        Self::new(EXIT_IO_FAILURE, message)
    }

    fn exit_code(&self) -> i32 {
        self.exit_code
    }
}

impl std::fmt::Display for MailboxCliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for MailboxCliError {}

#[derive(Debug, Parser)]
pub struct MailCli {
    #[clap(flatten)]
    pub config_overrides: CliConfigOverrides,

    #[command(subcommand)]
    pub command: MailSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum MailSubcommand {
    /// Send a mailbox message to the active Codex session.
    ///
    /// Runbook: docs/codex/analysis/mailbox/20251013-operations.md
    /// Architecture: docs/codex/analysis/mailbox/20251013-architecture.md
    Send(SendArgs),
}

#[derive(Debug, Parser)]
pub struct SendArgs {
    /// Subject rendered in mailbox notifications (optional).
    #[arg(long)]
    pub subject: Option<String>,

    /// Message body content. Use '-' to read from stdin.
    #[arg(long)]
    pub content: Option<String>,

    /// File to read message body from.
    #[arg(long = "content-file", value_name = "FILE")]
    pub content_file: Option<PathBuf>,

    /// MIME type for the body content.
    #[arg(long = "content-type", value_enum, default_value_t = ContentTypeCli::TextPlain)]
    pub content_type: ContentTypeCli,

    /// Sender identifier (`service.account` or human uid).
    #[arg(long = "sender-id")]
    pub sender_id: String,

    /// Sender role (enforces priority guardrails).
    #[arg(long = "sender-role", value_enum, default_value_t = SenderRoleCli::Orchestrator)]
    pub sender_role: SenderRoleCli,

    /// Human-friendly display name for the sender.
    #[arg(long = "sender-display-name")]
    pub sender_display_name: Option<String>,

    /// Escalation contact (mailto:, https://, slack://).
    #[arg(long = "sender-contact")]
    pub sender_contact: Option<String>,

    /// Runbook URL to surface alongside the message.
    #[arg(long = "sender-runbook")]
    pub sender_runbook: Option<String>,

    /// Mailbox priority (controls delivery ordering).
    #[arg(long = "priority", value_enum, default_value_t = PriorityCli::Normal)]
    pub priority: PriorityCli,

    /// Explicit posted_at timestamp (RFC3339). Defaults to now.
    #[arg(long = "posted-at")]
    pub posted_at: Option<String>,

    /// Absolute expiry timestamp (RFC3339). Mutually exclusive with --expires-in.
    #[arg(long = "expires-at")]
    pub expires_at: Option<String>,

    /// Relative expiry window (e.g. "15m", "2h"). Mutually exclusive with --expires-at.
    #[arg(long = "expires-in")]
    pub expires_in: Option<String>,

    /// Configure acknowledgement behaviour.
    /// Default is `passive` to preserve prior behaviour of waiting
    /// for a single delivery acknowledgement line from the mailbox.
    #[arg(long = "ack-mode", value_enum, default_value_t = AckModeCli::Passive)]
    pub ack_mode: AckModeCli,

    /// Deadline for required acknowledgements (RFC3339).
    #[arg(long = "ack-deadline")]
    pub ack_deadline: Option<String>,

    /// Auto-acknowledge after N seconds (passive only).
    #[arg(long = "ack-auto-seconds")]
    pub ack_auto_seconds: Option<u64>,

    /// Escalation ticket required for required ACKs at high/critical priority.
    #[arg(long = "ack-escalation-ticket")]
    pub ack_escalation_ticket: Option<String>,

    /// Audit request identifier (required).
    #[arg(long = "audit-request-id")]
    pub audit_request_id: Option<String>,

    /// External change/request ticket reference.
    #[arg(long = "audit-change-ticket")]
    pub audit_change_ticket: Option<String>,

    /// Principal accountable for the post.
    #[arg(long = "audit-created-by")]
    pub audit_created_by: Option<String>,

    /// Justification for elevated priority or rate limit overrides.
    #[arg(long = "audit-justification")]
    pub audit_justification: Option<String>,

    /// Target conversation id (UUID).
    #[arg(long = "conversation-id")]
    pub conversation_id: Option<Uuid>,

    /// Logical contact name (resolved via contacts.toml).
    #[arg(long = "to")]
    pub to: Option<String>,

    /// Target worker identifier.
    #[arg(long = "worker-id")]
    pub worker_id: Option<String>,

    /// Additional audience scopes (repeatable).
    #[arg(long = "audience-scope", value_enum, action = ArgAction::Append)]
    pub audience_scopes: Vec<AudienceScopeCli>,

    /// Allow broadcast fan-out for the given scopes.
    #[arg(long = "allow-broadcast", default_value_t = false)]
    pub allow_broadcast: bool,

    /// Rate limit scope override.
    #[arg(long = "rate-scope", value_enum)]
    pub rate_scope: Option<RateScopeCli>,

    /// Rate limit capacity for the selected scope.
    #[arg(long = "rate-capacity")]
    pub rate_capacity: Option<u32>,

    /// Rate limit interval in seconds.
    #[arg(long = "rate-interval")]
    pub rate_interval: Option<u32>,

    /// Attach a tag (repeatable, key=value).
    #[arg(long = "tag", value_name = "KEY=VALUE", action = ArgAction::Append)]
    pub tags: Vec<String>,

    /// Attach metadata (repeatable, key=value or key=json).
    #[arg(long = "metadata", value_name = "KEY=VALUE", action = ArgAction::Append)]
    pub metadata: Vec<String>,

    /// Provide message id (uuid). Defaults to randomly generated v7.
    #[arg(long = "message-id")]
    pub message_id: Option<Uuid>,

    /// Config profile to use (matches config.toml profile keys).
    #[arg(long = "profile", short = 'p')]
    pub config_profile: Option<String>,

    /// Overrides the working directory used for sandbox/tool resolution.
    #[arg(long = "cd", short = 'C', value_name = "DIR")]
    pub cwd: Option<PathBuf>,

    /// Timeout for waiting on delivery acknowledgement (humantime, e.g. "20s").
    #[arg(long = "timeout", default_value = "30s")]
    pub timeout: String,

    /// Emit acknowledgement details as JSON.
    #[arg(long = "json", default_value_t = false)]
    pub json: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum PriorityCli {
    Critical,
    High,
    Normal,
    Low,
}

impl From<PriorityCli> for MailboxPriority {
    fn from(value: PriorityCli) -> Self {
        match value {
            PriorityCli::Critical => MailboxPriority::Critical,
            PriorityCli::High => MailboxPriority::High,
            PriorityCli::Normal => MailboxPriority::Normal,
            PriorityCli::Low => MailboxPriority::Low,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum SenderRoleCli {
    Orchestrator,
    Operator,
    Automation,
    System,
}

impl From<SenderRoleCli> for MailboxSenderRole {
    fn from(value: SenderRoleCli) -> Self {
        match value {
            SenderRoleCli::Orchestrator => MailboxSenderRole::Orchestrator,
            SenderRoleCli::Operator => MailboxSenderRole::Operator,
            SenderRoleCli::Automation => MailboxSenderRole::Automation,
            SenderRoleCli::System => MailboxSenderRole::System,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum AckModeCli {
    None,
    Passive,
    Required,
}

impl From<AckModeCli> for MailboxAckMode {
    fn from(value: AckModeCli) -> Self {
        match value {
            AckModeCli::None => MailboxAckMode::None,
            AckModeCli::Passive => MailboxAckMode::Passive,
            AckModeCli::Required => MailboxAckMode::Required,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ContentTypeCli {
    #[value(alias = "text/plain")]
    TextPlain,
    #[value(alias = "text/markdown")]
    TextMarkdown,
    #[value(alias = "application/json")]
    ApplicationJson,
}

impl From<ContentTypeCli> for MailboxContentType {
    fn from(value: ContentTypeCli) -> Self {
        match value {
            ContentTypeCli::TextPlain => MailboxContentType::TextPlain,
            ContentTypeCli::TextMarkdown => MailboxContentType::TextMarkdown,
            ContentTypeCli::ApplicationJson => MailboxContentType::ApplicationJson,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum AudienceScopeCli {
    Session,
    Task,
    Workspace,
    Environment,
}

impl From<AudienceScopeCli> for MailboxAudienceScope {
    fn from(value: AudienceScopeCli) -> Self {
        match value {
            AudienceScopeCli::Session => MailboxAudienceScope::Session,
            AudienceScopeCli::Task => MailboxAudienceScope::Task,
            AudienceScopeCli::Workspace => MailboxAudienceScope::Workspace,
            AudienceScopeCli::Environment => MailboxAudienceScope::Environment,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum RateScopeCli {
    Conversation,
    Workspace,
    Orchestrator,
}

impl From<RateScopeCli> for MailboxRateLimitScope {
    fn from(value: RateScopeCli) -> Self {
        match value {
            RateScopeCli::Conversation => MailboxRateLimitScope::Conversation,
            RateScopeCli::Workspace => MailboxRateLimitScope::Workspace,
            RateScopeCli::Orchestrator => MailboxRateLimitScope::Orchestrator,
        }
    }
}

impl MailCli {
    pub async fn run(self, codex_linux_sandbox_exe: Option<PathBuf>) -> Result<()> {
        match self.command {
            MailSubcommand::Send(args) => {
                match run_send(args, self.config_overrides, codex_linux_sandbox_exe).await {
                    Ok(()) => Ok(()),
                    Err(err) => match err.downcast::<MailboxCliError>() {
                        Ok(cli_err) => {
                            eprintln!("{cli_err}");
                            process::exit(cli_err.exit_code());
                        }
                        Err(err) => Err(err),
                    },
                }
            }
        }
    }
}

async fn run_send(
    args: SendArgs,
    cli_overrides: CliConfigOverrides,
    codex_linux_sandbox_exe: Option<PathBuf>,
) -> Result<()> {
    let SendArgs {
        subject,
        content,
        content_file,
        content_type,
        sender_id,
        sender_role,
        sender_display_name,
        sender_contact,
        sender_runbook,
        priority,
        posted_at,
        expires_at,
        expires_in,
        ack_mode,
        ack_deadline,
        ack_auto_seconds,
        ack_escalation_ticket,
        audit_request_id,
        audit_change_ticket,
        audit_created_by,
        audit_justification,
        conversation_id,
        to,
        worker_id,
        audience_scopes,
        allow_broadcast,
        rate_scope,
        rate_capacity,
        rate_interval,
        tags,
        metadata,
        message_id,
        config_profile,
        cwd,
        timeout,
        json,
    } = args;

    // Load config early to resolve contacts if needed
    let config =
        load_config(cli_overrides.clone(), config_profile.clone(), cwd.clone(), codex_linux_sandbox_exe.clone()).await?;

    // Resolve contact name to conversation id if provided
    let resolved_contact_id: Option<Uuid> = if let Some(name) = &to {
        let ns = resolve_namespace();
        let contacts = codex_core::contacts::load_contacts(&config.codex_home, &ns)
            .map_err(|e| anyhow!(e))?;
        match contacts.resolve(name) {
            Some(id) => Some(id),
            None => {
                let (primary, _global) = codex_core::contacts::contacts_paths(&config.codex_home, &ns);
                return Err(anyhow::Error::new(MailboxCliError::io_failure(format!(
                    "Contact '{name}' not found in namespace '{ns}'. Check {} or run `codex_ctl mailbox sweep` if the mapping is stale.",
                    primary.display()
                ))));
            }
        }
    } else {
        None
    };

    let body_content = load_body(content, content_file)?;
    if body_content.trim().is_empty() {
        bail!("Mailbox body content may not be empty");
    }

    if expires_at.is_some() && expires_in.is_some() {
        bail!("--expires-at and --expires-in are mutually exclusive");
    }

    let wait_timeout =
        parse_duration(&timeout).with_context(|| format!("invalid timeout value '{timeout}'"))?;

    let posted_at = match posted_at {
        Some(raw) => {
            Some(parse_rfc3339(&raw).with_context(|| format!("invalid --posted-at: {raw}"))?)
        }
        None => Some(OffsetDateTime::now_utc()),
    };

    let expires_at = if let Some(raw) = expires_at {
        Some(parse_rfc3339(&raw).with_context(|| format!("invalid --expires-at: {raw}"))?)
    } else if let Some(raw) = expires_in {
        let delta = parse_duration(&raw).with_context(|| format!("invalid --expires-in: {raw}"))?;
        let time_delta = TimeDuration::try_from(delta)
            .map_err(|_| anyhow!("expires-in duration out of range"))?;
        Some(OffsetDateTime::now_utc() + time_delta)
    } else {
        None
    };

    let ack_deadline = match ack_deadline {
        Some(raw) => {
            Some(parse_rfc3339(&raw).with_context(|| format!("invalid --ack-deadline: {raw}"))?)
        }
        None => None,
    };

    let audit_request_id = audit_request_id
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow!("--audit-request-id is required"))?;

    if audit_request_id.len() > 200 {
        bail!("audit request id exceeds 200 characters");
    }

    let priority: MailboxPriority = priority.into();
    let ack_mode: MailboxAckMode = ack_mode.into();
    let rate_limit = build_rate_limit(&priority, rate_scope, rate_capacity, rate_interval)?;

    let message_id = message_id.unwrap_or_else(Uuid::now_v7);
    let sender = MailboxSender {
        id: sender_id.clone(),
        role: sender_role.into(),
        display_name: sender_display_name.filter(|s| !s.is_empty()),
        contact: sender_contact.filter(|s| !s.is_empty()),
        runbook: sender_runbook.filter(|s| !s.is_empty()),
    };

    let tags = parse_tags(tags)?;
    let metadata = parse_metadata(metadata)?;

    let target_conversation_id = conversation_id.or(resolved_contact_id);

    let audience = if target_conversation_id.is_some()
        || worker_id.is_some()
        || !audience_scopes.is_empty()
        || allow_broadcast
    {
        Some(build_audience(
            target_conversation_id,
            worker_id,
            audience_scopes,
            allow_broadcast,
        )?)
    } else {
        None
    };

    let body = MailboxBody {
        subject,
        content: body_content,
        content_type: content_type.into(),
    };

    let audit = codex_protocol::mailbox::MailboxAuditTrail {
        request_id: Some(audit_request_id.clone()),
        change_ticket: audit_change_ticket,
        created_by: audit_created_by,
        justification: audit_justification,
    };

    let ack_policy = MailboxAckPolicy {
        mode: ack_mode,
        deadline: ack_deadline,
        auto_ack_seconds: ack_auto_seconds,
        escalation_ticket: ack_escalation_ticket,
    };

    let mut message = MailboxMessage {
        message_id,
        priority,
        posted_at,
        expires_at,
        sender,
        audience,
        body,
        ack_policy,
        audit,
        attachments: vec![],
        tags,
        metadata,
        rate_limit,
    };

    apply_mailbox_defaults(&mut message);
    validate_mailbox_message(&message)?;

    if let Some(target_conversation_id) = target_conversation_id {
        send_via_registry(message, target_conversation_id, &config, wait_timeout, json)
            .await
            .map_err(|err| anyhow::Error::new(err))?;
        return Ok(());
    }

    let auth_manager = AuthManager::shared(config.codex_home.clone(), true);
    let manager = ConversationManager::new(auth_manager, SessionSource::Exec);
    let NewConversation { conversation, .. } = manager
        .new_conversation(config)
        .await
        .context("failed to spawn Codex conversation")?;

    let submission_id = conversation
        .submit(Op::MailboxEnvelope {
            envelope: message.clone(),
        })
        .await
        .context("failed to enqueue mailbox envelope")?;

    let (enqueued_event, delivered_event) =
        await_delivery_events(&conversation, message.message_id, wait_timeout).await?;

    if json {
        let payload = serde_json::json!({
            "submission_id": submission_id,
            "message_id": message.message_id,
            "request_id": message.audit.request_id,
            "priority": format!("{:?}", message.priority).to_lowercase(),
            "enqueued": event_snapshot(&enqueued_event),
            "delivered": event_snapshot(&delivered_event),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "Mailbox message {} enqueued (queue_depth={}, observed_at={})",
            message.message_id,
            enqueued_event.queue_depth.unwrap_or_default(),
            enqueued_event
                .observed_at
                .map(|t| t.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into()))
                .unwrap_or_else(|| "<unknown>".into()),
        );
        println!(
            "Delivery confirmed (queue_depth={}, observed_at={})",
            delivered_event.queue_depth.unwrap_or_default(),
            delivered_event
                .observed_at
                .map(|t| t.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into()))
                .unwrap_or_else(|| "<unknown>".into()),
        );
        if let Some(req) = &message.audit.request_id {
            println!("Audit request id: {req}");
        }
    }

    Ok(())
}

async fn send_via_registry(
    message: MailboxMessage,
    conversation_id: Uuid,
    config: &Config,
    wait_timeout: Duration,
    json: bool,
) -> Result<(), MailboxCliError> {
    let namespace = resolve_namespace();
    let mailbox_dir = config.codex_home.join(&namespace).join("mailbox");
    let registry_path = mailbox_dir.join("registry.json");

    let mut registry =
        MailboxRegistry::load(registry_path.clone(), namespace.clone()).map_err(|err| {
            MailboxCliError::io_failure(format!(
                "failed to load mailbox registry {}: {err}",
                registry_path.display()
            ))
        })?;

    // Prefer registry entry when present, but fall back to a deterministic socket path
    // when the registry has no record for the provided conversation id.
    let (socket_path, stored_socket_path, have_registry_entry) = match registry
        .find(&conversation_id)
        .cloned()
    {
        Some(entry) => {
            let stored = entry.socket_path.clone();
            let resolved = if stored.is_absolute() {
                stored.clone()
            } else {
                mailbox_dir.join(&stored)
            };
            (resolved, stored, true)
        }
        None => {
            let resolved = mailbox_dir.join(format!("{}.sock", conversation_id));
            // Use the resolved path as a stand-in for stored_socket_path when no registry
            // entry exists. We will avoid any registry mutation paths guarded by
            // `have_registry_entry` later.
            (resolved.clone(), resolved, false)
        }
    };

    let stream = match connect_with_retry(&socket_path).await {
        Ok(stream) => stream,
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            if have_registry_entry {
                // Treat unreachable sockets as stale: remove the entry to avoid replays
                let removed =
                    registry.remove_if_socket_matches(&conversation_id, &stored_socket_path)
                        || registry.remove(&conversation_id);
                if removed {
                    if let Err(save_err) = registry.save() {
                        return Err(MailboxCliError::io_failure(format!(
                            "failed to update mailbox registry {}: {save_err}",
                            registry_path.display()
                        )));
                    }
                }
                return Err(MailboxCliError::unknown_session(format!(
                    "Failed to reach socket {} for conversation {conversation_id}. \
Run `codex_ctl mailbox sweep` and retry once the worker reconnects.",
                    socket_path.display()
                )));
            }
            // No registry entry: clearly report the missing/unreachable socket path
            let cause = match err.kind() {
                io::ErrorKind::NotFound => "not found",
                io::ErrorKind::ConnectionRefused => "connection refused",
                _ => "unreachable",
            };
            return Err(MailboxCliError::unknown_session(format!(
                "Conversation {conversation_id} is not registered in namespace {namespace}, and the mailbox socket at {} is {cause}. \
Ensure the worker is online; the socket appears at this path when connected.",
                socket_path.display()
            )));
        }
        Err(err) => {
            return Err(MailboxCliError::io_failure(format!(
                "failed to connect to mailbox socket {}: {err}",
                socket_path.display()
            )));
        }
    };

    // If caller requested JSON and ack mode is 'none', write payload and emit minimal JSON
    // without waiting for an acknowledgement. This prevents empty/zero-length JSON outputs
    // when servers choose not to reply for ack-less deliveries.
    if json && matches!(message.ack_policy.mode, MailboxAckMode::None) {
        // Write message then return immediately with a minimal result payload.
        // If the listener closes immediately (EPIPE/ConnectionReset), treat it as
        // success for ack-less mode to support fire-and-forget receivers.
        let mut stream = stream;
        let mut payload = serde_json::to_vec(&message).map_err(|err| {
            MailboxCliError::io_failure(format!("failed to serialize mailbox message: {err}"))
        })?;
        payload.push(b'\n');
        use tokio::io::AsyncWriteExt;
        if let Err(err) = stream.write_all(&payload).await {
            if err.kind() != io::ErrorKind::BrokenPipe && err.kind() != io::ErrorKind::ConnectionReset {
                return Err(MailboxCliError::io_failure(format!(
                    "failed to send mailbox payload: {err}"
                )));
            }
        } else if let Err(err) = stream.flush().await {
            if err.kind() != io::ErrorKind::BrokenPipe && err.kind() != io::ErrorKind::ConnectionReset {
                return Err(MailboxCliError::io_failure(format!(
                    "failed to flush mailbox payload: {err}"
                )));
            }
        }

        let output = MailboxSendJsonOutput {
            ok: true,
            message_id: message.message_id,
            request_id: message.audit.request_id.clone(),
            conversation_id,
            ack: Some("none".to_string()),
            queue_depth: None,
            correlation_id: None,
            socket_path: socket_path.display().to_string(),
        };
        let serialized = serde_json::to_string_pretty(&output).map_err(|err| {
            MailboxCliError::io_failure(format!(
                "failed to serialize mailbox acknowledgement output: {err}"
            ))
        })?;
        println!("{serialized}");
        return Ok(());
    }

    let ack = write_message_and_receive_ack(stream, &message, wait_timeout).await?;

    if !ack.ok {
        if ack.err.as_deref() == Some("queue_full") {
            let queue_depth = ack.queue_depth.unwrap_or_default();
            let mut guidance = format!(
                "Mailbox queue is full (depth {queue_depth}). Clear the inbox or retry after consumers catch up."
            );
            if let Some(reason) = ack.reason.as_ref() {
                guidance.push_str(" ");
                guidance.push_str(reason);
            }
            return Err(MailboxCliError::queue_full(guidance));
        }
        if ack.err.as_deref() == Some("unknown_conversation") {
            if have_registry_entry {
                let removed =
                    registry.remove_if_socket_matches(&conversation_id, &stored_socket_path);
                if removed {
                    if let Err(save_err) = registry.save() {
                        return Err(MailboxCliError::io_failure(format!(
                            "failed to update mailbox registry {}: {save_err}",
                            registry_path.display()
                        )));
                    }
                }
            }
            return Err(MailboxCliError::unknown_session(format!(
                "Conversation {conversation_id} returned unknown_conversation. \
Run `codex_ctl mailbox sweep` and ensure the worker session is online."
            )));
        }
        let err_label = ack.err.unwrap_or_else(|| "unknown_error".to_string());
        let reason = ack
            .reason
            .unwrap_or_else(|| "no additional context provided".to_string());
        return Err(MailboxCliError::io_failure(format!(
            "Mailbox delivery failed ({err_label}): {reason}"
        )));
    }

    if let Some(ack_message_id) = ack.message_id {
        if ack_message_id != message.message_id {
            return Err(MailboxCliError::io_failure(format!(
                "Mailbox acknowledgement referenced unexpected message id {ack_message_id}"
            )));
        }
    }

    if json {
        let output = MailboxSendJsonOutput {
            ok: ack.ok,
            message_id: message.message_id,
            request_id: message.audit.request_id.clone(),
            conversation_id,
            ack: ack.ack.clone(),
            queue_depth: ack.queue_depth,
            correlation_id: ack.correlation_id.clone(),
            socket_path: socket_path.display().to_string(),
        };
        let serialized = serde_json::to_string_pretty(&output).map_err(|err| {
            MailboxCliError::io_failure(format!(
                "failed to serialize mailbox acknowledgement output: {err}"
            ))
        })?;
        println!("{serialized}");
    } else {
        let ack_label = ack.ack.as_deref().unwrap_or("delivered");
        let queue_depth = ack.queue_depth.unwrap_or_default();
        println!(
            "Mailbox message {} delivered to conversation {} (ack={}, queue_depth={}, socket={})",
            message.message_id,
            conversation_id,
            ack_label,
            queue_depth,
            socket_path.display()
        );
        if let Some(req) = &message.audit.request_id {
            println!("Audit request id: {req}");
        }
        if let Some(correlation) = &ack.correlation_id {
            println!("Delivery correlation id: {correlation}");
        }
    }

    Ok(())
}

async fn connect_with_retry(socket_path: &Path) -> Result<UnixStream, io::Error> {
    let mut delays = CONNECT_RETRY_DELAYS.iter();
    loop {
        match UnixStream::connect(socket_path).await {
            Ok(stream) => return Ok(stream),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                if let Some(delay) = delays.next() {
                    sleep(*delay).await;
                    continue;
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        }
    }
}

async fn write_message_and_receive_ack(
    stream: UnixStream,
    message: &MailboxMessage,
    wait_timeout: Duration,
) -> Result<MailboxIpcAck, MailboxCliError> {
    let mut stream = stream;
    let mut payload = serde_json::to_vec(message).map_err(|err| {
        MailboxCliError::io_failure(format!("failed to serialize mailbox message: {err}"))
    })?;
    payload.push(b'\n');

    stream.write_all(&payload).await.map_err(|err| {
        MailboxCliError::io_failure(format!("failed to send mailbox payload: {err}"))
    })?;
    stream.flush().await.map_err(|err| {
        MailboxCliError::io_failure(format!("failed to flush mailbox payload: {err}"))
    })?;

    let mut reader = BufReader::new(stream);
    let mut ack_line = String::new();
    let bytes_read = timeout(wait_timeout, reader.read_line(&mut ack_line))
        .await
        .map_err(|_| {
            #[cfg(feature = "otel")]
            {
                let ns = resolve_namespace();
                codex_otel::metrics::record_mailbox_error_total(&ns, "ack_timeout");
            }
            MailboxCliError::io_failure("timed out waiting for mailbox acknowledgement".to_string())
        })?
        .map_err(|err| {
            MailboxCliError::io_failure(format!("failed to read mailbox acknowledgement: {err}"))
        })?;

    if bytes_read == 0 {
        return Err(MailboxCliError::io_failure(
            "mailbox listener closed the connection without sending an acknowledgement".to_string(),
        ));
    }

    let ack_line = ack_line.trim();
    let ack: MailboxIpcAck = serde_json::from_str(ack_line).map_err(|err| {
        MailboxCliError::io_failure(format!(
            "failed to parse mailbox acknowledgement '{}': {err}",
            ack_line
        ))
    })?;
    Ok(ack)
}

fn resolve_namespace() -> String {
    env::var("CODEX_NAMESPACE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "codex".to_string())
}

fn load_body(content: Option<String>, content_file: Option<PathBuf>) -> Result<String> {
    if let Some(path) = content_file {
        let mut file = fs::File::open(&path)
            .with_context(|| format!("failed to open body file {}", path.display()))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)
            .with_context(|| format!("failed to read file {}", path.display()))?;
        return Ok(buf);
    }

    if let Some(raw) = content {
        if raw == "-" {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            return Ok(buf);
        }
        return Ok(raw);
    }

    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf)?;
    Ok(buf)
}

fn build_rate_limit(
    priority: &MailboxPriority,
    scope: Option<RateScopeCli>,
    capacity: Option<u32>,
    interval: Option<u32>,
) -> Result<Option<MailboxRateLimitHint>> {
    if scope.is_none() && capacity.is_none() && interval.is_none() {
        if is_critical(priority) {
            return Ok(Some(MailboxRateLimitHint {
                scope: MailboxRateLimitScope::Conversation,
                capacity: Some(DEFAULT_CRITICAL_MAILBOX_CAPACITY),
                interval_seconds: Some(DEFAULT_CRITICAL_MAILBOX_INTERVAL_SECONDS),
            }));
        }
        return Ok(None);
    }

    let scope: MailboxRateLimitScope = scope
        .map(|s| s.into())
        .unwrap_or(MailboxRateLimitScope::Conversation);
    Ok(Some(MailboxRateLimitHint {
        scope,
        capacity,
        interval_seconds: interval,
    }))
}

fn parse_tags(raw: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for entry in raw {
        let (key, value) = split_key_value(&entry)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn is_critical(priority: &MailboxPriority) -> bool {
    matches!(*priority, MailboxPriority::Critical)
}

fn parse_metadata(raw: Vec<String>) -> Result<BTreeMap<String, JsonValue>> {
    let mut map = BTreeMap::new();
    for entry in raw {
        let (key, value) = split_key_value(&entry)?;
        let value = serde_json::from_str(&value).unwrap_or(JsonValue::String(value));
        map.insert(key, value);
    }
    Ok(map)
}

fn split_key_value(raw: &str) -> Result<(String, String)> {
    let mut parts = raw.splitn(2, '=');
    let key = parts
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("invalid key=value pair: {raw}"))?;
    let value = parts
        .next()
        .map(|s| s.trim().to_string())
        .ok_or_else(|| anyhow!("invalid key=value pair: {raw}"))?;
    Ok((key, value))
}

fn parse_rfc3339(raw: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).map_err(|e| anyhow!("{e}"))
}

fn parse_duration(raw: &str) -> Result<Duration> {
    let dur = humantime::parse_duration(raw)?;
    Ok(dur)
}

fn build_audience(
    conversation_id: Option<Uuid>,
    worker_id: Option<String>,
    scopes: Vec<AudienceScopeCli>,
    allow_broadcast: bool,
) -> Result<MailboxAudience> {
    let scopes = if scopes.is_empty() {
        None
    } else {
        Some(scopes.into_iter().map(|scope| scope.into()).collect())
    };
    Ok(MailboxAudience {
        conversation_id,
        worker_id,
        scopes,
        allow_broadcast,
    })
}

async fn load_config(
    cli_overrides: CliConfigOverrides,
    config_profile: Option<String>,
    cwd: Option<PathBuf>,
    codex_linux_sandbox_exe: Option<PathBuf>,
) -> Result<Config> {
    let overrides = ConfigOverrides {
        model: None,
        review_model: None,
        cwd: cwd.map(|p| p.canonicalize().unwrap_or(p)),
        approval_policy: Some(AskForApproval::Never),
        sandbox_mode: None,
        model_provider: None,
        config_profile,
        codex_linux_sandbox_exe,
        base_instructions: None,
        include_plan_tool: None,
        include_apply_patch_tool: None,
        include_view_image_tool: None,
        show_raw_agent_reasoning: None,
        tools_web_search_request: None,
    };

    let kv_overrides = cli_overrides.parse_overrides().map_err(|e| anyhow!(e))?;
    let config = Config::load_with_cli_overrides(kv_overrides, overrides)
        .await
        .context("failed to load Codex configuration")?;
    Ok(config)
}

async fn await_delivery_events(
    conversation: &codex_core::CodexConversation,
    message_id: Uuid,
    timeout_duration: Duration,
) -> Result<(
    codex_core::protocol::MailboxDeliveryEvent,
    codex_core::protocol::MailboxDeliveryEvent,
)> {
    let deadline = Instant::now() + timeout_duration;
    let mut enqueued: Option<codex_core::protocol::MailboxDeliveryEvent> = None;
    let mut delivered: Option<codex_core::protocol::MailboxDeliveryEvent> = None;

    loop {
        let now = Instant::now();
        if now >= deadline {
            #[cfg(feature = "otel")]
            {
                let ns = resolve_namespace();
                codex_otel::metrics::record_mailbox_error_total(&ns, "ack_timeout");
            }
            bail!("timed out waiting for mailbox delivery acknowledgement");
        }

        let remaining = deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            #[cfg(feature = "otel")]
            {
                let ns = resolve_namespace();
                codex_otel::metrics::record_mailbox_error_total(&ns, "ack_timeout");
            }
            bail!("timed out waiting for mailbox delivery acknowledgement");
        }

        let next_event = timeout(remaining, conversation.next_event())
            .await
            .context("timed out waiting for event")?
            .context("codex event stream ended unexpectedly")?;

        if let Some(delivery) = extract_delivery(next_event, message_id) {
            match delivery.state {
                MailboxDeliveryState::Enqueued => enqueued = Some(delivery),
                MailboxDeliveryState::Delivered => delivered = Some(delivery),
            }
        }

        if let (Some(enq), Some(del)) = (&enqueued, &delivered) {
            return Ok((enq.clone(), del.clone()));
        }
    }
}

fn extract_delivery(
    event: Event,
    message_id: Uuid,
) -> Option<codex_core::protocol::MailboxDeliveryEvent> {
    match event.msg {
        EventMsg::MailboxDelivery(delivery) if delivery.message.message_id == message_id => {
            Some(delivery)
        }
        _ => None,
    }
}

fn event_snapshot(event: &codex_core::protocol::MailboxDeliveryEvent) -> JsonValue {
    serde_json::json!({
        "state": format!("{:?}", event.state).to_lowercase(),
        "queue_depth": event.queue_depth,
        "observed_at": event.observed_at.map(|t| t.format(&Rfc3339).unwrap_or_else(|_| "<invalid>".into())),
        "delivery_latency_ms": event.delivery_latency_ms,
        "correlation_id": event.correlation_id.clone(),
    })
}

#[derive(Debug, Deserialize)]
struct MailboxIpcAck {
    ok: bool,
    #[serde(default)]
    ack: Option<String>,
    #[serde(default)]
    err: Option<String>,
    #[serde(default)]
    queue_depth: Option<u64>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    message_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct MailboxSendJsonOutput {
    ok: bool,
    message_id: Uuid,
    request_id: Option<String>,
    conversation_id: Uuid,
    ack: Option<String>,
    queue_depth: Option<u64>,
    correlation_id: Option<String>,
    socket_path: String,
}
