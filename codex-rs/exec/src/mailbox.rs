use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use codex_core::CodexConversation;
use codex_core::apply_mailbox_defaults;
use codex_core::config::Config;
use codex_core::protocol::BackgroundEventEvent;
use codex_core::protocol::Event;
use codex_core::protocol::EventMsg;
use codex_core::protocol::MailboxDeliveryEvent;
use codex_core::protocol::MailboxDeliveryState;
use codex_core::protocol::Op;
use codex_core::protocol::Submission;
use codex_core::validate_mailbox_message;
use codex_protocol::ConversationId;
use codex_protocol::mailbox::MailboxMessage;
use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;
use fs2::FileExt as _;

mod mailbox_spool;
use mailbox_spool::MailboxSpoolWriter;

const DEFAULT_SOCKET_BACKLOG: libc::c_int = 16;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const MAX_STALE_HEARTBEAT_AGE: Duration = Duration::from_secs(300);
#[cfg(target_os = "linux")]
const UNIX_SOCKET_PATH_LIMIT: usize = 104;
#[cfg(not(target_os = "linux"))]
const UNIX_SOCKET_PATH_LIMIT: usize = 100;

const REGISTRY_VERSION: u32 = 1;
const ACK_TIMEOUT: Duration = Duration::from_secs(10);

pub struct MailboxServer {
    session_id: String,
    socket_path: PathBuf,
    registry: MailboxRegistry,
    shutdown: Arc<Notify>,
    listener_handle: JoinHandle<()>,
    heartbeat_handle: JoinHandle<()>,
    events: Arc<MailboxEventRegistry>,
    spool: Option<MailboxSpoolWriter>,
}

impl MailboxServer {
    pub async fn start_if_enabled(
        config: &Config,
        conversation: Arc<CodexConversation>,
        conversation_id: ConversationId,
    ) -> Result<Option<Self>> {
        if !mailbox_ipc_enabled() {
            return Ok(None);
        }

        let namespace = mailbox_namespace();
        let mailbox_dir = config.codex_home.join(&namespace).join("mailbox");
        fs::create_dir_all(&mailbox_dir).with_context(|| {
            format!(
                "failed to create mailbox directory {}",
                mailbox_dir.display()
            )
        })?;

        let registry_path = mailbox_dir.join("registry.json");
        let registry = MailboxRegistry::new(registry_path);
        registry
            .sweep_stale_entries(MAX_STALE_HEARTBEAT_AGE)
            .await?;

        let session_id = conversation_id.to_string();
        let socket_path = resolve_socket_path(&mailbox_dir, namespace.as_str(), &session_id)?;

        if socket_path.exists() {
            fs::remove_file(&socket_path).with_context(|| {
                format!("failed to remove stale socket {}", socket_path.display())
            })?;
        }

        let backlog = mailbox_backlog();
        let listener = bind_unix_listener(&socket_path, backlog)
            .with_context(|| format!("failed to bind mailbox socket {}", socket_path.display()))?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", socket_path.display()))?;

        let now = OffsetDateTime::now_utc();
        let entry = RegistryEntry {
            session_id: session_id.clone(),
            pid: std::process::id(),
            socket_path: socket_path.to_string_lossy().into_owned(),
            created_at: now,
            last_heartbeat: now,
            namespace: namespace.clone(),
        };
        registry.upsert_entry(entry).await?;

        let shutdown = Arc::new(Notify::new());
        let events = Arc::new(MailboxEventRegistry::default());

        // Initialize per-conversation spool writer (best-effort, non-fatal)
        let spool = match MailboxSpoolWriter::new(
            config.clone(),
            &namespace,
            conversation_id,
        )
        .await
        {
            Ok(writer) => Some(writer),
            Err(err) => {
                warn!(target: "codex::mailbox", event = "mailbox.spool.init_failed", ?err, "failed to initialize mailbox spool; continuing without persistence");
                None
            }
        };

        let listener_shutdown = shutdown.clone();
        let listener_registry = registry.clone();
        let listener_events = events.clone();
        let listener_session = session_id.clone();
        let listener_handle = tokio::spawn(async move {
            if let Err(err) = run_accept_loop(
                listener,
                conversation,
                listener_events,
                listener_registry,
                listener_session,
                listener_shutdown,
            )
            .await
            {
                error!(target: "codex::mailbox", event = "mailbox.error.accept_loop", ?err, "mailbox accept loop exited with error");
            }
        });

        let heartbeat_shutdown = shutdown.clone();
        let heartbeat_registry = registry.clone();
        let heartbeat_session = session_id.clone();
        let heartbeat_handle = tokio::spawn(async move {
            run_heartbeat_loop(heartbeat_registry, heartbeat_session, heartbeat_shutdown).await;
        });

        Ok(Some(Self {
            session_id,
            socket_path,
            registry,
            shutdown,
            listener_handle,
            heartbeat_handle,
            events,
            spool,
        }))
    }

    pub async fn shutdown(self) -> Result<()> {
        self.shutdown.notify_waiters();

        if let Err(err) = self.listener_handle.await {
            warn!(target: "codex::mailbox", event = "mailbox.error.listener_task", ?err, "mailbox listener task panicked");
        }
        if let Err(err) = self.heartbeat_handle.await {
            warn!(target: "codex::mailbox", event = "mailbox.error.heartbeat_task", ?err, "mailbox heartbeat task panicked");
        }

        if let Err(err) = self.registry.remove_entry(&self.session_id).await {
            warn!(target: "codex::mailbox", event = "mailbox.error.registry_remove", ?err, "failed to remove mailbox registry entry");
        }

        sleep(Duration::from_millis(200)).await;

        if let Err(err) = tokio::fs::remove_file(&self.socket_path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(target: "codex::mailbox", event = "mailbox.error.unlink", ?err, socket = %self.socket_path.display(), "failed to unlink mailbox socket");
            }
        }

        Ok(())
    }

    pub async fn handle_event(&self, event: &Event) {
        if let Some(outcome) = classify_event(event) {
            self.events.resolve(&event.id, outcome).await;
        }

        // Append mailbox delivery events to per-conversation spool (best-effort)
        if let EventMsg::MailboxDelivery(delivery) = &event.msg {
            if matches!(delivery.state, MailboxDeliveryState::Enqueued | MailboxDeliveryState::Delivered) {
                if let Some(spool) = &self.spool {
                    if let Err(err) = spool.append_delivery(delivery.clone()).await {
                        warn!(target: "codex::mailbox", event = "mailbox.spool.append_failed", ?err, "failed to append mailbox delivery to spool");
                    }
                }
            }
        }
    }
}

fn mailbox_namespace() -> String {
    std::env::var("CODEX_NAMESPACE").unwrap_or_else(|_| "codex".to_string())
}

fn parse_env_bool(key: &str) -> Option<bool> {
    let raw = std::env::var(key).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "enabled" => Some(true),
        "0" | "false" | "off" | "disabled" => Some(false),
        _ => None,
    }
}

fn mailbox_ipc_enabled() -> bool {
    if let Some(force) = parse_env_bool("CODEX_MAILBOX_ENABLE_FORCE") {
        return force;
    }
    parse_env_bool("CODEX_MAILBOX_ENABLE").unwrap_or(false)
}

fn mailbox_backlog() -> libc::c_int {
    if let Ok(raw) = std::env::var("CODEX_MAILBOX_SOCKET_BACKLOG") {
        if let Ok(value) = raw.trim().parse::<libc::c_int>() {
            if value > 0 {
                return value;
            }
        }
    }
    DEFAULT_SOCKET_BACKLOG
}

fn bind_unix_listener(path: &Path, backlog: libc::c_int) -> Result<UnixListener> {
    let std_listener = StdUnixListener::bind(path)
        .with_context(|| format!("failed to bind unix socket {}", path.display()))?;
    std_listener
        .set_nonblocking(true)
        .context("failed to set mailbox socket nonblocking")?;
    unsafe {
        if libc::listen(std_listener.as_raw_fd(), backlog) != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("failed to listen on {}", path.display()));
        }
    }
    UnixListener::from_std(std_listener).context("failed to convert unix listener to tokio type")
}

async fn run_accept_loop(
    listener: UnixListener,
    conversation: Arc<CodexConversation>,
    events: Arc<MailboxEventRegistry>,
    registry: MailboxRegistry,
    session_id: String,
    shutdown: Arc<Notify>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, _addr)) => {
                        info!(target: "codex::mailbox", event = "mailbox.accept", session_id = %session_id, "accepted mailbox connection");
                        let convo = Arc::clone(&conversation);
                        let events = events.clone();
                        let reg = registry.clone();
                        let session = session_id.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_client(stream, convo, events, reg, session).await {
                                warn!(target: "codex::mailbox", event = "mailbox.error.client", ?err, "mailbox ipc client handler error");
                            }
                        });
                    }
                    Err(err) => {
                        if err.kind() == std::io::ErrorKind::WouldBlock {
                            continue;
                        }
                        warn!(target: "codex::mailbox", event = "mailbox.error.accept", ?err, "mailbox listener accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_heartbeat_loop(registry: MailboxRegistry, session_id: String, shutdown: Arc<Notify>) {
    let mut ticker = interval(HEARTBEAT_INTERVAL);
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            _ = ticker.tick() => {
                let now = OffsetDateTime::now_utc();
                if let Err(err) = registry.update_heartbeat(&session_id, now).await {
                    warn!(target: "codex::mailbox", event = "mailbox.error.heartbeat", ?err, "failed to record mailbox heartbeat");
                }
            }
        }
    }
}

async fn handle_client(
    stream: UnixStream,
    conversation: Arc<CodexConversation>,
    events: Arc<MailboxEventRegistry>,
    registry: MailboxRegistry,
    session_id: String,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    while reader.read_line(&mut line).await? != 0 {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            line.clear();
            continue;
        }
        let response =
            process_envelope(trimmed, Arc::clone(&conversation), Arc::clone(&events)).await;
        let ack = AckResponse::from_outcome(response);
        let payload = serde_json::to_vec(&ack).context("failed to serialize mailbox ack")?;
        write_ack(&mut write_half, &payload).await?;
        if ack.ok {
            let _ = registry
                .update_heartbeat(&session_id, OffsetDateTime::now_utc())
                .await;
        }
        line.clear();
    }
    Ok(())
}

async fn write_ack(writer: &mut OwnedWriteHalf, payload: &[u8]) -> Result<()> {
    writer.write_all(payload).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn process_envelope(
    raw: &str,
    conversation: Arc<CodexConversation>,
    events: Arc<MailboxEventRegistry>,
) -> MailboxOutcome {
    let mut message = match parse_envelope(raw) {
        Ok(msg) => msg,
        Err(err) => return MailboxOutcome::Invalid(err),
    };
    if let Err(err) = validate_message(&mut message) {
        return MailboxOutcome::Invalid(err);
    }

    let submission_id = Uuid::new_v4().to_string();
    let receiver = events.register(submission_id.clone()).await;

    let op = Op::MailboxEnvelope {
        envelope: message.clone(),
    };
    let submission = Submission {
        id: submission_id.clone(),
        op,
    };

    if let Err(err) = conversation.submit_with_id(submission).await {
        events.cancel(&submission_id).await;
        return MailboxOutcome::SubmitFailed(err.to_string());
    }

    match timeout(ACK_TIMEOUT, receiver).await {
        Ok(Ok(MailboxEventOutcome::Enqueued {
            submission_id,
            delivery,
        })) => {
            // Record accept and queue depth gauge per namespace
            #[cfg(feature = "otel")]
            {
                let ns = mailbox_namespace();
                codex_otel::metrics::record_mailbox_accept_total(&ns);
                if let Some(depth) = delivery.queue_depth {
                    codex_otel::metrics::update_mailbox_queue_depth_gauge(&ns, depth as u64);
                }
            }
            MailboxOutcome::Enqueued(submission_id, delivery)
        },
        Ok(Ok(MailboxEventOutcome::QueueFull(capacity))) => MailboxOutcome::QueueFull(capacity),
        Ok(Ok(MailboxEventOutcome::Disabled)) => MailboxOutcome::Disabled,
        Ok(Ok(MailboxEventOutcome::DispatcherClosed)) => MailboxOutcome::DispatcherClosed,
        Ok(Ok(MailboxEventOutcome::Error(message))) => MailboxOutcome::RuntimeError(message),
        Ok(Err(_)) => MailboxOutcome::RuntimeError("ack channel closed".to_string()),
        Err(_) => MailboxOutcome::Timeout,
    }
}

fn parse_envelope(raw: &str) -> Result<MailboxMessage, anyhow::Error> {
    #[derive(Deserialize)]
    struct Wrapped {
        message: MailboxMessage,
    }

    if let Ok(wrapper) = serde_json::from_str::<Wrapped>(raw) {
        return Ok(wrapper.message);
    }
    serde_json::from_str::<MailboxMessage>(raw).map_err(|e| e.into())
}

fn validate_message(message: &mut MailboxMessage) -> Result<(), anyhow::Error> {
    apply_mailbox_defaults(message);
    validate_mailbox_message(message)
}

#[derive(Debug)]
enum MailboxOutcome {
    Enqueued(String, MailboxDeliveryEvent),
    QueueFull(Option<usize>),
    Disabled,
    DispatcherClosed,
    RuntimeError(String),
    Invalid(anyhow::Error),
    SubmitFailed(String),
    Timeout,
}

#[derive(Serialize)]
struct AckResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    submission_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity: Option<usize>,
}

impl AckResponse {
    fn from_outcome(outcome: MailboxOutcome) -> Self {
        match outcome {
            MailboxOutcome::Enqueued(submission_id, delivery) => {
                info!(
                    target: "codex::mailbox",
                    event = "mailbox.ack",
                    message_id = %delivery.message.message_id,
                    submission_id = %submission_id,
                    queue_depth = ?delivery.queue_depth,
                    "mailbox envelope accepted"
                );
                AckResponse {
                    ok: true,
                    submission_id: Some(submission_id),
                    message_id: Some(delivery.message.message_id),
                    queue_depth: delivery.queue_depth,
                    err: None,
                    detail: None,
                    capacity: None,
                }
            }
            MailboxOutcome::QueueFull(capacity) => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "enqueue_full");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("queue_full".to_string()),
                    detail: Some("Mailbox queue is full".to_string()),
                    capacity,
                }
            }
            MailboxOutcome::Disabled => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "disabled");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("disabled".to_string()),
                    detail: Some("Mailbox dispatcher disabled".to_string()),
                    capacity: None,
                }
            }
            MailboxOutcome::DispatcherClosed => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "dispatcher_closed");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("closed".to_string()),
                    detail: Some("Mailbox dispatcher unavailable".to_string()),
                    capacity: None,
                }
            }
            MailboxOutcome::RuntimeError(msg) => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "runtime_error");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("error".to_string()),
                    detail: Some(msg),
                    capacity: None,
                }
            }
            MailboxOutcome::Invalid(err) => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "invalid_message");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("invalid_message".to_string()),
                    detail: Some(err.to_string()),
                    capacity: None,
                }
            }
            MailboxOutcome::SubmitFailed(msg) => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "submit_failed");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("submit_failed".to_string()),
                    detail: Some(msg),
                    capacity: None,
                }
            }
            MailboxOutcome::Timeout => {
                #[cfg(feature = "otel")]
                {
                    let ns = mailbox_namespace();
                    codex_otel::metrics::record_mailbox_error_total(&ns, "ack_timeout");
                }
                AckResponse {
                    ok: false,
                    submission_id: None,
                    message_id: None,
                    queue_depth: None,
                    err: Some("timeout".to_string()),
                    detail: Some("Timed out waiting for mailbox acknowledgement".to_string()),
                    capacity: None,
                }
            }
        }
    }
}

#[derive(Default)]
struct MailboxEventRegistry {
    waiters: Mutex<BTreeMap<String, Vec<tokio::sync::oneshot::Sender<MailboxEventOutcome>>>>,
}

impl MailboxEventRegistry {
    async fn register(
        &self,
        submission_id: String,
    ) -> tokio::sync::oneshot::Receiver<MailboxEventOutcome> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut waiters = self.waiters.lock().await;
        waiters.entry(submission_id).or_default().push(tx);
        rx
    }

    async fn cancel(&self, submission_id: &str) {
        let mut waiters = self.waiters.lock().await;
        waiters.remove(submission_id);
    }

    async fn resolve(&self, submission_id: &str, outcome: MailboxEventOutcome) {
        let mut waiters = self.waiters.lock().await;
        if let Some(listeners) = waiters.remove(submission_id) {
            for tx in listeners {
                let _ = tx.send(outcome.clone());
            }
        }
    }
}

#[derive(Clone)]
enum MailboxEventOutcome {
    Enqueued {
        submission_id: String,
        delivery: MailboxDeliveryEvent,
    },
    QueueFull(Option<usize>),
    Disabled,
    DispatcherClosed,
    Error(String),
}

fn classify_event(event: &Event) -> Option<MailboxEventOutcome> {
    match &event.msg {
        EventMsg::MailboxDelivery(delivery)
            if matches!(
                delivery.state,
                MailboxDeliveryState::Enqueued | MailboxDeliveryState::Delivered
            ) =>
        {
            Some(MailboxEventOutcome::Enqueued {
                submission_id: event.id.clone(),
                delivery: delivery.clone(),
            })
        }
        EventMsg::Error(err) => classify_error_message(&err.message),
        EventMsg::BackgroundEvent(BackgroundEventEvent { message }) => {
            if message.contains("Mailbox dispatcher disabled") {
                Some(MailboxEventOutcome::Disabled)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn classify_error_message(message: &str) -> Option<MailboxEventOutcome> {
    if message.contains("Mailbox queue is full") {
        let capacity = extract_capacity(message);
        Some(MailboxEventOutcome::QueueFull(capacity))
    } else if message.contains("Mailbox dispatcher unavailable") {
        Some(MailboxEventOutcome::DispatcherClosed)
    } else if message.contains("Mailbox dispatcher disabled") {
        Some(MailboxEventOutcome::Disabled)
    } else {
        Some(MailboxEventOutcome::Error(message.to_string()))
    }
}

fn extract_capacity(message: &str) -> Option<usize> {
    let start = message.find("capacity = ")? + "capacity = ".len();
    let rest = &message[start..];
    let end = rest.find(')')?;
    rest[..end].trim().parse().ok()
}

#[derive(Clone, Serialize, Deserialize)]
struct RegistryEntry {
    session_id: String,
    pid: u32,
    socket_path: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    last_heartbeat: OffsetDateTime,
    namespace: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct RegistryFile {
    version: u32,
    entries: BTreeMap<String, RegistryEntry>,
}

#[derive(Clone)]
struct MailboxRegistry {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl MailboxRegistry {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            lock: Arc::new(Mutex::new(())),
        }
    }

    async fn upsert_entry(&self, entry: RegistryEntry) -> Result<()> {
        let _guard = self.lock.lock().await;
        let _flock = acquire_registry_lock(&self.path)?;
        let mut registry = self.load().await?;
        registry.version = REGISTRY_VERSION;
        registry.entries.insert(entry.session_id.clone(), entry);
        self.write(&registry).await
    }

    async fn update_heartbeat(&self, session_id: &str, ts: OffsetDateTime) -> Result<()> {
        let _guard = self.lock.lock().await;
        let _flock = acquire_registry_lock(&self.path)?;
        let mut registry = self.load().await?;
        if let Some(entry) = registry.entries.get_mut(session_id) {
            entry.last_heartbeat = ts;
            self.write(&registry).await?;
        }
        Ok(())
    }

    async fn remove_entry(&self, session_id: &str) -> Result<()> {
        let _guard = self.lock.lock().await;
        let _flock = acquire_registry_lock(&self.path)?;
        let mut registry = self.load().await?;
        registry.entries.remove(session_id);
        self.write(&registry).await
    }

    async fn sweep_stale_entries(&self, max_age: Duration) -> Result<()> {
        let _guard = self.lock.lock().await;
        let _flock = acquire_registry_lock(&self.path)?;
        let mut registry = self.load().await?;
        let now = OffsetDateTime::now_utc();
        let mut removed = Vec::new();
        registry.entries.retain(|session, entry| {
            let mut keep = true;
            if !pid_alive(entry.pid) {
                keep = false;
            } else if !Path::new(&entry.socket_path).exists() {
                keep = false;
            } else if now - entry.last_heartbeat > max_age {
                keep = false;
            }
            if !keep {
                removed.push((session.clone(), entry.socket_path.clone()));
            }
            keep
        });

        if !removed.is_empty() {
            for (_, socket_path) in &removed {
                if let Err(err) = fs::remove_file(socket_path) {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        debug!(?err, socket = %socket_path, "failed to remove stale mailbox socket");
                    }
                }
            }
            registry.version = REGISTRY_VERSION;
            self.write(&registry).await?;
        }
        Ok(())
    }

    async fn load(&self) -> Result<RegistryFile> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || read_registry(&path))
            .await
            .context("join registry read")?
    }

    async fn write(&self, registry: &RegistryFile) -> Result<()> {
        let path = self.path.clone();
        let data = registry.clone();
        tokio::task::spawn_blocking(move || write_registry(&path, &data))
            .await
            .context("join registry write")?
    }
}

fn unix_socket_path_len(path: &Path) -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().len()
    }
    #[cfg(not(target_os = "linux"))]
    {
        path.as_os_str().to_string_lossy().len()
    }
}

fn truncated_socket_name(session_id: &str) -> String {
    let compact: String = session_id.chars().filter(|c| *c != '-').take(12).collect();
    if compact.is_empty() {
        session_id.chars().take(12).collect()
    } else {
        compact
    }
}

fn resolve_socket_path(default_dir: &Path, namespace: &str, session_id: &str) -> Result<PathBuf> {
    let original = default_dir.join(format!("{session_id}.sock"));
    if unix_socket_path_len(&original) < UNIX_SOCKET_PATH_LIMIT {
        return Ok(original);
    }
    warn!(target: "codex::mailbox", event = "mailbox.socket_path.too_long", path = %original.display(), limit = UNIX_SOCKET_PATH_LIMIT, "mailbox socket path exceeds system limit; attempting fallback");

    let mut attempts = Vec::new();
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        attempts.push((
            PathBuf::from(runtime)
                .join("cx")
                .join(namespace)
                .join("mailbox"),
            "xdg_runtime",
        ));
    }
    attempts.push((
        std::env::temp_dir()
            .join("codex-mailbox")
            .join(namespace)
            .join("mailbox"),
        "temp_dir",
    ));

    let short_name = truncated_socket_name(session_id);
    for (dir, label) in attempts {
        let candidate = dir.join(format!("{short_name}.sock"));
        if unix_socket_path_len(&candidate) >= UNIX_SOCKET_PATH_LIMIT {
            continue;
        }
        if let Err(err) = fs::create_dir_all(&dir) {
            warn!(target: "codex::mailbox", event = "mailbox.socket_path.dir_create_failed", ?err, path = %dir.display(), "failed to create fallback mailbox directory");
            continue;
        }
        info!(target: "codex::mailbox", event = "mailbox.socket_path.fallback", original = %original.display(), fallback = %candidate.display(), strategy = label, "using shortened mailbox socket path");
        return Ok(candidate);
    }

    Err(anyhow!(
        "mailbox socket path {} exceeds limit {} and no fallback directory succeeded",
        original.display(),
        UNIX_SOCKET_PATH_LIMIT
    ))
}

fn pid_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

fn read_registry(path: &Path) -> Result<RegistryFile> {
    if !path.exists() {
        return Ok(RegistryFile {
            version: REGISTRY_VERSION,
            entries: BTreeMap::new(),
        });
    }
    let data =
        fs::read(path).with_context(|| format!("failed to read registry {}", path.display()))?;
    if data.is_empty() {
        return Ok(RegistryFile {
            version: REGISTRY_VERSION,
            entries: BTreeMap::new(),
        });
    }
    let parsed = serde_json::from_slice::<RegistryFile>(&data)
        .with_context(|| format!("failed to parse registry {}", path.display()))?;
    Ok(parsed)
}

fn write_registry(path: &Path, registry: &RegistryFile) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("registry path {} missing parent", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create registry parent {}", parent.display()))?;
    // Use a unique temporary path in the same directory to avoid
    // cross-process clobbering when multiple writers are active.
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("registry path {} missing filename", path.display()))?
        .to_string_lossy()
        .into_owned();
    let tmp_path = parent.join(format!("{}.tmp-{}", file_name, Uuid::new_v4()));
    let serialized = serde_json::to_vec_pretty(registry)?;
    let mut file = fs::File::create(&tmp_path)
        .with_context(|| format!("failed to create temp registry {}", tmp_path.display()))?;
    file.write_all(&serialized)
        .with_context(|| format!("failed to write temp registry {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync temp registry {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("failed to replace registry {}", path.display()))?;
    Ok(())
}

fn acquire_registry_lock(registry_path: &Path) -> Result<File> {
    let file_name = registry_path
        .file_name()
        .ok_or_else(|| anyhow!("registry path {} missing filename", registry_path.display()))?
        .to_string_lossy();
    let lock_path = registry_path
        .with_file_name(format!("{}.lock", file_name));
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create lock parent {}", parent.display()))?;
    }
    let file = File::options()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open lock file {}", lock_path.display()))?;
    file.lock_exclusive()
        .with_context(|| format!("lock {}", lock_path.display()))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registry_concurrent_upserts_are_atomic_and_merged() -> Result<()> {
        // Run several rounds to shake out flakes
        for _ in 0..10 {
            let dir = TempDir::new().context("create temp dir")?;
            let path = dir.path().join("registry.json");
            let r1 = MailboxRegistry::new(path.clone());
            let r2 = MailboxRegistry::new(path.clone());

            let now = OffsetDateTime::now_utc();
            let e1 = RegistryEntry {
                session_id: "s1".into(),
                pid: std::process::id(),
                socket_path: dir.path().join("s1.sock").to_string_lossy().into_owned(),
                created_at: now,
                last_heartbeat: now,
                namespace: "test".into(),
            };
            let e2 = RegistryEntry {
                session_id: "s2".into(),
                pid: std::process::id(),
                socket_path: dir.path().join("s2.sock").to_string_lossy().into_owned(),
                created_at: now,
                last_heartbeat: now,
                namespace: "test".into(),
            };

            // Concurrent writers simulating separate processes (distinct MailboxRegistry instances)
            let t1 = tokio::spawn(async move {
                for _ in 0..5 {
                    // small jitter
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    r1.upsert_entry(e1.clone()).await.unwrap();
                }
            });
            let t2 = tokio::spawn(async move {
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    r2.upsert_entry(e2.clone()).await.unwrap();
                }
            });

            let _ = tokio::join!(t1, t2);

            // Validate both entries are present
            let reg = MailboxRegistry::new(path);
            let file = reg.load().await?;
            assert!(file.entries.contains_key("s1"), "missing s1 entry");
            assert!(file.entries.contains_key("s2"), "missing s2 entry");
        }
        Ok(())
    }
}
