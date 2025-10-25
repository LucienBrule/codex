use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use async_nats::{Client, ConnectOptions, Event, RequestError, RequestErrorKind};
use async_trait::async_trait;
use bytes::Bytes;
use codex_protocol::mailbox::MailboxMessage;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::pin;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{DeliveryBackendKind, MailServerConfig};
use crate::registry::{RegistryRecord, RegistryWatcher};

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
struct DispatchRequest {
    submission_id: String,
    source_conversation_id: Uuid,
    target_conversation_id: Uuid,
    message: MailboxMessage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DispatchStatus {
    Delivered,
    QueueFull,
    DispatcherClosed,
    Disabled,
    Timeout,
    UnknownSession,
    TransportError,
    InvalidRequest,
}

#[derive(Debug, Serialize)]
struct DispatchResponse {
    status: DispatchStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity: Option<usize>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
struct MailboxAckPayload {
    ok: bool,
    #[serde(default)]
    submission_id: Option<String>,
    #[serde(default)]
    message_id: Option<Uuid>,
    #[serde(default)]
    queue_depth: Option<usize>,
    #[serde(default)]
    err: Option<String>,
    #[serde(default)]
    detail: Option<String>,
    #[serde(default)]
    capacity: Option<usize>,
}

fn interpret_ack_payload(ack: MailboxAckPayload) -> Result<MailboxAckPayload, DeliveryError> {
    if ack.ok {
        return Ok(ack);
    }

    match ack.err.as_deref() {
        Some("queue_full") => Err(DeliveryError::QueueFull {
            capacity: ack.capacity,
            detail: ack.detail.clone(),
        }),
        Some("disabled") => Err(DeliveryError::Disabled(ack.detail.clone())),
        Some("closed") => Err(DeliveryError::DispatcherClosed(ack.detail.clone())),
        Some("unknown_session") => Err(DeliveryError::UnknownSession(ack.detail.clone())),
        Some("timeout") => Err(DeliveryError::AckTimeout),
        Some(other) => Err(DeliveryError::Io {
            err: std::io::Error::other(other.to_string()),
            detail: ack.detail.clone(),
        }),
        None => Err(DeliveryError::Io {
            err: std::io::Error::other("dispatcher returned failure without err field"),
            detail: ack.detail.clone(),
        }),
    }
}

fn ack_from_delivery_error(error: &DeliveryError) -> MailboxAckPayload {
    match error {
        DeliveryError::QueueFull { capacity, detail } => MailboxAckPayload {
            ok: false,
            submission_id: None,
            message_id: None,
            queue_depth: None,
            err: Some("queue_full".to_string()),
            detail: detail.clone(),
            capacity: *capacity,
        },
        DeliveryError::DispatcherClosed(detail) => MailboxAckPayload {
            ok: false,
            submission_id: None,
            message_id: None,
            queue_depth: None,
            err: Some("closed".to_string()),
            detail: detail.clone(),
            capacity: None,
        },
        DeliveryError::Disabled(detail) => MailboxAckPayload {
            ok: false,
            submission_id: None,
            message_id: None,
            queue_depth: None,
            err: Some("disabled".to_string()),
            detail: detail.clone(),
            capacity: None,
        },
        DeliveryError::AckTimeout => MailboxAckPayload {
            ok: false,
            submission_id: None,
            message_id: None,
            queue_depth: None,
            err: Some("timeout".to_string()),
            detail: Some("timed out waiting for acknowledgement".to_string()),
            capacity: None,
        },
        DeliveryError::Io { err, detail } => MailboxAckPayload {
            ok: false,
            submission_id: None,
            message_id: None,
            queue_depth: None,
            err: Some("transport_error".to_string()),
            detail: Some(detail.clone().unwrap_or_else(|| err.to_string())),
            capacity: None,
        },
        DeliveryError::UnknownSession(detail) => MailboxAckPayload {
            ok: false,
            submission_id: None,
            message_id: None,
            queue_depth: None,
            err: Some("unknown_session".to_string()),
            detail: detail.clone(),
            capacity: None,
        },
    }
}

fn request_error_reason(kind: RequestErrorKind) -> &'static str {
    match kind {
        RequestErrorKind::TimedOut => "timeout",
        RequestErrorKind::NoResponders => "no_responders",
        RequestErrorKind::Other => "other",
    }
}

fn map_request_error(err: RequestError) -> DeliveryError {
    match err.kind() {
        RequestErrorKind::TimedOut => {
            DeliveryError::UnknownSession(Some("broker request timed out".to_string()))
        }
        RequestErrorKind::NoResponders => {
            DeliveryError::UnknownSession(Some("no broker responders present".to_string()))
        }
        RequestErrorKind::Other => DeliveryError::Io {
            err: std::io::Error::other(err.to_string()),
            detail: Some("broker request failed".to_string()),
        },
    }
}

#[derive(Debug)]
enum DeliveryError {
    QueueFull {
        capacity: Option<usize>,
        detail: Option<String>,
    },
    DispatcherClosed(Option<String>),
    Disabled(Option<String>),
    AckTimeout,
    Io {
        err: std::io::Error,
        detail: Option<String>,
    },
    UnknownSession(Option<String>),
}

#[async_trait]
trait DeliveryBackend: Send + Sync {
    async fn send(
        &self,
        request: &DispatchRequest,
        entry: Option<&RegistryRecord>,
    ) -> Result<MailboxAckPayload, DeliveryError>;
}

#[derive(Clone)]
struct UnixSocketBackend {
    connect_timeout: Duration,
    ack_timeout: Duration,
}

impl UnixSocketBackend {
    fn new(config: &MailServerConfig) -> Self {
        Self {
            connect_timeout: config.connect_timeout,
            ack_timeout: config.ack_timeout,
        }
    }
}

#[async_trait]
impl DeliveryBackend for UnixSocketBackend {
    async fn send(
        &self,
        request: &DispatchRequest,
        entry: Option<&RegistryRecord>,
    ) -> Result<MailboxAckPayload, DeliveryError> {
        let entry = match entry {
            Some(entry) => entry,
            None => {
                return Err(DeliveryError::UnknownSession(Some(
                    "target conversation not registered locally".to_string(),
                )));
            }
        };
        let socket_path = &entry.socket_path;
        let connect = UnixStream::connect(socket_path);
        let mut stream = match timeout(self.connect_timeout, connect).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                return Err(DeliveryError::Io {
                    err,
                    detail: Some("failed to connect to mailbox socket".to_string()),
                });
            }
            Err(_) => {
                return Err(DeliveryError::Io {
                    err: std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"),
                    detail: Some("connecting to mailbox socket timed out".to_string()),
                });
            }
        };

        let mut payload =
            serde_json::to_vec(&request.message).map_err(|err| DeliveryError::Io {
                err: std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string()),
                detail: Some("failed to serialize mailbox message".to_string()),
            })?;
        payload.push(b'\n');

        stream
            .write_all(&payload)
            .await
            .map_err(|err| DeliveryError::Io {
                err,
                detail: Some("failed to write mailbox payload".to_string()),
            })?;
        stream.flush().await.map_err(|err| DeliveryError::Io {
            err,
            detail: Some("failed to flush mailbox payload".to_string()),
        })?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        let bytes = timeout(self.ack_timeout, reader.read_line(&mut line))
            .await
            .map_err(|_| DeliveryError::AckTimeout)?
            .map_err(|err| DeliveryError::Io {
                err,
                detail: Some("failed to read mailbox acknowledgement".to_string()),
            })?;

        if bytes == 0 {
            return Err(DeliveryError::DispatcherClosed(Some(
                "mailbox listener closed connection without acknowledgement".to_string(),
            )));
        }

        let ack: MailboxAckPayload =
            serde_json::from_str(line.trim()).map_err(|err| DeliveryError::Io {
                err: std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()),
                detail: Some("failed to parse mailbox acknowledgement".to_string()),
            })?;

        interpret_ack_payload(ack)
    }
}

#[derive(Clone)]
struct NatsBackend {
    inner: Arc<NatsInner>,
}

struct NatsInner {
    client: Client,
    namespace: String,
    subject_prefix: String,
    request_timeout: Duration,
    local_backend: UnixSocketBackend,
}

struct BrokerRuntime {
    handles: Vec<JoinHandle<()>>,
}

impl BrokerRuntime {
    fn new(handles: Vec<JoinHandle<()>>) -> Self {
        Self { handles }
    }
}

impl Drop for BrokerRuntime {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

impl NatsBackend {
    async fn connect(
        config: &MailServerConfig,
        registry: RegistryWatcher,
    ) -> Result<(Self, BrokerRuntime)> {
        let broker = config
            .broker
            .as_ref()
            .context("nats backend requires broker configuration")?;

        let namespace = config.namespace.clone();
        let namespace_for_events = Arc::new(namespace.clone());
        let subject_prefix = broker.subject_prefix.clone();
        let request_timeout = broker.request_timeout;
        let local_backend = UnixSocketBackend::new(config);

        let mut options = ConnectOptions::new()
            .name(format!("codex-mail-server-{}", namespace))
            .retry_on_initial_connect()
            .request_timeout(Some(request_timeout))
            .max_reconnects(None);

        if let Some(token) = &broker.token {
            options = options.token(token.clone());
        }

        let events_namespace = namespace_for_events.clone();
        options = options.event_callback(move |event| {
            let ns = events_namespace.clone();
            async move {
                match event {
                    Event::Connected => {
                        info!(
                            target: "codex::mailbox",
                            event = "broker.connected",
                            namespace = %ns,
                            "connected to NATS broker"
                        );
                        #[cfg(feature = "otel")]
                        codex_otel::metrics::record_mailbox_broker_connected(ns.as_str());
                    }
                    Event::Disconnected => {
                        warn!(
                            target: "codex::mailbox",
                            event = "broker.disconnected",
                            namespace = %ns,
                            "lost connection to NATS broker; retrying"
                        );
                        #[cfg(feature = "otel")]
                        codex_otel::metrics::record_mailbox_broker_disconnected(ns.as_str());
                    }
                    Event::LameDuckMode => {
                        warn!(
                            target: "codex::mailbox",
                            event = "broker.lame_duck",
                            namespace = %ns,
                            "broker entered lame duck mode"
                        );
                    }
                    Event::Draining => {
                        info!(
                            target: "codex::mailbox",
                            event = "broker.draining",
                            namespace = %ns,
                            "broker drain requested"
                        );
                    }
                    Event::Closed => {
                        warn!(
                            target: "codex::mailbox",
                            event = "broker.closed",
                            namespace = %ns,
                            "broker connection closed"
                        );
                    }
                    Event::SlowConsumer(sid) => {
                        warn!(
                            target: "codex::mailbox",
                            event = "broker.slow_consumer",
                            namespace = %ns,
                            sid,
                            "slow consumer detected on broker subscription"
                        );
                    }
                    Event::ServerError(err) => {
                        error!(
                            target: "codex::mailbox",
                            event = "broker.server_error",
                            namespace = %ns,
                            error = %err,
                            "server-side broker error observed"
                        );
                    }
                    Event::ClientError(err) => {
                        error!(
                            target: "codex::mailbox",
                            event = "broker.client_error",
                            namespace = %ns,
                            error = %err,
                            "client-side broker error observed"
                        );
                        #[cfg(feature = "otel")]
                        codex_otel::metrics::record_mailbox_broker_publish_failed(
                            ns.as_str(),
                            "client_error",
                        );
                    }
                }
            }
        });

        let client = options
            .connect(&broker.url)
            .await
            .with_context(|| format!("failed to connect to NATS broker at {}", broker.url))?;

        info!(
            target: "codex::mailbox",
            event = "broker.connect.success",
            namespace = %namespace,
            url = %broker.url,
            "nats backend connected"
        );

        let inner = Arc::new(NatsInner {
            client: client.clone(),
            namespace: namespace.clone(),
            subject_prefix: subject_prefix.clone(),
            request_timeout,
            local_backend,
        });

        let consumer_handle = spawn_nats_consumer(inner.clone(), registry);
        let runtime = BrokerRuntime::new(vec![consumer_handle]);

        Ok((Self { inner }, runtime))
    }

    fn subject_for(&self, conversation_id: &Uuid) -> String {
        format!(
            "{}.{}.{}",
            self.inner.subject_prefix, self.inner.namespace, conversation_id
        )
    }
}

fn spawn_nats_consumer(inner: Arc<NatsInner>, registry: RegistryWatcher) -> JoinHandle<()> {
    tokio::spawn(async move {
        let subject = format!("{}.{}.*", inner.subject_prefix, inner.namespace);
        match inner.client.subscribe(subject.clone()).await {
            Ok(mut subscription) => {
                info!(
                    target: "codex::mailbox",
                    event = "broker.consumer.ready",
                    namespace = %inner.namespace,
                    subject = %subject,
                    "broker consumer subscribed"
                );
                while let Some(message) = subscription.next().await {
                    let registry_clone = registry.clone();
                    let inner_clone = inner.clone();
                    if let Err(err) =
                        process_broker_message(inner_clone, registry_clone, message).await
                    {
                        warn!(
                            target: "codex::mailbox",
                            event = "broker.consumer.error",
                            namespace = %inner.namespace,
                            ?err,
                            "failed to process broker message"
                        );
                    }
                }
            }
            Err(err) => {
                error!(
                    target: "codex::mailbox",
                    event = "broker.consumer.subscribe_error",
                    namespace = %inner.namespace,
                    subject = %subject,
                    ?err,
                    "failed to subscribe to broker subject"
                );
            }
        }
    })
}

async fn process_broker_message(
    inner: Arc<NatsInner>,
    registry: RegistryWatcher,
    message: async_nats::Message,
) -> Result<()> {
    let subject = message.subject.clone();
    let reply = message.reply.clone();
    let payload = message.payload.clone();
    let prefix = format!("{}.{}.", inner.subject_prefix, inner.namespace);
    if !subject.starts_with(&prefix) {
        debug!(
            target: "codex::mailbox",
            event = "broker.consumer.subject_mismatch",
            namespace = %inner.namespace,
            subject = %subject,
            "ignoring broker message for unrelated subject"
        );
        return Ok(());
    }

    let id_segment = &subject[prefix.len()..];
    let target_conversation_id = match Uuid::parse_str(id_segment) {
        Ok(id) => id,
        Err(err) => {
            warn!(
                target: "codex::mailbox",
                event = "broker.consumer.invalid_subject",
                namespace = %inner.namespace,
                subject = %subject,
                ?err,
                "broker subject missing valid conversation id"
            );
            return Ok(());
        }
    };

    let request: DispatchRequest = match serde_json::from_slice(&payload) {
        Ok(request) => request,
        Err(err) => {
            warn!(
                target: "codex::mailbox",
                event = "broker.consumer.deser_error",
                namespace = %inner.namespace,
                subject = %subject,
                ?err,
                "failed to deserialize broker payload"
            );
            return Ok(());
        }
    };

    let entry = match registry.lookup(&target_conversation_id) {
        Some(entry) => entry,
        None => {
            debug!(
                target: "codex::mailbox",
                event = "broker.consumer.missing_registry",
                namespace = %inner.namespace,
                subject = %subject,
                conversation = %target_conversation_id,
                "no local registry entry; leaving message for other consumers"
            );
            return Ok(());
        }
    };

    #[cfg(feature = "otel")]
    codex_otel::metrics::update_mailbox_broker_inflight(inner.namespace.as_str(), 1);

    let started = Instant::now();
    let send_result = inner.local_backend.send(&request, Some(&entry)).await;

    #[cfg(feature = "otel")]
    codex_otel::metrics::update_mailbox_broker_inflight(inner.namespace.as_str(), -1);

    let ack = match send_result {
        Ok(mut ack) => {
            if ack.submission_id.is_none() {
                ack.submission_id = Some(request.submission_id.clone());
            }
            #[cfg(feature = "otel")]
            {
                codex_otel::metrics::record_mailbox_broker_delivery_total(
                    inner.namespace.as_str(),
                    &target_conversation_id.to_string(),
                );
                codex_otel::metrics::record_mailbox_broker_publish_latency(
                    inner.namespace.as_str(),
                    started.elapsed().as_millis() as u64,
                );
            }
            ack
        }
        Err(err) => {
            #[cfg(feature = "otel")]
            codex_otel::metrics::record_mailbox_broker_publish_failed(
                inner.namespace.as_str(),
                "local_delivery_failed",
            );
            ack_from_delivery_error(&err)
        }
    };

    if let Some(reply_subject) = reply {
        let buf = match serde_json::to_vec(&ack) {
            Ok(buf) => buf,
            Err(err) => {
                error!(
                    target: "codex::mailbox",
                    event = "broker.consumer.serialize_ack_error",
                    namespace = %inner.namespace,
                    ?err,
                    "failed to serialize broker acknowledgement"
                );
                return Ok(());
            }
        };
        if let Err(err) = inner.client.publish(reply_subject, Bytes::from(buf)).await {
            error!(
                target: "codex::mailbox",
                event = "broker.consumer.respond_error",
                namespace = %inner.namespace,
                ?err,
                "failed to publish broker acknowledgement"
            );
        }
    }

    Ok(())
}

#[async_trait]
impl DeliveryBackend for NatsBackend {
    async fn send(
        &self,
        request: &DispatchRequest,
        entry: Option<&RegistryRecord>,
    ) -> Result<MailboxAckPayload, DeliveryError> {
        if let Some(entry) = entry {
            return self.inner.local_backend.send(request, Some(entry)).await;
        }

        let payload = serde_json::to_vec(request).map_err(|err| DeliveryError::Io {
            err: std::io::Error::other(err.to_string()),
            detail: Some("failed to serialize broker payload".to_string()),
        })?;
        let subject = self.subject_for(&request.target_conversation_id);

        #[cfg(feature = "otel")]
        codex_otel::metrics::record_mailbox_broker_publish_total(
            self.inner.namespace.as_str(),
            &request.target_conversation_id.to_string(),
        );
        #[cfg(feature = "otel")]
        codex_otel::metrics::update_mailbox_broker_inflight(self.inner.namespace.as_str(), 1);

        let started = Instant::now();
        let response = timeout(
            self.inner.request_timeout,
            self.inner.client.request(subject, Bytes::from(payload)),
        )
        .await;

        let message = match response {
            Ok(Ok(message)) => {
                #[cfg(feature = "otel")]
                codex_otel::metrics::update_mailbox_broker_inflight(
                    self.inner.namespace.as_str(),
                    -1,
                );
                message
            }
            Ok(Err(err)) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::update_mailbox_broker_inflight(
                        self.inner.namespace.as_str(),
                        -1,
                    );
                    codex_otel::metrics::record_mailbox_broker_publish_failed(
                        self.inner.namespace.as_str(),
                        request_error_reason(err.kind()),
                    );
                }
                return Err(map_request_error(err));
            }
            Err(_) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::update_mailbox_broker_inflight(
                        self.inner.namespace.as_str(),
                        -1,
                    );
                    codex_otel::metrics::record_mailbox_broker_publish_failed(
                        self.inner.namespace.as_str(),
                        "timeout",
                    );
                }
                return Err(DeliveryError::UnknownSession(Some(
                    "no remote mailbox responded via broker".to_string(),
                )));
            }
        };

        #[cfg(feature = "otel")]
        codex_otel::metrics::record_mailbox_broker_publish_latency(
            self.inner.namespace.as_str(),
            started.elapsed().as_millis() as u64,
        );

        let ack: MailboxAckPayload =
            serde_json::from_slice(&message.payload).map_err(|err| DeliveryError::Io {
                err: std::io::Error::other(err.to_string()),
                detail: Some("failed to deserialize broker acknowledgement".to_string()),
            })?;

        interpret_ack_payload(ack)
    }
}

pub struct MailDispatcherServer {
    config: MailServerConfig,
    registry: RegistryWatcher,
    semaphore: Arc<Semaphore>,
    backend: Arc<dyn DeliveryBackend>,
    _broker_runtime: Option<BrokerRuntime>,
}

impl MailDispatcherServer {
    pub async fn new(config: MailServerConfig, registry: RegistryWatcher) -> Result<Self> {
        let semaphore = Arc::new(Semaphore::new(config.max_inflight));
        let (backend, broker_runtime): (Arc<dyn DeliveryBackend>, Option<BrokerRuntime>) =
            match config.delivery_backend {
                DeliveryBackendKind::UnixSocket => {
                    (Arc::new(UnixSocketBackend::new(&config)), None)
                }
                DeliveryBackendKind::Nats => {
                    let (backend, runtime) =
                        NatsBackend::connect(&config, registry.clone()).await?;
                    (Arc::new(backend), Some(runtime))
                }
            };

        Ok(Self {
            config,
            registry,
            semaphore,
            backend,
            _broker_runtime: broker_runtime,
        })
    }

    pub async fn run(self) -> Result<()> {
        self.config.ensure_socket_parent()?;
        if self.config.socket_path.exists() {
            tokio::fs::remove_file(&self.config.socket_path).await.ok();
        }

        let listener = UnixListener::bind(&self.config.socket_path).with_context(|| {
            format!(
                "failed to bind dispatcher socket {}",
                self.config.socket_path.display()
            )
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(
                &self.config.socket_path,
                std::fs::Permissions::from_mode(0o660),
            )
            .await
            .ok();
        }

        info!(
            target: "codex::mailbox",
            event = "dispatcher.start",
            namespace = %self.config.namespace,
            socket = %self.config.socket_path.display(),
            registry = %self.config.registry_path.display(),
            max_inflight = self.config.max_inflight,
            "codex mail dispatcher starting"
        );

        let socket_path = self.config.socket_path.clone();
        let semaphore = self.semaphore.clone();
        let registry = self.registry.clone();
        let config = self.config.clone();
        let backend = self.backend.clone();

        let shutdown = tokio::signal::ctrl_c();
        pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!(target: "codex::mailbox", event = "dispatcher.shutdown", "received shutdown signal");
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            let permit = semaphore.clone().acquire_owned().await;
                            let registry = registry.clone();
                            let config = config.clone();
                            let backend = backend.clone();
                            tokio::spawn(async move {
                                if let Ok(permit) = permit {
                                    if let Err(err) =
                                        handle_connection(stream, registry, config, backend).await
                                    {
                                        error!(target: "codex::mailbox", event = "dispatcher.connection_error", ?err, "connection handling failed");
                                    }
                                    drop(permit);
                                }
                            });
                        }
                        Err(err) => {
                            error!(target: "codex::mailbox", event = "dispatcher.accept_error", ?err, "failed to accept dispatcher connection");
                            sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
        }

        tokio::fs::remove_file(socket_path).await.ok();
        Ok(())
    }
}

async fn handle_connection(
    stream: UnixStream,
    registry: RegistryWatcher,
    config: MailServerConfig,
    backend: Arc<dyn DeliveryBackend>,
) -> Result<()> {
    let peer = stream.peer_addr().ok();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .await
        .context("failed to read dispatcher request")?;
    if bytes == 0 {
        return Ok(());
    }

    let request: DispatchRequest = match serde_json::from_str(line.trim()) {
        Ok(req) => req,
        Err(err) => {
            warn!(target: "codex::mailbox", event = "dispatcher.invalid_request", ?err, "failed to parse dispatcher request");
            respond(
                reader.into_inner(),
                &DispatchResponse {
                    status: DispatchStatus::InvalidRequest,
                    queue_depth: None,
                    detail: Some(format!("invalid request payload: {err}")),
                    capacity: None,
                },
            )
            .await?;
            return Ok(());
        }
    };

    if request.source_conversation_id == request.target_conversation_id {
        warn!(
            target: "codex::mailbox",
            event = "dispatcher.self_loop",
            conversation_id = %request.source_conversation_id,
            "dispatcher received self-loop request; dropping"
        );
        respond(
            reader.into_inner(),
            &DispatchResponse {
                status: DispatchStatus::InvalidRequest,
                queue_depth: None,
                detail: Some("self-loop delivery prohibited".to_string()),
                capacity: None,
            },
        )
        .await?;
        return Ok(());
    }

    let registry_entry = registry.lookup(&request.target_conversation_id);

    if registry_entry.is_none()
        && matches!(config.delivery_backend, DeliveryBackendKind::UnixSocket)
    {
        warn!(
            target: "codex::mailbox",
            event = "dispatcher.unknown_target",
            source = %request.source_conversation_id,
            target = %request.target_conversation_id,
            "target conversation not present in registry"
        );
        respond(
            reader.into_inner(),
            &DispatchResponse {
                status: DispatchStatus::UnknownSession,
                queue_depth: None,
                detail: Some("target conversation not registered".to_string()),
                capacity: None,
            },
        )
        .await?;
        return Ok(());
    }

    if registry_entry.is_none() {
        debug!(
            target: "codex::mailbox",
            event = "dispatcher.broker_fallback",
            source = %request.source_conversation_id,
            target = %request.target_conversation_id,
            "registry missing target locally; falling back to broker backend"
        );
    }

    let DispatchOutcome {
        status,
        queue_depth,
        detail,
        capacity,
    } = dispatch_with_backend(&request, registry_entry.as_ref(), backend.as_ref(), &config).await;

    respond(
        reader.into_inner(),
        &DispatchResponse {
            status,
            queue_depth,
            detail,
            capacity,
        },
    )
    .await?;

    info!(
        target: "codex::mailbox",
        event = "dispatcher.request.complete",
        peer = ?peer,
        source_conversation = %request.source_conversation_id,
        target_conversation = %request.target_conversation_id,
        message_id = %request.message.message_id,
        request_id = %request.message.audit.request_id.as_deref().unwrap_or(""),
        queue_depth,
        status = format_status(&status),
        "dispatcher request processed"
    );

    Ok(())
}

struct DispatchOutcome {
    status: DispatchStatus,
    queue_depth: Option<usize>,
    detail: Option<String>,
    capacity: Option<usize>,
}

async fn dispatch_with_backend<B>(
    request: &DispatchRequest,
    entry: Option<&RegistryRecord>,
    backend: &B,
    config: &MailServerConfig,
) -> DispatchOutcome
where
    B: DeliveryBackend + ?Sized,
{
    let mut attempts = 0usize;
    let started = Instant::now();
    let namespace_for_metrics = entry
        .map(|record| record.namespace.as_str())
        .unwrap_or(config.namespace.as_str());

    loop {
        attempts += 1;
        match backend.send(request, entry).await {
            Ok(ack) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_accept_total(namespace_for_metrics);
                    if let (Some(record), Some(depth)) = (entry, ack.queue_depth) {
                        codex_otel::metrics::update_mailbox_queue_depth_gauge(
                            &record.namespace,
                            depth as u64,
                        );
                    }
                }
                return DispatchOutcome {
                    status: DispatchStatus::Delivered,
                    queue_depth: ack.queue_depth,
                    detail: None,
                    capacity: None,
                };
            }
            Err(DeliveryError::QueueFull { capacity, detail }) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        namespace_for_metrics,
                        "queue_full",
                    );
                }
                return DispatchOutcome {
                    status: DispatchStatus::QueueFull,
                    queue_depth: None,
                    detail: Some(detail.unwrap_or_else(|| "mailbox queue is full".to_string())),
                    capacity,
                };
            }
            Err(DeliveryError::DispatcherClosed(detail)) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        namespace_for_metrics,
                        "dispatcher_closed",
                    );
                }
                return DispatchOutcome {
                    status: DispatchStatus::DispatcherClosed,
                    queue_depth: None,
                    detail: Some(detail.unwrap_or_else(|| "dispatcher closed".to_string())),
                    capacity: None,
                };
            }
            Err(DeliveryError::Disabled(detail)) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        namespace_for_metrics,
                        "dispatcher_disabled",
                    );
                }
                return DispatchOutcome {
                    status: DispatchStatus::Disabled,
                    queue_depth: None,
                    detail: Some(detail.unwrap_or_else(|| "dispatcher disabled".to_string())),
                    capacity: None,
                };
            }
            Err(DeliveryError::AckTimeout) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        namespace_for_metrics,
                        "ack_timeout",
                    );
                }
                return DispatchOutcome {
                    status: DispatchStatus::Timeout,
                    queue_depth: None,
                    detail: Some("timed out waiting for mailbox acknowledgement".to_string()),
                    capacity: None,
                };
            }
            Err(DeliveryError::UnknownSession(detail)) => {
                #[cfg(feature = "otel")]
                codex_otel::metrics::record_mailbox_error_total(
                    namespace_for_metrics,
                    "unknown_session",
                );
                return DispatchOutcome {
                    status: DispatchStatus::UnknownSession,
                    queue_depth: None,
                    detail: detail
                        .or_else(|| Some("target conversation not registered".to_string())),
                    capacity: None,
                };
            }
            Err(DeliveryError::Io { err, detail }) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        namespace_for_metrics,
                        "io_error",
                    );
                }
                warn!(
                    target: "codex::mailbox",
                    event = "dispatcher.forward.retry",
                    source = %request.source_conversation_id,
                    target = %request.target_conversation_id,
                    attempt = attempts,
                    error = %err,
                    detail = detail.as_deref().unwrap_or(""),
                    elapsed_ms = started.elapsed().as_millis(),
                    "forward attempt failed; will retry if backoff remains"
                );
                if let Some(delay) = config.retry_backoff.get(attempts.saturating_sub(1)) {
                    sleep(*delay).await;
                    continue;
                }
                return DispatchOutcome {
                    status: DispatchStatus::TransportError,
                    queue_depth: None,
                    detail: Some(detail.unwrap_or_else(|| err.to_string())),
                    capacity: None,
                };
            }
        }
    }
}

async fn respond(mut stream: UnixStream, response: &DispatchResponse) -> Result<()> {
    let mut buf = serde_json::to_vec(response)?;
    buf.push(b'\n');
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

fn format_status(status: &DispatchStatus) -> &'static str {
    match status {
        DispatchStatus::Delivered => "delivered",
        DispatchStatus::QueueFull => "queue_full",
        DispatchStatus::DispatcherClosed => "dispatcher_closed",
        DispatchStatus::Disabled => "disabled",
        DispatchStatus::Timeout => "timeout",
        DispatchStatus::UnknownSession => "unknown_session",
        DispatchStatus::TransportError => "transport_error",
        DispatchStatus::InvalidRequest => "invalid_request",
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::net::UnixListener;
    use tokio::sync::{Mutex, oneshot};

    fn base_config() -> MailServerConfig {
        MailServerConfig {
            namespace: "test".to_string(),
            codex_home: PathBuf::new(),
            socket_path: PathBuf::new(),
            registry_path: PathBuf::new(),
            ack_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_millis(200),
            retry_backoff: vec![Duration::from_millis(50), Duration::from_millis(100)],
            registry_poll_interval: Duration::from_millis(50),
            max_inflight: 8,
            delivery_backend: DeliveryBackendKind::UnixSocket,
            broker: None,
        }
    }

    fn make_record(socket_path: &Path) -> RegistryRecord {
        RegistryRecord {
            conversation_id: Uuid::now_v7(),
            session_id: Uuid::now_v7().to_string(),
            socket_path: socket_path.to_path_buf(),
            pid: std::process::id(),
            namespace: "test".to_string(),
        }
    }

    #[derive(Clone)]
    struct MockBackend {
        responses: Arc<Mutex<VecDeque<Result<MailboxAckPayload, DeliveryError>>>>,
        calls: Arc<AtomicUsize>,
    }

    impl MockBackend {
        fn new(responses: Vec<Result<MailboxAckPayload, DeliveryError>>) -> Self {
            let deque: VecDeque<_> = responses.into_iter().collect();
            Self {
                responses: Arc::new(Mutex::new(deque)),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl DeliveryBackend for MockBackend {
        async fn send(
            &self,
            _request: &DispatchRequest,
            _entry: Option<&RegistryRecord>,
        ) -> Result<MailboxAckPayload, DeliveryError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut guard = self.responses.lock().await;
            match guard.pop_front() {
                Some(result) => result,
                None => panic!("mock backend exhausted"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_with_unix_backend_succeeds() -> Result<()> {
        let dir = TempDir::new().context("temp dir")?;
        let mailbox_sock = dir.path().join("mailbox.sock");
        let listener = UnixListener::bind(&mailbox_sock).context("bind mailbox socket")?;

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                let _ = reader.read_line(&mut line).await;
                let msg: MailboxMessage = serde_json::from_str(line.trim()).unwrap();
                let ack = json!({
                    "ok": true,
                    "submission_id": "sub-1",
                    "message_id": msg.message_id,
                    "queue_depth": 3
                });
                let mut stream = reader.into_inner();
                stream
                    .write_all(serde_json::to_string(&ack).unwrap().as_bytes())
                    .await
                    .unwrap();
                stream.write_all(b"\n").await.unwrap();
                stream.flush().await.unwrap();
                let _ = tx.send(msg);
            }
        });

        let config = base_config();
        let record = make_record(&mailbox_sock);
        let message = MailboxMessage::default();
        let request = DispatchRequest {
            submission_id: "sub-1".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: record.conversation_id,
            message: message.clone(),
        };

        let backend = UnixSocketBackend::new(&config);
        let outcome = dispatch_with_backend(&request, Some(&record), &backend, &config).await;
        assert!(matches!(outcome.status, DispatchStatus::Delivered));
        assert_eq!(outcome.queue_depth, Some(3));
        assert!(rx.await.is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_handles_queue_full() -> Result<()> {
        let dir = TempDir::new().context("temp dir")?;
        let mailbox_sock = dir.path().join("mailbox.sock");
        let listener = UnixListener::bind(&mailbox_sock).context("bind mailbox socket")?;

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                let _ = reader.read_line(&mut line).await;
                let ack = json!({
                    "ok": false,
                    "err": "queue_full",
                    "detail": "Mailbox queue is full",
                    "capacity": 16
                });
                let mut stream = reader.into_inner();
                stream
                    .write_all(serde_json::to_string(&ack).unwrap().as_bytes())
                    .await
                    .unwrap();
                stream.write_all(b"\n").await.unwrap();
                stream.flush().await.unwrap();
            }
        });

        let mut config = base_config();
        config.retry_backoff = vec![]; // no retries for deterministic test
        let record = make_record(&mailbox_sock);
        let request = DispatchRequest {
            submission_id: "sub-1".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: record.conversation_id,
            message: MailboxMessage::default(),
        };

        let backend = UnixSocketBackend::new(&config);
        let outcome = dispatch_with_backend(&request, Some(&record), &backend, &config).await;
        assert!(matches!(outcome.status, DispatchStatus::QueueFull));
        assert_eq!(outcome.capacity, Some(16));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_eventually_reports_transport_error() -> Result<()> {
        let dir = TempDir::new().context("temp dir")?;
        let mailbox_sock = dir.path().join("missing.sock");
        // No listener bound intentionally.

        let mut config = base_config();
        config.retry_backoff = vec![Duration::from_millis(10)];
        let record = make_record(&mailbox_sock);
        let request = DispatchRequest {
            submission_id: "sub-1".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: record.conversation_id,
            message: MailboxMessage::default(),
        };

        let backend = UnixSocketBackend::new(&config);
        let outcome = dispatch_with_backend(&request, Some(&record), &backend, &config).await;
        assert!(matches!(outcome.status, DispatchStatus::TransportError));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_retries_transport_error_then_succeeds() -> Result<()> {
        let mut config = base_config();
        config.retry_backoff = vec![Duration::from_millis(1), Duration::from_millis(1)];

        let record = make_record(&PathBuf::from("unused.sock"));
        let request = DispatchRequest {
            submission_id: "sub-1".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: record.conversation_id,
            message: MailboxMessage::default(),
        };

        let responses = vec![
            Err(DeliveryError::Io {
                err: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
                detail: Some("connect failed".to_string()),
            }),
            Ok(MailboxAckPayload {
                ok: true,
                submission_id: Some("sub-1".to_string()),
                message_id: Some(Uuid::now_v7()),
                queue_depth: Some(2),
                err: None,
                detail: None,
                capacity: None,
            }),
        ];

        let backend = MockBackend::new(responses);
        let outcome = dispatch_with_backend(&request, Some(&record), &backend, &config).await;

        assert!(matches!(outcome.status, DispatchStatus::Delivered));
        assert_eq!(outcome.queue_depth, Some(2));
        assert_eq!(backend.call_count(), 2);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_propagates_disabled_error() -> Result<()> {
        let config = base_config();
        let record = make_record(&PathBuf::from("unused.sock"));
        let request = DispatchRequest {
            submission_id: "sub-1".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: record.conversation_id,
            message: MailboxMessage::default(),
        };

        let backend = MockBackend::new(vec![Err(DeliveryError::Disabled(Some(
            "dispatcher disabled".to_string(),
        )))]);

        let outcome = dispatch_with_backend(&request, Some(&record), &backend, &config).await;

        assert!(matches!(outcome.status, DispatchStatus::Disabled));
        assert_eq!(backend.call_count(), 1);
        assert_eq!(outcome.detail.as_deref(), Some("dispatcher disabled"));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nats_dispatch_falls_back_when_registry_missing() -> Result<()> {
        let mut config = base_config();
        config.delivery_backend = DeliveryBackendKind::Nats;
        let request = DispatchRequest {
            submission_id: "sub-1".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: Uuid::now_v7(),
            message: MailboxMessage::default(),
        };

        let ack = MailboxAckPayload {
            ok: true,
            submission_id: Some("sub-1".to_string()),
            message_id: Some(Uuid::now_v7()),
            queue_depth: Some(1),
            err: None,
            detail: None,
            capacity: None,
        };

        let backend = MockBackend::new(vec![Ok(ack)]);
        let outcome = dispatch_with_backend(&request, None, &backend, &config).await;

        assert!(matches!(outcome.status, DispatchStatus::Delivered));
        assert_eq!(backend.call_count(), 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nats_dispatch_reports_unknown_session() -> Result<()> {
        let mut config = base_config();
        config.delivery_backend = DeliveryBackendKind::Nats;
        let request = DispatchRequest {
            submission_id: "sub-2".to_string(),
            source_conversation_id: Uuid::now_v7(),
            target_conversation_id: Uuid::now_v7(),
            message: MailboxMessage::default(),
        };

        let backend = MockBackend::new(vec![Err(DeliveryError::UnknownSession(Some(
            "missing".to_string(),
        )))]);

        let outcome = dispatch_with_backend(&request, None, &backend, &config).await;

        assert!(matches!(outcome.status, DispatchStatus::UnknownSession));
        assert_eq!(backend.call_count(), 1);
        assert_eq!(outcome.detail.as_deref(), Some("missing"));
        Ok(())
    }
}
