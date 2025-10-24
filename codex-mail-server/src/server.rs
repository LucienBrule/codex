use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use codex_protocol::mailbox::MailboxMessage;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::pin;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep, timeout};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::MailServerConfig;
use crate::registry::{RegistryRecord, RegistryWatcher};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
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

pub struct MailDispatcherServer {
    config: MailServerConfig,
    registry: RegistryWatcher,
    semaphore: Arc<Semaphore>,
}

impl MailDispatcherServer {
    pub fn new(config: MailServerConfig, registry: RegistryWatcher) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_inflight));
        Self {
            config,
            registry,
            semaphore,
        }
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
                            tokio::spawn(async move {
                                if let Ok(permit) = permit {
                                    if let Err(err) = handle_connection(stream, registry, &config).await {
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
    config: &MailServerConfig,
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

    let registry_entry = match registry.lookup(&request.target_conversation_id) {
        Some(entry) => entry,
        None => {
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
    };

    let DispatchOutcome {
        status,
        queue_depth,
        detail,
        capacity,
    } = dispatch_to_socket(&request, &registry_entry, config).await;

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

async fn dispatch_to_socket(
    request: &DispatchRequest,
    entry: &RegistryRecord,
    config: &MailServerConfig,
) -> DispatchOutcome {
    let mut attempts = 0usize;
    let started = Instant::now();

    loop {
        attempts += 1;
        match attempt_forward(request, entry, config).await {
            Ok(ack) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_accept_total(&entry.namespace);
                    if let Some(depth) = ack.queue_depth {
                        codex_otel::metrics::update_mailbox_queue_depth_gauge(
                            &entry.namespace,
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
            Err(ForwardError::QueueFull { capacity, detail }) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(&entry.namespace, "queue_full");
                }
                return DispatchOutcome {
                    status: DispatchStatus::QueueFull,
                    queue_depth: None,
                    detail: Some(detail.unwrap_or_else(|| "mailbox queue is full".to_string())),
                    capacity,
                };
            }
            Err(ForwardError::DispatcherClosed(detail)) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        &entry.namespace,
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
            Err(ForwardError::Disabled(detail)) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        &entry.namespace,
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
            Err(ForwardError::AckTimeout) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(
                        &entry.namespace,
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
            Err(ForwardError::Io { err, detail }) => {
                #[cfg(feature = "otel")]
                {
                    codex_otel::metrics::record_mailbox_error_total(&entry.namespace, "io_error");
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

async fn attempt_forward(
    request: &DispatchRequest,
    entry: &RegistryRecord,
    config: &MailServerConfig,
) -> Result<MailboxAckPayload, ForwardError> {
    let socket_path = &entry.socket_path;
    let connect = UnixStream::connect(socket_path);
    let mut stream = match timeout(config.connect_timeout, connect).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => {
            return Err(ForwardError::Io {
                err,
                detail: Some("failed to connect to mailbox socket".to_string()),
            });
        }
        Err(_) => {
            return Err(ForwardError::Io {
                err: std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"),
                detail: Some("connecting to mailbox socket timed out".to_string()),
            });
        }
    };

    let mut payload = serde_json::to_vec(&request.message).map_err(|err| ForwardError::Io {
        err: std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string()),
        detail: Some("failed to serialize mailbox message".to_string()),
    })?;
    payload.push(b'\n');

    stream
        .write_all(&payload)
        .await
        .map_err(|err| ForwardError::Io {
            err,
            detail: Some("failed to write mailbox payload".to_string()),
        })?;
    stream.flush().await.map_err(|err| ForwardError::Io {
        err,
        detail: Some("failed to flush mailbox payload".to_string()),
    })?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let bytes = timeout(config.ack_timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| ForwardError::AckTimeout)?
        .map_err(|err| ForwardError::Io {
            err,
            detail: Some("failed to read mailbox acknowledgement".to_string()),
        })?;

    if bytes == 0 {
        return Err(ForwardError::DispatcherClosed(Some(
            "mailbox listener closed connection without acknowledgement".to_string(),
        )));
    }

    let ack: MailboxAckPayload =
        serde_json::from_str(line.trim()).map_err(|err| ForwardError::Io {
            err: std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()),
            detail: Some("failed to parse mailbox acknowledgement".to_string()),
        })?;

    if ack.ok {
        return Ok(ack);
    }

    match ack.err.as_deref() {
        Some("queue_full") => Err(ForwardError::QueueFull {
            capacity: ack.capacity,
            detail: ack.detail.clone(),
        }),
        Some("disabled") => Err(ForwardError::Disabled(ack.detail.clone())),
        Some("closed") => Err(ForwardError::DispatcherClosed(ack.detail.clone())),
        Some(other) => Err(ForwardError::Io {
            err: std::io::Error::new(std::io::ErrorKind::Other, other.to_string()),
            detail: ack.detail.clone(),
        }),
        None => Err(ForwardError::Io {
            err: std::io::Error::new(std::io::ErrorKind::Other, "unknown dispatcher error"),
            detail: ack.detail.clone(),
        }),
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

#[derive(Debug)]
enum ForwardError {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::net::UnixListener;
    use tokio::sync::oneshot;

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
        }
    }

    fn make_record(socket_path: &PathBuf) -> RegistryRecord {
        RegistryRecord {
            conversation_id: Uuid::now_v7(),
            session_id: Uuid::now_v7().to_string(),
            socket_path: socket_path.clone(),
            pid: std::process::id(),
            namespace: "test".to_string(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_to_socket_succeeds() -> Result<()> {
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

        let outcome = dispatch_to_socket(&request, &record, &config).await;
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

        let outcome = dispatch_to_socket(&request, &record, &config).await;
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

        let outcome = dispatch_to_socket(&request, &record, &config).await;
        assert!(matches!(outcome.status, DispatchStatus::TransportError));
        Ok(())
    }
}
