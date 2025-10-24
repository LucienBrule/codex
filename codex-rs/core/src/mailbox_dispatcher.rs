use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

use codex_protocol::mailbox::MailboxMessage;

use crate::contacts::get_runtime_home_and_namespace;

const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 1_500;
const DEFAULT_ACK_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Clone)]
pub struct MailboxDispatcherClient {
    endpoint: PathBuf,
    connect_timeout: Duration,
    ack_timeout: Duration,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DispatchStatus {
    Delivered,
    QueueFull,
    DispatcherClosed,
    Disabled,
    Timeout,
    UnknownSession,
    TransportError,
    InvalidRequest,
}

#[derive(Debug, Clone)]
pub struct DispatchOutcome {
    pub status: DispatchStatus,
    pub queue_depth: Option<usize>,
    pub capacity: Option<usize>,
    pub detail: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum MailboxDispatcherError {
    #[error("dispatcher endpoint unavailable: {0}")]
    Unavailable(std::io::Error),
    #[error("dispatcher request failed: {0}")]
    Io(std::io::Error),
    #[error("dispatcher request timed out")]
    Timeout,
    #[error("dispatcher responded with invalid payload: {0}")]
    InvalidResponse(String),
}

#[derive(Serialize)]
struct ClientDispatchRequest<'a> {
    submission_id: &'a str,
    source_conversation_id: Uuid,
    target_conversation_id: Uuid,
    message: &'a MailboxMessage,
}

#[derive(Deserialize)]
struct ClientDispatchResponse {
    status: DispatchStatus,
    #[serde(default)]
    queue_depth: Option<usize>,
    #[serde(default)]
    detail: Option<String>,
    #[serde(default)]
    capacity: Option<usize>,
}

impl MailboxDispatcherClient {
    pub fn from_env() -> Option<Self> {
        let forced = parse_bool_env("CODEX_MAIL_SERVER_FORCE");

        if let Some(force) = forced {
            if !force {
                tracing::debug!(
                    target: "codex::mailbox",
                    event = "dispatcher.detect.disabled",
                    reason = "force_env_false",
                    "dispatcher client disabled via CODEX_MAIL_SERVER_FORCE"
                );
                return None;
            }
        }

        if matches!(parse_bool_env("CODEX_MAIL_SERVER_DISABLE"), Some(true)) {
            tracing::debug!(
                target: "codex::mailbox",
                event = "dispatcher.detect.disabled",
                reason = "mail_server_disable",
                "dispatcher client disabled via CODEX_MAIL_SERVER_DISABLE"
            );
            return None;
        }

        let endpoint = match endpoint_from_env().or_else(default_endpoint_path) {
            Some(path) => path,
            None => {
                tracing::debug!(
                    target: "codex::mailbox",
                    event = "dispatcher.detect.disabled",
                    reason = "no_endpoint",
                    "dispatcher endpoint not configured; skipping client construction"
                );
                return None;
            }
        };

        let endpoint_exists = endpoint_exists(&endpoint);

        let enabled = forced
            .or_else(|| parse_bool_env("CODEX_MAIL_SERVER_ENABLE"))
            .or_else(|| parse_bool_env("CODEX_MAILBOX_OOB"))
            .unwrap_or(endpoint_exists);

        if !enabled {
            tracing::debug!(
                target: "codex::mailbox",
                event = "dispatcher.detect.disabled",
                reason = "flags_off",
                endpoint = %endpoint.display(),
                endpoint_exists,
                "dispatcher enable flags resolved false"
            );
            return None;
        }

        let connect_timeout = std::env::var("CODEX_MAIL_SERVER_CLIENT_CONNECT_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS));

        let ack_timeout = std::env::var("CODEX_MAIL_SERVER_CLIENT_ACK_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_ACK_TIMEOUT_SECS));

        tracing::debug!(
            target: "codex::mailbox",
            event = "dispatcher.detect.enabled",
            endpoint = %endpoint.display(),
            endpoint_exists,
            connect_timeout_ms = connect_timeout.as_millis(),
            ack_timeout_secs = ack_timeout.as_secs(),
            "dispatcher client constructed from environment"
        );

        Some(Self {
            endpoint,
            connect_timeout,
            ack_timeout,
        })
    }

    pub async fn dispatch(
        &self,
        submission_id: &str,
        source_conversation_id: Uuid,
        target_conversation_id: Uuid,
        message: &MailboxMessage,
    ) -> Result<DispatchOutcome, MailboxDispatcherError> {
        let stream = UnixStream::connect(&self.endpoint);
        let mut stream = match timeout(self.connect_timeout, stream).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => return Err(MailboxDispatcherError::Unavailable(err)),
            Err(_) => {
                return Err(MailboxDispatcherError::Timeout);
            }
        };

        let request = ClientDispatchRequest {
            submission_id,
            source_conversation_id,
            target_conversation_id,
            message,
        };
        let mut payload = serde_json::to_vec(&request).map_err(|err| {
            MailboxDispatcherError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                err.to_string(),
            ))
        })?;
        payload.push(b'\n');

        stream
            .write_all(&payload)
            .await
            .map_err(MailboxDispatcherError::Io)?;
        stream.flush().await.map_err(MailboxDispatcherError::Io)?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        let bytes = timeout(self.ack_timeout, reader.read_line(&mut line))
            .await
            .map_err(|_| MailboxDispatcherError::Timeout)?
            .map_err(MailboxDispatcherError::Io)?;

        if bytes == 0 {
            return Err(MailboxDispatcherError::InvalidResponse(
                "dispatcher closed connection without response".to_string(),
            ));
        }

        let response: ClientDispatchResponse = serde_json::from_str(line.trim())
            .map_err(|err| MailboxDispatcherError::InvalidResponse(err.to_string()))?;

        Ok(DispatchOutcome {
            status: response.status,
            queue_depth: response.queue_depth,
            capacity: response.capacity,
            detail: response.detail,
        })
    }

    pub fn endpoint(&self) -> &Path {
        &self.endpoint
    }
}

fn parse_bool_env(var: &str) -> Option<bool> {
    std::env::var(var).ok().and_then(|raw| {
        let lowered = raw.trim().to_ascii_lowercase();
        match lowered.as_str() {
            "1" | "true" | "on" | "enabled" => Some(true),
            "0" | "false" | "off" | "disabled" => Some(false),
            _ => None,
        }
    })
}

fn normalize_endpoint(raw: String) -> Option<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("unix://") {
        Some(PathBuf::from(rest))
    } else {
        Some(PathBuf::from(trimmed))
    }
}

fn default_endpoint_path() -> Option<PathBuf> {
    let (home, namespace) = get_runtime_home_and_namespace();
    Some(home.join(namespace).join("mailbox").join("dispatcher.sock"))
}

fn endpoint_from_env() -> Option<PathBuf> {
    std::env::var("CODEX_MAIL_SERVER_ENDPOINT")
        .ok()
        .and_then(normalize_endpoint)
}

fn endpoint_exists(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            let file_type = meta.file_type();
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                if file_type.is_socket() {
                    return true;
                }
            }
            file_type.is_file()
        }
        Err(_) => false,
    }
}
