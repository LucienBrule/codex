use serde::Deserialize;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(1_500);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(5_000);

/// Environment variable that points to the vm-pty host daemon socket.
const SOCKET_ENV: &str = "CODEX_VM_PTY_SOCKET";
const CONNECT_TIMEOUT_ENV: &str = "CODEX_VM_PTY_CONNECT_TIMEOUT_MS";
const REQUEST_TIMEOUT_ENV: &str = "CODEX_VM_PTY_REQUEST_TIMEOUT_MS";

pub fn socket_path_from_env() -> Option<PathBuf> {
    std::env::var(SOCKET_ENV)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

pub fn is_configured() -> bool {
    socket_path_from_env().is_some()
}

#[derive(Debug, Clone)]
pub struct VmPtyClient {
    endpoint: PathBuf,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl VmPtyClient {
    pub fn from_env() -> Result<Option<Self>, VmPtyClientError> {
        let Some(endpoint) = socket_path_from_env() else {
            return Ok(None);
        };

        let connect_timeout = parse_timeout_env(CONNECT_TIMEOUT_ENV, DEFAULT_CONNECT_TIMEOUT)?;
        let request_timeout = parse_timeout_env(REQUEST_TIMEOUT_ENV, DEFAULT_REQUEST_TIMEOUT)?;

        Ok(Some(Self::new(endpoint, connect_timeout, request_timeout)))
    }

    pub fn with_request_timeout(&self, request_timeout: Duration) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            connect_timeout: self.connect_timeout,
            request_timeout,
        }
    }

    pub fn new(endpoint: PathBuf, connect_timeout: Duration, request_timeout: Duration) -> Self {
        Self {
            endpoint,
            connect_timeout,
            request_timeout,
        }
    }

    async fn perform_request<T: Serialize>(
        &self,
        action: &str,
        payload: &T,
    ) -> Result<VmPtyResponseInternal, VmPtyClientError> {
        let req_id = Uuid::new_v4().to_string();
        let connect_future = UnixStream::connect(&self.endpoint);

        let mut stream = match timeout(self.connect_timeout, connect_future).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                return Err(VmPtyClientError::Connect {
                    endpoint: self.endpoint.clone(),
                    source: err,
                });
            }
            Err(_) => {
                return Err(VmPtyClientError::Timeout {
                    endpoint: self.endpoint.clone(),
                });
            }
        };

        let envelope = VmPtyEnvelope {
            id: &req_id,
            action,
            payload,
        };

        let mut payload_bytes = serde_json::to_vec(&envelope).map_err(VmPtyClientError::Serialize)?;
        payload_bytes.push(b'\n');

        let send_and_read = async {
            stream.write_all(&payload_bytes).await?;
            stream.flush().await?;

            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            let read = reader.read_line(&mut line).await?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "vm-pty daemon closed connection without responding",
                ));
            }

            Ok::<_, std::io::Error>(line)
        };

        let line = match timeout(self.request_timeout, send_and_read).await {
            Ok(result) => result.map_err(VmPtyClientError::Io)?,
            Err(_) => {
                return Err(VmPtyClientError::Timeout {
                    endpoint: self.endpoint.clone(),
                });
            }
        };

        let response: VmPtyResponseInternal =
            serde_json::from_str(line.trim_end()).map_err(VmPtyClientError::Deserialize)?;

        if response.id != req_id {
            return Err(VmPtyClientError::MismatchedResponseId {
                expected: req_id,
                actual: response.id,
            });
        }

        Ok(response)
    }

    async fn request_json<T: Serialize>(
        &self,
        action: &str,
        payload: &T,
    ) -> Result<JsonValue, VmPtyClientError> {
        let response = self.perform_request(action, payload).await?;
        let VmPtyResponseInternal {
            id: _,
            status,
            ok,
            result,
            error,
        } = response;

        let success = matches!(status.as_deref(), Some("ok")) || ok.unwrap_or(false);
        if success {
            result.ok_or(VmPtyClientError::MissingResult)
        } else {
            let error = error.unwrap_or_default();
            Err(VmPtyClientError::Server {
                code: error.code.unwrap_or_else(|| "E_UNKNOWN".to_string()),
                message: error
                    .message
                    .unwrap_or_else(|| "vm-pty daemon reported an error".to_string()),
            })
        }
    }

    pub async fn pty_open(
        &self,
        request: VmPtyOpenRequest,
    ) -> Result<VmPtyOpenResponse, VmPtyClientError> {
        let response = self.perform_request("pty_open", &request).await?;
        let VmPtyResponseInternal {
            id: _,
            status,
            ok,
            result,
            error,
        } = response;

        let success = matches!(status.as_deref(), Some("ok")) || ok.unwrap_or(false);
        if !success {
            let error = error.unwrap_or_default();
            return Err(VmPtyClientError::Server {
                code: error.code.unwrap_or_else(|| "E_UNKNOWN".to_string()),
                message: error
                    .message
                    .unwrap_or_else(|| "vm-pty daemon reported an error".to_string()),
            });
        }

        let result_value = result.ok_or(VmPtyClientError::MissingResult)?;
        let parsed: VmPtyOpenResultInternal =
            serde_json::from_value(result_value).map_err(VmPtyClientError::Deserialize)?;
        Ok(VmPtyOpenResponse {
            session_id: parsed.session_id,
            initial_output: parsed.initial_output.unwrap_or_default(),
            cols: parsed.cols.unwrap_or(80),
            rows: parsed.rows.unwrap_or(24),
        })
    }

    pub async fn pty_write(
        &self,
        session_id: &str,
        data: &str,
        cursor: Option<u64>,
    ) -> Result<JsonValue, VmPtyClientError> {
        let payload = if let Some(cursor) = cursor {
            json!({
                "session_id": session_id,
                "data": data,
                "cursor": cursor,
            })
        } else {
            json!({
                "session_id": session_id,
                "data": data,
            })
        };
        self.request_json("pty_write", &payload).await
    }

    pub async fn pty_read(
        &self,
        session_id: &str,
        max_bytes: Option<u64>,
        cursor: Option<u64>,
    ) -> Result<JsonValue, VmPtyClientError> {
        let payload = match (max_bytes, cursor) {
            (Some(max_bytes), Some(cursor)) => json!({
                "session_id": session_id,
                "max_bytes": max_bytes,
                "cursor": cursor,
            }),
            (Some(max_bytes), None) => json!({
                "session_id": session_id,
                "max_bytes": max_bytes,
            }),
            (None, Some(cursor)) => json!({
                "session_id": session_id,
                "cursor": cursor,
            }),
            (None, None) => json!({
                "session_id": session_id,
            }),
        };
        self.request_json("pty_read", &payload).await
    }

    pub async fn pty_attach(&self, vm_id: &str) -> Result<VmPtyAttachResponse, VmPtyClientError> {
        let response = self
            .perform_request("pty_attach", &json!({ "vm_id": vm_id }))
            .await?;
        let VmPtyResponseInternal { id: _, status, ok, result, error } = response;
        let success = matches!(status.as_deref(), Some("ok")) || ok.unwrap_or(false);
        if !success {
            let error = error.unwrap_or_default();
            return Err(VmPtyClientError::Server {
                code: error.code.unwrap_or_else(|| "E_UNKNOWN".to_string()),
                message: error.message.unwrap_or_else(|| "vm-pty daemon reported an error".to_string()),
            });
        }
        let result_value = result.ok_or(VmPtyClientError::MissingResult)?;
        let parsed: VmPtyAttachResultInternal = serde_json::from_value(result_value)
            .map_err(VmPtyClientError::Deserialize)?;
        Ok(VmPtyAttachResponse { session_id: parsed.session_id, cols: parsed.cols.unwrap_or(80), rows: parsed.rows.unwrap_or(24) })
    }

    pub async fn pty_resize(
        &self,
        session_id: &str,
        cols: Option<u16>,
        rows: Option<u16>,
        cursor: Option<u64>,
    ) -> Result<JsonValue, VmPtyClientError> {
        let payload = match (cols, rows, cursor) {
            (Some(cols), Some(rows), Some(cursor)) => json!({
                "session_id": session_id,
                "cols": cols,
                "rows": rows,
                "cursor": cursor,
            }),
            (Some(cols), Some(rows), None) => json!({
                "session_id": session_id,
                "cols": cols,
                "rows": rows,
            }),
            (Some(cols), None, Some(cursor)) => json!({
                "session_id": session_id,
                "cols": cols,
                "cursor": cursor,
            }),
            (Some(cols), None, None) => json!({
                "session_id": session_id,
                "cols": cols,
            }),
            (None, Some(rows), Some(cursor)) => json!({
                "session_id": session_id,
                "rows": rows,
                "cursor": cursor,
            }),
            (None, Some(rows), None) => json!({
                "session_id": session_id,
                "rows": rows,
            }),
            (None, None, Some(cursor)) => json!({
                "session_id": session_id,
                "cursor": cursor,
            }),
            (None, None, None) => json!({
                "session_id": session_id,
            }),
        };
        self.request_json("pty_resize", &payload).await
    }

    pub async fn pty_signal(
        &self,
        session_id: &str,
        signal: &str,
    ) -> Result<JsonValue, VmPtyClientError> {
        let payload = json!({
            "session_id": session_id,
            "signal": signal,
        });
        self.request_json("pty_signal", &payload).await
    }
}

fn parse_timeout_env(var: &'static str, default: Duration) -> Result<Duration, VmPtyClientError> {
    match std::env::var(var) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(default);
            }
            let millis: u64 = trimmed
                .parse()
                .map_err(|err: std::num::ParseIntError| VmPtyClientError::InvalidConfig {
                    var,
                    reason: err.to_string(),
                })?;
            Ok(Duration::from_millis(millis))
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(value)) => Err(VmPtyClientError::InvalidConfig {
            var,
            reason: format!("non-unicode value: {:?}", value),
        }),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct VmPtyOpenRequest {
    pub vm_id: String,
    pub workspace: String,
    pub cwd: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub env: BTreeMap<String, String>,
    pub shell: String,
    pub cols: u16,
    pub rows: u16,
}

impl VmPtyOpenRequest {
    pub fn new(
        vm_id: String,
        workspace: String,
        cwd: String,
        env: BTreeMap<String, String>,
        shell: String,
        cols: u16,
        rows: u16,
    ) -> Self {
        Self {
            vm_id,
            workspace,
            cwd,
            env,
            shell,
            cols,
            rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmPtyOpenResponse {
    pub session_id: String,
    pub initial_output: String,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct VmPtyAttachResultInternal {
    session_id: String,
    cols: Option<u16>,
    rows: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmPtyAttachResponse {
    pub session_id: String,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Error)]
pub enum VmPtyClientError {
    #[error("vm-pty client configuration error for {var}: {reason}")]
    InvalidConfig { var: &'static str, reason: String },
    #[error("failed to connect to vm-pty endpoint {endpoint:?}: {source}")]
    Connect {
        endpoint: PathBuf,
        source: std::io::Error,
    },
    #[error("vm-pty request to {endpoint:?} timed out")]
    Timeout { endpoint: PathBuf },
    #[error("vm-pty request I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize vm-pty request: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to parse vm-pty response: {0}")]
    Deserialize(serde_json::Error),
    #[error("vm-pty daemon responded with error {code}: {message}")]
    Server { code: String, message: String },
    #[error("vm-pty response missing result payload")]
    MissingResult,
    #[error("vm-pty response id mismatch (expected {expected}, got {actual})")]
    MismatchedResponseId { expected: String, actual: String },
    #[error("vm-pty response invalid: {0}")]
    InvalidResponse(String),
}

#[derive(Serialize)]
struct VmPtyEnvelope<'a, T> {
    id: &'a str,
    action: &'a str,
    payload: &'a T,
}

#[derive(Debug, Deserialize)]
struct VmPtyResponseInternal {
    id: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    ok: Option<bool>,
    #[serde(default)]
    result: Option<JsonValue>,
    #[serde(default)]
    error: Option<VmPtyErrorBody>,
}

#[derive(Debug, Deserialize)]
struct VmPtyOpenResultInternal {
    session_id: String,
    #[serde(default)]
    initial_output: Option<String>,
    #[serde(default)]
    cols: Option<u16>,
    #[serde(default)]
    rows: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct VmPtyErrorBody {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::UnixListener;
    use tokio::time::sleep;

    async fn spawn_stub_server(
        socket_path: PathBuf,
        mut response: serde_json::Value,
        captured: std::sync::Arc<tokio::sync::Mutex<Vec<serde_json::Value>>>,
    ) {
        let listener = UnixListener::bind(&socket_path).expect("bind unix listener");

        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read request");

        let value: serde_json::Value =
            serde_json::from_str(line.trim_end()).expect("parse request json");
        {
            let mut guard = captured.lock().await;
            guard.push(value.clone());
        }

        if let Some(request_id) = value.get("id").and_then(|v| v.as_str()) {
            response["id"] = serde_json::Value::String(request_id.to_string());
        }

        let mut stream = reader.into_inner();
        let mut payload = response.to_string().into_bytes();
        payload.push(b'\n');
        stream.write_all(&payload).await.expect("write response");
        stream.flush().await.expect("flush response");
    }

    async fn wait_for_socket(path: &Path) {
        for _ in 0..100 {
            if path.exists() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("socket {path:?} did not become available");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pty_open_successful_response() {
        let tmp = TempDir::new().expect("tempdir");
        let socket_path = tmp.path().join("vm-pty.sock");
        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let response = serde_json::json!({
            "id": "test-id",
            "status": "ok",
            "result": {
                "session_id": "session-123",
                "initial_output": "welcome\n",
                "cols": 100,
                "rows": 40
            }
        });

        let captured_clone = captured.clone();
        tokio::spawn(spawn_stub_server(
            socket_path.clone(),
            response.clone(),
            captured_clone,
        ));

        wait_for_socket(&socket_path).await;

        let client = VmPtyClient::new(
            socket_path,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let request = VmPtyOpenRequest::new(
            "vm-1".to_string(),
            "/workspace".to_string(),
            "/workspace".to_string(),
            BTreeMap::new(),
            "/bin/bash".to_string(),
            80,
            24,
        );
        let result = client.pty_open(request).await.expect("pty_open");

        assert_eq!(
            result,
            VmPtyOpenResponse {
                session_id: "session-123".to_string(),
                initial_output: "welcome\n".to_string(),
                cols: 100,
                rows: 40,
            }
        );

        let guard = captured.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0]["action"], "pty_open");
        assert_eq!(guard[0]["payload"]["vm_id"], "vm-1");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pty_open_error_response() {
        let tmp = TempDir::new().expect("tempdir");
        let socket_path = tmp.path().join("vm-pty.sock");
        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let response = serde_json::json!({
            "id": "test-id",
            "status": "error",
            "error": {
                "code": "E_NO_VM",
                "message": "vm not available"
            }
        });

        let captured_clone = captured.clone();
        tokio::spawn(spawn_stub_server(
            socket_path.clone(),
            response,
            captured_clone,
        ));

        wait_for_socket(&socket_path).await;

        let client = VmPtyClient::new(
            socket_path,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let request = VmPtyOpenRequest::new(
            "vm-missing".to_string(),
            "/workspace".to_string(),
            "/workspace".to_string(),
            BTreeMap::new(),
            "/bin/bash".to_string(),
            80,
            24,
        );

        let err = client
            .pty_open(request)
            .await
            .expect_err("pty_open should fail");
        match err {
            VmPtyClientError::Server { code, message } => {
                assert_eq!(code, "E_NO_VM");
                assert_eq!(message, "vm not available");
            }
            other => panic!("expected server error, got {other:?}"),
        }

        let guard = captured.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0]["payload"]["vm_id"], "vm-missing");
    }
}
