use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use dirs::home_dir;

const DEFAULT_ACK_TIMEOUT_SECS: u64 = 10;
const DEFAULT_CONNECT_TIMEOUT_MILLIS: u64 = 1_500;
const DEFAULT_REGISTRY_POLL_MILLIS: u64 = 1_000;
const DEFAULT_BACKOFF_MILLIS: &[u64] = &[100, 250, 500, 1_000, 2_000];
const DEFAULT_MAX_INFLIGHT: usize = 128;
const DEFAULT_DELIVERY_BACKEND: &str = "unix";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryBackendKind {
    UnixSocket,
}

impl DeliveryBackendKind {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "unix" | "uds" | "unix_socket" => Some(Self::UnixSocket),
            _ => None,
        }
    }
}

fn resolve_namespace() -> String {
    env::var("CODEX_NAMESPACE")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .unwrap_or_else(|| "codex".to_string())
}

fn resolve_codex_home() -> Result<PathBuf> {
    if let Ok(home) = env::var("CODEX_HOME") {
        let path = PathBuf::from(home);
        return Ok(path);
    }
    if let Some(home) = home_dir() {
        return Ok(home.join(".codex"));
    }
    anyhow::bail!("unable to resolve Codex home directory; set CODEX_HOME or HOME");
}

fn parse_env_duration_ms(var: &str) -> Option<Duration> {
    env::var(var)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok().map(Duration::from_millis))
}

fn parse_env_duration_secs(var: &str) -> Option<Duration> {
    env::var(var)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok().map(Duration::from_secs))
}

fn parse_backoff(var: &str) -> Option<Vec<Duration>> {
    env::var(var).ok().map(|raw| {
        raw.split(',')
            .filter_map(|piece| piece.trim().parse::<u64>().ok())
            .map(|millis| Duration::from_millis(millis))
            .collect::<Vec<_>>()
    })
}

fn parse_env_usize(var: &str) -> Option<usize> {
    env::var(var)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

#[derive(Debug, Clone)]
pub struct MailServerConfig {
    pub namespace: String,
    pub codex_home: PathBuf,
    pub socket_path: PathBuf,
    pub registry_path: PathBuf,
    pub ack_timeout: Duration,
    pub connect_timeout: Duration,
    pub retry_backoff: Vec<Duration>,
    pub registry_poll_interval: Duration,
    pub max_inflight: usize,
    pub delivery_backend: DeliveryBackendKind,
}

impl MailServerConfig {
    pub fn from_env() -> Result<Self> {
        let namespace = resolve_namespace();
        let codex_home = resolve_codex_home()?;
        let default_socket = codex_home
            .join(&namespace)
            .join("mailbox")
            .join("dispatcher.sock");
        let socket_path = env::var("CODEX_MAIL_SERVER_SOCKET")
            .map(PathBuf::from)
            .unwrap_or(default_socket);

        let default_registry = codex_home
            .join(&namespace)
            .join("mailbox")
            .join("registry.json");
        let registry_path = env::var("CODEX_MAILBOX_REGISTRY_PATH")
            .map(PathBuf::from)
            .unwrap_or(default_registry);

        let delivery_backend = env::var("CODEX_MAIL_SERVER_DELIVERY_BACKEND")
            .ok()
            .map(|raw| {
                DeliveryBackendKind::parse(&raw).ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid CODEX_MAIL_SERVER_DELIVERY_BACKEND value: {}",
                        raw
                    )
                })
            })
            .transpose()?
            .unwrap_or_else(|| {
                DeliveryBackendKind::parse(DEFAULT_DELIVERY_BACKEND)
                    .expect("default delivery backend must parse")
            });

        let ack_timeout = parse_env_duration_secs("CODEX_MAIL_SERVER_ACK_TIMEOUT_SECS")
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_ACK_TIMEOUT_SECS));
        let connect_timeout = parse_env_duration_ms("CODEX_MAIL_SERVER_CONNECT_TIMEOUT_MS")
            .unwrap_or_else(|| Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MILLIS));
        let registry_poll_interval = parse_env_duration_ms("CODEX_MAIL_SERVER_REGISTRY_POLL_MS")
            .unwrap_or_else(|| Duration::from_millis(DEFAULT_REGISTRY_POLL_MILLIS));
        let retry_backoff = parse_backoff("CODEX_MAIL_SERVER_RETRY_MS")
            .filter(|vec| !vec.is_empty())
            .unwrap_or_else(|| {
                DEFAULT_BACKOFF_MILLIS
                    .iter()
                    .map(|v| Duration::from_millis(*v))
                    .collect()
            });
        let max_inflight =
            parse_env_usize("CODEX_MAIL_SERVER_MAX_INFLIGHT").unwrap_or(DEFAULT_MAX_INFLIGHT);

        Ok(Self {
            namespace,
            codex_home,
            socket_path,
            registry_path,
            ack_timeout,
            connect_timeout,
            retry_backoff,
            registry_poll_interval,
            max_inflight,
            delivery_backend,
        })
    }

    pub fn ensure_socket_parent(&self) -> Result<()> {
        if let Some(parent) = self.socket_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create socket directory {}", parent.display())
            })?;
        }
        Ok(())
    }
}

pub fn load_config() -> Result<MailServerConfig> {
    MailServerConfig::from_env()
}
