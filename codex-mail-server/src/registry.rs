use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::MailServerConfig;

#[derive(Clone, Debug)]
pub struct RegistryRecord {
    pub conversation_id: Uuid,
    pub session_id: String,
    pub socket_path: PathBuf,
    pub pid: u32,
    pub namespace: String,
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<Uuid, RegistryRecord>,
    version: u32,
    last_loaded: Option<SystemTime>,
}

#[derive(Deserialize)]
struct RawRegistryFile {
    version: Option<u32>,
    entries: HashMap<String, RawRegistryEntry>,
}

#[derive(Deserialize)]
struct RawRegistryEntry {
    session_id: String,
    pid: u32,
    socket_path: String,
    namespace: Option<String>,
}

#[derive(Clone)]
pub struct RegistryWatcher {
    state: Arc<RwLock<RegistryState>>,
}

impl RegistryWatcher {
    pub async fn new(config: &MailServerConfig) -> Result<Self> {
        let state = Arc::new(RwLock::new(RegistryState::default()));
        let path = config.registry_path.clone();
        let interval = config.registry_poll_interval;
        let namespace = config.namespace.clone();
        let state_clone = state.clone();

        tokio::spawn(async move {
            loop {
                if let Err(err) = reload_registry(&path, &namespace, &state_clone).await {
                    warn!(target: "codex::mailbox", event = "dispatcher.registry.reload_error", ?err, path = %path.display());
                }
                sleep(interval).await;
            }
        });

        Ok(Self { state })
    }

    pub fn lookup(&self, conversation_id: &Uuid) -> Option<RegistryRecord> {
        let guard = self.state.read();
        guard.entries.get(conversation_id).cloned()
    }

    pub fn snapshot(&self) -> HashMap<Uuid, RegistryRecord> {
        let guard = self.state.read();
        guard.entries.clone()
    }
}

async fn reload_registry(
    path: &PathBuf,
    namespace: &str,
    state: &Arc<RwLock<RegistryState>>,
) -> Result<()> {
    let meta = match tokio::fs::metadata(path).await {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            debug!(target: "codex::mailbox", event = "dispatcher.registry.missing", path = %path.display(), "mailbox registry missing; waiting for creation");
            return Ok(());
        }
        Err(err) => {
            return Err(err).context(format!(
                "failed to stat mailbox registry {}",
                path.display()
            ));
        }
    };

    let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let should_reload = {
        let guard = state.read();
        guard
            .last_loaded
            .map(|prev| prev < modified)
            .unwrap_or(true)
    };

    if !should_reload {
        return Ok(());
    }

    let contents = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read mailbox registry {}", path.display()))?;
    if contents.is_empty() {
        return Ok(());
    }

    let raw: RawRegistryFile = serde_json::from_slice(&contents)
        .with_context(|| format!("failed to parse mailbox registry {}", path.display()))?;

    let mut entries: HashMap<Uuid, RegistryRecord> = HashMap::new();
    for (key, value) in raw.entries.into_iter() {
        let session_id = value.session_id;
        let namespace = value.namespace.unwrap_or_else(|| namespace.to_string());
        let socket_path = PathBuf::from(value.socket_path);
        if socket_path.exists() {
            match Uuid::parse_str(&key) {
                Ok(conversation_id) => {
                    let record = RegistryRecord {
                        conversation_id,
                        session_id,
                        socket_path,
                        pid: value.pid,
                        namespace,
                    };
                    entries.insert(conversation_id, record);
                }
                Err(_) => {
                    warn!(
                        target: "codex::mailbox",
                        event = "dispatcher.registry.invalid_id",
                        conversation = %key,
                        "invalid conversation UUID in registry"
                    );
                }
            }
        } else {
            debug!(
                target: "codex::mailbox",
                event = "dispatcher.registry.stale_socket",
                conversation = %key,
                path = %socket_path.display(),
                "registry entry references missing socket; skipping"
            );
        }
    }

    let mut guard = state.write();
    let previous = guard.entries.clone();
    guard.entries = entries;
    guard.version = raw.version.unwrap_or(0);
    guard.last_loaded = Some(modified);
    let current = guard.entries.clone();
    drop(guard);

    log_registry_diff(&previous, &current);

    Ok(())
}

fn log_registry_diff(
    previous: &HashMap<Uuid, RegistryRecord>,
    current: &HashMap<Uuid, RegistryRecord>,
) {
    for entry in current.values() {
        if !previous.contains_key(&entry.conversation_id) {
            info!(
                target: "codex::mailbox",
                event = "dispatcher.registry.added",
                namespace = %entry.namespace,
                conversation_id = %entry.conversation_id,
                socket = %entry.socket_path.display(),
                pid = entry.pid,
                "registry entry added"
            );
        }
    }
    for entry in previous.values() {
        if !current.contains_key(&entry.conversation_id) {
            info!(
                target: "codex::mailbox",
                event = "dispatcher.registry.removed",
                namespace = %entry.namespace,
                conversation_id = %entry.conversation_id,
                "registry entry removed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DeliveryBackendKind;
    use serde_json::json;
    use std::time::Duration;
    use tempfile::TempDir;

    fn make_config(registry_path: PathBuf, socket_path: PathBuf) -> MailServerConfig {
        MailServerConfig {
            namespace: "test".to_string(),
            codex_home: registry_path.parent().unwrap().to_path_buf(),
            socket_path,
            registry_path,
            ack_timeout: Duration::from_secs(1),
            connect_timeout: Duration::from_millis(100),
            retry_backoff: vec![Duration::from_millis(10)],
            registry_poll_interval: Duration::from_millis(50),
            max_inflight: 4,
            delivery_backend: DeliveryBackendKind::UnixSocket,
            broker: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_watcher_tracks_updates() -> Result<()> {
        let dir = TempDir::new().context("create temp dir")?;
        let registry_path = dir.path().join("registry.json");
        let socket_path = dir.path().join("mailbox.sock");
        tokio::fs::write(&socket_path, &[])
            .await
            .context("create socket placeholder")?;

        let conversation_id = Uuid::now_v7();
        let entry = json!({
            "version": 1,
            "entries": {
                conversation_id.to_string(): {
                    "session_id": conversation_id.to_string(),
                    "pid": 1234,
                    "socket_path": socket_path.to_string_lossy(),
                    "namespace": "test"
                }
            }
        });
        tokio::fs::write(&registry_path, serde_json::to_vec_pretty(&entry)?)
            .await
            .context("write registry")?;

        let config = make_config(registry_path.clone(), dir.path().join("dispatcher.sock"));
        let watcher = RegistryWatcher::new(&config).await?;

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(watcher.lookup(&conversation_id).is_some());

        let empty = json!({
            "version": 1,
            "entries": {}
        });
        tokio::fs::write(&registry_path, serde_json::to_vec_pretty(&empty)?)
            .await
            .context("clear registry")?;

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(watcher.lookup(&conversation_id).is_none());

        Ok(())
    }
}
