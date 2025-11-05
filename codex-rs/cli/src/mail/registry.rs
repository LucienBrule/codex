use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

const REGISTRY_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MailboxRegistryEntry {
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub conversation_id: Option<Uuid>,
    pub socket_path: PathBuf,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub last_heartbeat: Option<String>,
    #[serde(default)]
    pub registered_at: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MailboxRegistryFile {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    entries: BTreeMap<Uuid, MailboxRegistryEntry>,
}

#[derive(Debug)]
pub struct MailboxRegistry {
    path: PathBuf,
    data: MailboxRegistryFile,
}

impl MailboxRegistryFile {
    fn empty(namespace: &str) -> Self {
        Self {
            version: REGISTRY_VERSION,
            namespace: namespace.to_string(),
            updated_at: None,
            entries: BTreeMap::new(),
        }
    }

    fn normalize(&mut self) {
        for (id, entry) in &mut self.entries {
            entry.session_id.get_or_insert(*id);
            entry.conversation_id.get_or_insert(*id);
        }
    }
}

impl MailboxRegistry {
    pub fn load(path: PathBuf, namespace: String) -> Result<Self> {
        let mut data = match fs::read(&path) {
            Ok(bytes) => {
                if bytes.is_empty() {
                    MailboxRegistryFile::empty(&namespace)
                } else {
                    serde_json::from_slice::<MailboxRegistryFile>(&bytes).with_context(|| {
                        format!("failed to parse mailbox registry at {}", path.display())
                    })?
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                MailboxRegistryFile::empty(&namespace)
            }
            Err(err) => {
                return Err(anyhow::Error::from(err).context(format!(
                    "failed to read mailbox registry at {}",
                    path.display()
                )));
            }
        };

        data.namespace = namespace.clone();
        data.version = REGISTRY_VERSION;
        data.normalize();

        Ok(MailboxRegistry { path, data })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn namespace(&self) -> &str {
        &self.data.namespace
    }

    pub fn find(&self, conversation_id: &Uuid) -> Option<&MailboxRegistryEntry> {
        self.data.entries.get(conversation_id)
    }

    pub fn remove_if_socket_matches(&mut self, conversation_id: &Uuid, socket_path: &Path) -> bool {
        let should_remove = self
            .data
            .entries
            .get(conversation_id)
            .is_some_and(|entry| entry.socket_path == socket_path);

        if should_remove {
            self.data.entries.remove(conversation_id);
        }

        should_remove
    }

    pub fn remove(&mut self, conversation_id: &Uuid) -> bool {
        self.data.entries.remove(conversation_id).is_some()
    }

    pub fn purge_where<F>(&mut self, mut predicate: F) -> Vec<Uuid>
    where
        F: FnMut(&Uuid, &MailboxRegistryEntry) -> bool,
    {
        let to_remove: Vec<Uuid> = self
            .data
            .entries
            .iter()
            .filter_map(|(id, entry)| {
                if predicate(id, entry) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        for id in &to_remove {
            self.data.entries.remove(id);
        }

        to_remove
    }

    pub fn insert(&mut self, entry: MailboxRegistryEntry) {
        let key = entry
            .session_id
            .or(entry.conversation_id)
            .unwrap_or_else(Uuid::now_v7);
        let mut entry = entry;
        entry.session_id.get_or_insert(key);
        entry.conversation_id.get_or_insert(key);
        self.data.entries.insert(key, entry);
    }

    pub fn save(&mut self) -> Result<()> {
        self.data.version = REGISTRY_VERSION;
        self.data.updated_at = Some(timestamp_now());
        self.write_atomic()
    }

    fn write_atomic(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create mailbox directory {}", parent.display())
            })?;
        }

        let serialized = serde_json::to_vec_pretty(&self.data)
            .context("failed to serialize mailbox registry")?;

        let tmp_path = self.path.with_file_name(format!(
            "{}.tmp-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("registry.json"),
            Uuid::now_v7()
        ));

        fs::write(&tmp_path, &serialized).with_context(|| {
            format!(
                "failed to write temporary mailbox registry {}",
                tmp_path.display()
            )
        })?;

        fs::rename(&tmp_path, &self.path).with_context(|| {
            format!(
                "failed to commit mailbox registry to {}",
                self.path.display()
            )
        })
    }
}

fn default_version() -> u32 {
    REGISTRY_VERSION
}

fn timestamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
