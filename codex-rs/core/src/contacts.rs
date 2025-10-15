use regex_lite::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Contacts {
    pub map: HashMap<String, Uuid>,
    pub effective_paths: Vec<PathBuf>,
}

impl Contacts {
    pub fn resolve(&self, name: &str) -> Option<Uuid> {
        self.map.get(name).copied()
    }
}

#[derive(Debug, Deserialize)]
struct ContactsTomlRoot {
    #[serde(default)]
    contacts: toml::value::Table,
}

#[derive(Debug, Deserialize)]
struct ContactEntryTable {
    conversation_id: String,
    #[allow(dead_code)]
    description: Option<String>,
    #[allow(dead_code)]
    aliases: Option<Vec<String>>,
}

fn resolve_namespace() -> String {
    std::env::var("CODEX_NAMESPACE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "codex".to_string())
}

pub fn contacts_paths(codex_home: &Path, namespace: &str) -> (PathBuf, PathBuf) {
    let primary = codex_home.join(namespace).join("contacts.toml");
    let global = codex_home.join("contacts.toml");
    (primary, global)
}

/// Load contacts with overlay precedence: namespaced file overrides global.
/// If CODEX_CONTACTS_FILE is set, use only that file.
pub fn load_contacts(codex_home: &Path, namespace: &str) -> anyhow::Result<Contacts> {
    let key_re = Regex::new(r"^[a-z0-9][a-z0-9.-]{2,100}$").unwrap();
    let mut map: HashMap<String, Uuid> = HashMap::new();
    let mut effective_paths: Vec<PathBuf> = Vec::new();

    if let Ok(override_path) = std::env::var("CODEX_CONTACTS_FILE") {
        let p = PathBuf::from(override_path);
        if p.exists() {
            let contents = fs::read_to_string(&p)?;
            let root: ContactsTomlRoot = toml::from_str(&contents)?;
            load_table_into_map_flat(&mut map, &root.contacts, &key_re)?;
            effective_paths.push(p);
        }
        return Ok(Contacts { map, effective_paths });
    }

    let (primary, global) = contacts_paths(codex_home, namespace);

    if global.exists() {
        let contents = fs::read_to_string(&global)?;
        let root: ContactsTomlRoot = toml::from_str(&contents)?;
        load_table_into_map_flat(&mut map, &root.contacts, &key_re)?;
        effective_paths.push(global.clone());
    }

    if primary.exists() {
        let contents = fs::read_to_string(&primary)?;
        let root: ContactsTomlRoot = toml::from_str(&contents)?;
        // namespaced entries override global
        load_table_into_map_flat(&mut map, &root.contacts, &key_re)?;
        effective_paths.push(primary.clone());
    }

    Ok(Contacts { map, effective_paths })
}

fn load_into_map(
    out: &mut HashMap<String, Uuid>,
    contacts: &HashMap<String, toml::Value>,
    key_re: &Regex,
) -> anyhow::Result<()> {
    for (key, value) in contacts.iter() {
        if !key_re.is_match(key) {
            anyhow::bail!(
                "invalid contact key '{key}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
            );
        }
        match value {
            toml::Value::String(uuid_str) => {
                let id = Uuid::parse_str(uuid_str)
                    .map_err(|_| anyhow::anyhow!("invalid UUID for contact '{key}'"))?;
                out.insert(key.clone(), id);
            }
            toml::Value::Table(tbl) => {
                // Convert table into struct for validation
                let entry: ContactEntryTable = ContactEntryTable::deserialize(toml::Value::Table(tbl.clone()))?;
                let id = Uuid::parse_str(&entry.conversation_id).map_err(|_| {
                    anyhow::anyhow!("invalid UUID for contact '{key}' in table 'conversation_id'")
                })?;
                out.insert(key.clone(), id);
                if let Some(aliases) = entry.aliases {
                    for alias in aliases {
                        if !key_re.is_match(&alias) {
                            anyhow::bail!(
                                "invalid alias '{alias}' for contact '{key}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                            );
                        }
                        out.insert(alias, id);
                    }
                }
            }
            other => {
                anyhow::bail!("unsupported contact entry type for '{key}': {other:?}");
            }
        }
    }
    Ok(())
}

// Flatten dotted keys (expanded by TOML into nested tables) and populate `out`.
fn load_table_into_map_flat(
    out: &mut HashMap<String, Uuid>,
    contacts: &toml::value::Table,
    key_re: &Regex,
) -> anyhow::Result<()> {
    fn is_contact_entry_table(tbl: &toml::value::Table) -> bool {
        tbl.contains_key("conversation_id")
            || tbl.contains_key("description")
            || tbl.contains_key("aliases")
    }

    fn visit(
        out: &mut HashMap<String, Uuid>,
        key_re: &Regex,
        prefix: &str,
        value: &toml::Value,
    ) -> anyhow::Result<()> {
        match value {
            toml::Value::String(uuid_str) => {
                if !key_re.is_match(prefix) {
                    anyhow::bail!(
                        "invalid contact key '{prefix}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                    );
                }
                let id = Uuid::parse_str(uuid_str)
                    .map_err(|_| anyhow::anyhow!("invalid UUID for contact '{prefix}'"))?;
                out.insert(prefix.to_string(), id);
                Ok(())
            }
            toml::Value::Table(tbl) => {
                if is_contact_entry_table(tbl) {
                    if !key_re.is_match(prefix) {
                        anyhow::bail!(
                            "invalid contact key '{prefix}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                        );
                    }
                    let entry: ContactEntryTable =
                        ContactEntryTable::deserialize(toml::Value::Table(tbl.clone()))?;
                    let id = Uuid::parse_str(&entry.conversation_id).map_err(|_| {
                        anyhow::anyhow!(
                            "invalid UUID for contact '{prefix}' in table 'conversation_id'"
                        )
                    })?;
                    out.insert(prefix.to_string(), id);
                    if let Some(aliases) = entry.aliases {
                        for alias in aliases {
                            if !key_re.is_match(&alias) {
                                anyhow::bail!(
                                    "invalid alias '{alias}' for contact '{prefix}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                                );
                            }
                            out.insert(alias, id);
                        }
                    }
                    Ok(())
                } else {
                    for (k, v) in tbl.iter() {
                        let new_prefix = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                        visit(out, key_re, &new_prefix, v)?;
                    }
                    Ok(())
                }
            }
            other => {
                if !prefix.is_empty() {
                    anyhow::bail!("unsupported contact entry type for '{prefix}': {other:?}");
                }
                Ok(())
            }
        }
    }

    for (k, v) in contacts.iter() {
        visit(out, key_re, k, v)?;
    }

    Ok(())
}

/// Utility for MCP: returns (codex_home, namespace)
pub fn get_runtime_home_and_namespace() -> (PathBuf, String) {
    let codex_home = crate::config::find_codex_home().unwrap_or_else(|_| std::env::temp_dir());
    let ns = resolve_namespace();
    (codex_home, ns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_flat_and_table_with_overlay_and_aliases() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path();
        // global contacts
        fs::write(
            home.join("contacts.toml"),
            r#"
[contacts]
impl.codex.search = "550e8400-e29b-41d4-a716-446655440000"
qa.worker.alpha = { conversation_id = "b6e6a2b9-0f5f-4a63-a5d7-2f9d7f2e9a11", description = "QA Worker Alpha", aliases = ["qa.alpha", "qa-a"] }
"#,
        )
        .unwrap();

        // namespaced override
        let ns = "codex";
        let ns_dir = home.join(ns);
        fs::create_dir_all(&ns_dir).unwrap();
        fs::write(
            ns_dir.join("contacts.toml"),
            r#"
[contacts]
impl.codex.search = "11111111-1111-4111-8111-111111111111"
"#,
        )
        .unwrap();

        let contacts = load_contacts(home, ns).unwrap();
        assert_eq!(
            contacts
                .resolve("impl.codex.search")
                .unwrap()
                .to_string(),
            "11111111-1111-4111-8111-111111111111"
        );
        assert_eq!(
            contacts.resolve("qa.worker.alpha").unwrap().to_string(),
            "b6e6a2b9-0f5f-4a63-a5d7-2f9d7f2e9a11"
        );
        assert_eq!(
            contacts.resolve("qa.alpha").unwrap().to_string(),
            "b6e6a2b9-0f5f-4a63-a5d7-2f9d7f2e9a11"
        );
    }

    #[test]
    fn invalid_key_and_uuid_errors() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path();
        fs::write(
            home.join("contacts.toml"),
            r#"
[contacts]
BadKey = "not-a-uuid"
"#,
        )
        .unwrap();
        let err = load_contacts(home, "codex").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid contact key"));
    }
}
