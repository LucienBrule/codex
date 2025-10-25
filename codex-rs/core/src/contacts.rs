use regex_lite::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
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
        return Ok(Contacts {
            map,
            effective_paths,
        });
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

    Ok(Contacts {
        map,
        effective_paths,
    })
}

fn load_into_map(
    out: &mut HashMap<String, Uuid>,
    contacts: &HashMap<String, toml::Value>,
    key_re: &Regex,
) -> anyhow::Result<()> {
    for (key, value) in contacts.iter() {
        if !key_re.is_match(key) {
            anyhow::bail!("invalid contact key '{key}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$");
        }
        match value {
            toml::Value::String(uuid_str) => {
                let id = Uuid::parse_str(uuid_str)
                    .map_err(|_| anyhow::anyhow!("invalid UUID for contact '{key}'"))?;
                out.insert(key.clone(), id);
            }
            toml::Value::Table(tbl) => {
                // Convert table into struct for validation
                let entry: ContactEntryTable =
                    ContactEntryTable::deserialize(toml::Value::Table(tbl.clone()))?;
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
                        let new_prefix = if prefix.is_empty() {
                            k.clone()
                        } else {
                            format!("{prefix}.{k}")
                        };
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicMember {
    Contact(String),
    Conversation(Uuid),
    Topic(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDefinition {
    pub description: Option<String>,
    pub members: Vec<TopicMember>,
}

impl TopicDefinition {
    pub fn members(&self) -> &[TopicMember] {
        &self.members
    }
}

#[derive(Debug, Clone)]
pub struct Topics {
    pub map: HashMap<String, TopicDefinition>,
    pub effective_paths: Vec<PathBuf>,
}

impl Topics {
    pub fn get(&self, name: &str) -> Option<&TopicDefinition> {
        self.map.get(name)
    }

    pub fn resolve_to_conversation_ids(
        &self,
        topic: &str,
        contacts: &Contacts,
    ) -> anyhow::Result<Vec<Uuid>> {
        let mut visiting: HashSet<String> = HashSet::new();
        let mut seen: HashSet<Uuid> = HashSet::new();
        let mut resolved: Vec<Uuid> = Vec::new();
        self.resolve_recursive(topic, contacts, &mut visiting, &mut seen, &mut resolved)?;
        Ok(resolved)
    }

    fn resolve_recursive(
        &self,
        topic: &str,
        contacts: &Contacts,
        visiting: &mut HashSet<String>,
        seen: &mut HashSet<Uuid>,
        resolved: &mut Vec<Uuid>,
    ) -> anyhow::Result<()> {
        if !visiting.insert(topic.to_string()) {
            anyhow::bail!("topics contain recursive membership cycle at '{topic}'");
        }

        let outcome = (|| -> anyhow::Result<()> {
            let definition = self
                .map
                .get(topic)
                .ok_or_else(|| anyhow::anyhow!("topic '{topic}' is not defined"))?;

            for member in &definition.members {
                match member {
                    TopicMember::Conversation(id) => {
                        if seen.insert(*id) {
                            resolved.push(*id);
                        }
                    }
                    TopicMember::Contact(contact_name) => {
                        let id = contacts.resolve(contact_name).ok_or_else(|| {
                            anyhow::anyhow!(
                                "topic '{topic}' references contact '{contact_name}' that is not defined"
                            )
                        })?;
                        if seen.insert(id) {
                            resolved.push(id);
                        }
                    }
                    TopicMember::Topic(child_topic) => {
                        self.resolve_recursive(child_topic, contacts, visiting, seen, resolved)?
                    }
                }
            }
            Ok(())
        })();

        visiting.remove(topic);
        outcome
    }
}

#[derive(Debug, Deserialize)]
struct TopicsTomlRoot {
    #[serde(default)]
    topics: toml::value::Table,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TopicEntryTable {
    #[serde(default)]
    description: Option<String>,
    members: Vec<TopicMemberRaw>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TopicMemberRaw {
    Contact { contact: String },
    Topic { topic: String },
    Conversation { conversation_id: String },
}

pub fn topics_paths(codex_home: &Path, namespace: &str) -> (PathBuf, PathBuf) {
    let primary = codex_home.join(namespace).join("topics.toml");
    let global = codex_home.join("topics.toml");
    (primary, global)
}

pub fn load_topics(codex_home: &Path, namespace: &str) -> anyhow::Result<Topics> {
    let key_re = Regex::new(r"^[a-z0-9][a-z0-9.-]{2,100}$").unwrap();
    let mut map: HashMap<String, TopicDefinition> = HashMap::new();
    let mut effective_paths: Vec<PathBuf> = Vec::new();

    if let Ok(override_path) = std::env::var("CODEX_TOPICS_FILE") {
        let p = PathBuf::from(override_path);
        if p.exists() {
            let contents = fs::read_to_string(&p)?;
            let root: TopicsTomlRoot = toml::from_str(&contents)?;
            load_topics_table_flat(&mut map, &root.topics, &key_re)?;
            validate_topic_graph(&map)?;
            effective_paths.push(p);
        }
        return Ok(Topics {
            map,
            effective_paths,
        });
    }

    let (primary, global) = topics_paths(codex_home, namespace);

    if global.exists() {
        let contents = fs::read_to_string(&global)?;
        let root: TopicsTomlRoot = toml::from_str(&contents)?;
        load_topics_table_flat(&mut map, &root.topics, &key_re)?;
        effective_paths.push(global.clone());
    }

    if primary.exists() {
        let contents = fs::read_to_string(&primary)?;
        let root: TopicsTomlRoot = toml::from_str(&contents)?;
        load_topics_table_flat(&mut map, &root.topics, &key_re)?;
        effective_paths.push(primary.clone());
    }

    validate_topic_graph(&map)?;

    Ok(Topics {
        map,
        effective_paths,
    })
}

fn load_topics_table_flat(
    out: &mut HashMap<String, TopicDefinition>,
    topics: &toml::value::Table,
    key_re: &Regex,
) -> anyhow::Result<()> {
    fn is_topic_entry(tbl: &toml::value::Table) -> bool {
        tbl.contains_key("members") || tbl.contains_key("description")
    }

    fn visit(
        out: &mut HashMap<String, TopicDefinition>,
        key_re: &Regex,
        prefix: &str,
        value: &toml::Value,
    ) -> anyhow::Result<()> {
        match value {
            toml::Value::Table(tbl) => {
                if is_topic_entry(tbl) {
                    if !key_re.is_match(prefix) {
                        anyhow::bail!(
                            "invalid topic key '{prefix}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                        );
                    }
                    let entry: TopicEntryTable =
                        TopicEntryTable::deserialize(toml::Value::Table(tbl.clone()))?;
                    if entry.members.is_empty() {
                        anyhow::bail!("topic '{prefix}' must declare at least one member");
                    }
                    let mut members = Vec::with_capacity(entry.members.len());
                    for raw_member in entry.members {
                        members.push(convert_topic_member(prefix, raw_member, key_re)?);
                    }
                    out.insert(
                        prefix.to_string(),
                        TopicDefinition {
                            description: entry.description,
                            members,
                        },
                    );
                    Ok(())
                } else {
                    for (k, v) in tbl.iter() {
                        let new_prefix = if prefix.is_empty() {
                            k.clone()
                        } else {
                            format!("{prefix}.{k}")
                        };
                        visit(out, key_re, &new_prefix, v)?;
                    }
                    Ok(())
                }
            }
            other => {
                anyhow::bail!(
                    "unsupported topic entry type for '{prefix}': expected table, got {other:?}"
                );
            }
        }
    }

    for (k, v) in topics.iter() {
        visit(out, key_re, k, v)?;
    }

    Ok(())
}

fn convert_topic_member(
    topic_name: &str,
    raw: TopicMemberRaw,
    key_re: &Regex,
) -> anyhow::Result<TopicMember> {
    match raw {
        TopicMemberRaw::Contact { contact } => {
            if !key_re.is_match(&contact) {
                anyhow::bail!(
                    "invalid contact member '{contact}' in topic '{topic_name}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                );
            }
            Ok(TopicMember::Contact(contact))
        }
        TopicMemberRaw::Topic { topic } => {
            if !key_re.is_match(&topic) {
                anyhow::bail!(
                    "invalid nested topic member '{topic}' in topic '{topic_name}': must match ^[a-z0-9][a-z0-9.-]{{2,100}}$"
                );
            }
            Ok(TopicMember::Topic(topic))
        }
        TopicMemberRaw::Conversation { conversation_id } => {
            let id = Uuid::parse_str(&conversation_id).map_err(|_| {
                anyhow::anyhow!(
                    "invalid UUID '{conversation_id}' in topic '{topic_name}' conversation_id member"
                )
            })?;
            Ok(TopicMember::Conversation(id))
        }
    }
}

fn validate_topic_graph(map: &HashMap<String, TopicDefinition>) -> anyhow::Result<()> {
    for (topic, definition) in map.iter() {
        for member in &definition.members {
            if let TopicMember::Topic(child) = member {
                if child == topic {
                    anyhow::bail!("topic '{topic}' cannot include itself as a member");
                }
                if !map.contains_key(child) {
                    anyhow::bail!("topic '{topic}' references unknown topic '{child}'");
                }
            }
        }
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut visiting: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = Vec::new();

    for topic in map.keys() {
        if !visited.contains(topic) {
            detect_topic_cycle(topic, map, &mut visited, &mut visiting, &mut stack)?;
        }
    }

    Ok(())
}

fn detect_topic_cycle(
    current: &str,
    map: &HashMap<String, TopicDefinition>,
    visited: &mut HashSet<String>,
    visiting: &mut HashSet<String>,
    stack: &mut Vec<String>,
) -> anyhow::Result<()> {
    visiting.insert(current.to_string());
    stack.push(current.to_string());

    if let Some(definition) = map.get(current) {
        for member in &definition.members {
            if let TopicMember::Topic(child) = member {
                if visiting.contains(child) {
                    let mut cycle = stack.clone();
                    cycle.push(child.clone());
                    let path = cycle.join(" -> ");
                    anyhow::bail!("topics contain recursive membership cycle: {path}");
                }
                if !visited.contains(child) {
                    detect_topic_cycle(child, map, visited, visiting, stack)?;
                }
            }
        }
    }

    visiting.remove(current);
    visited.insert(current.to_string());
    stack.pop();
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
            contacts.resolve("impl.codex.search").unwrap().to_string(),
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

    #[test]
    fn load_topics_with_overlay_and_resolution() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path();
        let ns = "codex";
        let ns_dir = home.join(ns);
        fs::create_dir_all(&ns_dir).unwrap();

        fs::write(
            home.join("topics.toml"),
            r#"
[topics.story]
  [topics.story.team]
  description = "Team members via conversation ids"
  members = [
    { conversation_id = "00000000-0000-4000-8000-000000000000" },
    { topic = "story.ops" }
  ]

  [topics.story.ops]
  members = [
    { conversation_id = "00000000-0000-4000-9000-000000000000" }
  ]

  [topics.story.all]
  members = [
    { topic = "story.team" },
    { topic = "story.ops" }
  ]
"#,
        )
        .unwrap();

        fs::write(
            ns_dir.join("topics.toml"),
            r#"
[topics.story]
  [topics.story.team]
  description = "Override team with contacts"
  members = [
    { contact = "impl.codex.search" },
    { conversation_id = "00000000-0000-4000-7000-000000000000" }
  ]
"#,
        )
        .unwrap();

        let contacts = Contacts {
            map: HashMap::from([
                (
                    "impl.codex.search".to_string(),
                    Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
                ),
                (
                    "qa.worker".to_string(),
                    Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
                ),
            ]),
            effective_paths: vec![],
        };

        let topics = load_topics(home, ns).unwrap();

        let resolved = topics
            .resolve_to_conversation_ids("story.all", &contacts)
            .unwrap();

        assert_eq!(
            resolved,
            vec![
                Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
                Uuid::parse_str("00000000-0000-4000-7000-000000000000").unwrap(),
                Uuid::parse_str("00000000-0000-4000-9000-000000000000").unwrap()
            ]
        );
    }

    #[test]
    fn load_topics_rejects_unknown_nested_topics() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path();

        fs::write(
            home.join("topics.toml"),
            r#"
[topics.invalid]
  [topics.invalid.bad]
  members = [
    { topic = "missing.topic" }
  ]
"#,
        )
        .unwrap();

        let err = load_topics(home, "codex").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("references unknown topic"));
    }

    #[test]
    fn load_topics_rejects_cycles() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path();

        fs::write(
            home.join("topics.toml"),
            r#"
[topics.chain]
  [topics.chain.alpha]
  members = [
    { topic = "chain.beta" }
  ]

  [topics.chain.beta]
  members = [
    { topic = "chain.alpha" }
  ]
"#,
        )
        .unwrap();

        let err = load_topics(home, "codex").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("recursive membership cycle"));
    }

    #[test]
    fn resolve_topics_requires_contacts_for_contact_members() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path();

        fs::write(
            home.join("topics.toml"),
            r#"
[topics.simple]
  [topics.simple.team]
  members = [
    { contact = "unknown.contact" }
  ]
"#,
        )
        .unwrap();

        let topics = load_topics(home, "codex").unwrap();

        let contacts = Contacts {
            map: HashMap::new(),
            effective_paths: vec![],
        };

        let err = topics
            .resolve_to_conversation_ids("simple.team", &contacts)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("references contact 'unknown.contact'"));
    }
}
