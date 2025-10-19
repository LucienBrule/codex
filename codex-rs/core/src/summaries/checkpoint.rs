use std::path::{Path, PathBuf};

use codex_protocol::ConversationId;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Default runtime summaries directory.
///
/// Historically, checkpoints were stored in a single, non-namespaced path.
/// As part of CS hardening, we now resolve a base directory with the following
/// precedence (first match wins):
/// 1) `$CODEX_SUMMARIES_BASE_DIR` (when set and non-empty)
/// 2) Config `[summaries].base_dir` (when set)
/// 3) `$XDG_RUNTIME_DIR/cx/<namespace>/summaries`
/// 4) `$XDG_RUNTIME_DIR/<namespace>/summaries`
/// 5) Legacy default: `/run/user/1000/cx/summaries`
///
/// Note: The namespace comes from `$CODEX_NAMESPACE` and defaults to `codex`.
const DEFAULT_BASE_DIR: &str = "/run/user/1000/cx/summaries";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SummaryCheckpoint {
    summary: String,
    /// ISO-8601 UTC timestamp when this checkpoint was written.
    updated_at: String,
}

fn resolve_namespace() -> String {
    std::env::var("CODEX_NAMESPACE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "codex".to_string())
}

fn default_runtime_dir_from_const() -> PathBuf {
    // DEFAULT_BASE_DIR is "/run/user/1000/cx/summaries" -> runtime dir is two parents up.
    let p = Path::new(DEFAULT_BASE_DIR);
    p.parent()
        .and_then(|q| q.parent())
        .map(|r| r.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/run/user/1000"))
}

fn base_dir_candidates() -> Vec<PathBuf> {
    // 1) Explicit env and 2) config override
    if let Some(override_dir) = crate::config::summaries_base_dir_override() {
        return vec![override_dir];
    }

    // Resolve namespace and runtime dir
    let ns = resolve_namespace();
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(default_runtime_dir_from_const);

    let mut out = Vec::with_capacity(3);
    out.push(runtime_dir.join("cx").join(&ns).join("summaries"));
    out.push(runtime_dir.join(&ns).join("summaries"));
    out.push(PathBuf::from(DEFAULT_BASE_DIR));
    out
}

fn base_dir() -> PathBuf {
    // Use the highest-precedence candidate. Reads will try subsequent fallbacks.
    base_dir_candidates()
        .into_iter()
        .next()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_BASE_DIR))
}

fn checkpoint_dir_for_base(base: &Path, conversation_id: &ConversationId) -> PathBuf {
    base.join(conversation_id.to_string())
}

/// Returns the final path to the checkpoint file and a temporary path used during atomic writes.
pub(crate) fn checkpoint_paths_for_base(
    base: &Path,
    conversation_id: &ConversationId,
) -> (PathBuf, PathBuf) {
    let dir = checkpoint_dir_for_base(base, conversation_id);
    let final_path = dir.join("summary.json");
    let tmp_path = dir.join("summary.json.part");
    (final_path, tmp_path)
}

/// Public helper to compute the default checkpoint path for this conversation.
pub(crate) fn checkpoint_path(conversation_id: &ConversationId) -> PathBuf {
    let (p, _) = checkpoint_paths_for_base(&base_dir(), conversation_id);
    p
}

/// Atomically write a checkpoint for `conversation_id` under the default base dir.
pub(crate) async fn write_summary_checkpoint(
    conversation_id: &ConversationId,
    summary: &str,
) -> std::io::Result<()> {
    write_summary_checkpoint_with_base(&base_dir(), conversation_id, summary).await
}

/// Atomically write a checkpoint for `conversation_id` under a custom `base` dir.
///
/// Useful for tests.
pub(crate) async fn write_summary_checkpoint_with_base(
    base: &Path,
    conversation_id: &ConversationId,
    summary: &str,
) -> std::io::Result<()> {
    let (final_path, tmp_path) = checkpoint_paths_for_base(base, conversation_id);
    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Compose the checkpoint payload with a timestamp.
    let payload = SummaryCheckpoint {
        summary: summary.to_string(),
        updated_at: OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "<invalid>".to_string()),
    };
    let json = serde_json::to_vec(&payload).expect("serializing checkpoint");

    // Write to a temporary file then atomically rename into place.
    tokio::fs::write(&tmp_path, json).await?;
    tokio::fs::rename(&tmp_path, &final_path).await?;
    Ok(())
}

/// Read a checkpoint for `conversation_id` from the default base dir.
pub(crate) async fn read_summary_checkpoint(
    conversation_id: &ConversationId,
) -> std::io::Result<Option<String>> {
    // Attempt read using resolver precedence for backwards compatibility.
    for base in base_dir_candidates() {
        match read_summary_checkpoint_with_base(&base, conversation_id).await {
            Ok(Some(s)) => return Ok(Some(s)),
            Ok(None) => {
                // Not found at this base; try next candidate.
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Continue fallback chain.
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

/// Read a checkpoint for `conversation_id` from a custom `base` dir.
/// Returns `Ok(None)` when the checkpoint is missing.
pub(crate) async fn read_summary_checkpoint_with_base(
    base: &Path,
    conversation_id: &ConversationId,
) -> std::io::Result<Option<String>> {
    let (final_path, _tmp_path) = checkpoint_paths_for_base(base, conversation_id);
    match tokio::fs::read(&final_path).await {
        Ok(bytes) => {
            let checkpoint: SummaryCheckpoint = serde_json::from_slice(&bytes)
                .map_err(|e| std::io::Error::other(format!(
                    "failed to parse summary checkpoint: {e}"
                )))?;
            Ok(Some(checkpoint.summary))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod summaries_checkpoint {
    use super::*;
    use tempfile::tempdir;
    use serial_test::serial;

    #[tokio::test]
    async fn path_builder() {
        let id = ConversationId::from_string("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
        let base = tempdir().unwrap();
        let (final_path, tmp_path) = checkpoint_paths_for_base(base.path(), &id);
        assert!(final_path.ends_with("67e55044-10b1-426f-9247-bb680e5fe0c8/summary.json"));
        assert!(tmp_path.ends_with("67e55044-10b1-426f-9247-bb680e5fe0c8/summary.json.part"));
    }

    #[tokio::test]
    async fn write_and_read_roundtrip() {
        let id = ConversationId::default();
        let base = tempdir().unwrap();
        write_summary_checkpoint_with_base(base.path(), &id, "hello")
            .await
            .unwrap();
        let read = read_summary_checkpoint_with_base(base.path(), &id)
            .await
            .unwrap();
        assert_eq!(read.as_deref(), Some("hello"));
    }

    #[test]
    #[serial(summaries_checkpoint)]
    fn default_checkpoint_path_is_namespaced_under_cx() {
        // Ensure no explicit override interferes
        unsafe { std::env::remove_var("CODEX_SUMMARIES_BASE_DIR"); }
        // Force deterministic runtime dir
        unsafe { std::env::set_var("XDG_RUNTIME_DIR", "/run/user/1000"); }
        unsafe { std::env::set_var("CODEX_NAMESPACE", "purple"); }
        let id = ConversationId::from_string("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
        let p = checkpoint_path(&id);
        let expected = PathBuf::from("/run/user/1000/cx/purple/summaries")
            .join(id.to_string())
            .join("summary.json");
        assert_eq!(p, expected);
    }

    #[test]
    #[serial(summaries_checkpoint)]
    fn env_override_wins() {
        let tmp = tempdir().unwrap();
        unsafe { std::env::set_var("CODEX_SUMMARIES_BASE_DIR", tmp.path()); }
        unsafe { std::env::set_var("CODEX_NAMESPACE", "ignored"); } // should be ignored when override is set
        let id = ConversationId::from_string("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
        let p = checkpoint_path(&id);
        assert!(p.starts_with(tmp.path()));
        assert!(p.ends_with("67e55044-10b1-426f-9247-bb680e5fe0c8/summary.json"));
        unsafe { std::env::remove_var("CODEX_SUMMARIES_BASE_DIR"); }
    }

    #[test]
    #[serial(summaries_checkpoint)]
    fn config_override_wins() {
        // Prepare a temporary CODEX_HOME with a config override
        let home = tempdir().unwrap();
        let base = tempdir().unwrap();
        let config_path = home.path().join(crate::config::CONFIG_TOML_FILE);
        std::fs::write(
            &config_path,
            format!("[summaries]\nbase_dir = \"{}\"\n", base.path().display()),
        )
        .unwrap();
        unsafe { std::env::set_var("CODEX_HOME", home.path()); }
        unsafe { std::env::remove_var("CODEX_SUMMARIES_BASE_DIR"); }
        unsafe { std::env::set_var("CODEX_NAMESPACE", "ignored"); }

        let id = ConversationId::from_string("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
        let p = checkpoint_path(&id);
        assert!(p.starts_with(base.path()));
        assert!(p.ends_with("67e55044-10b1-426f-9247-bb680e5fe0c8/summary.json"));
    }
}
