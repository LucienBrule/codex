//! Persistence layer for the global, append-only *message history* file.
//!
//! The history is stored at `~/.codex/history.jsonl` with **one JSON object per
//! line** so that it can be efficiently appended to and parsed with standard
//! JSON-Lines tooling. Each record has the following schema:
//!
//! ````text
//! {"conversation_id":"<uuid>","ts":<unix_seconds>,"text":"<message>"}
//! ````
//!
//! To minimise the chance of interleaved writes when multiple processes are
//! appending concurrently, callers should *prepare the full line* (record +
//! trailing `\n`) and write it with a **single `write(2)` system call** while
//! the file descriptor is opened with the `O_APPEND` flag. POSIX guarantees
//! that writes up to `PIPE_BUF` bytes are atomic in that case.

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Result;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::config::Config;
use crate::config_types::HistoryPersistence;

use codex_protocol::ConversationId;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// Filename that stores the message history inside `~/.codex`.
const HISTORY_FILENAME: &str = "history.jsonl";

const MAX_RETRIES: usize = 10;
const RETRY_SLEEP: Duration = Duration::from_millis(100);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HistoryEntry {
    pub session_id: String,
    pub ts: u64,
    pub text: String,
}

fn history_filepath(config: &Config) -> PathBuf {
    let mut path = config.codex_home.clone();
    path.push(HISTORY_FILENAME);
    path
}

/// Append a `text` entry associated with `conversation_id` to the history file. Uses
/// advisory file locking to ensure that concurrent writes do not interleave,
/// which entails a small amount of blocking I/O internally.
pub(crate) async fn append_entry(
    text: &str,
    conversation_id: &ConversationId,
    config: &Config,
) -> Result<()> {
    match config.history.persistence {
        HistoryPersistence::SaveAll => {
            // Save everything: proceed.
        }
        HistoryPersistence::None => {
            // No history persistence requested.
            return Ok(());
        }
    }

    // TODO: check `text` for sensitive patterns

    // Resolve `~/.codex/history.jsonl` and ensure the parent directory exists.
    let path = history_filepath(config);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Compute timestamp (seconds since the Unix epoch).
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| std::io::Error::other(format!("system clock before Unix epoch: {e}")))?
        .as_secs();

    // Construct the JSON line first so we can write it in a single syscall.
    let entry = HistoryEntry {
        session_id: conversation_id.to_string(),
        ts,
        text: text.to_string(),
    };
    let mut line = serde_json::to_string(&entry)
        .map_err(|e| std::io::Error::other(format!("failed to serialise history entry: {e}")))?;
    line.push('\n');

    // Open in append-only mode.
    let mut options = OpenOptions::new();
    options.append(true).read(true).create(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }

    let mut history_file = options.open(&path)?;

    // Ensure permissions.
    ensure_owner_only_permissions(&history_file).await?;

    // Perform a blocking write under an advisory write lock using std::fs.
    let max_bytes_override = effective_max_bytes(config);
    tokio::task::spawn_blocking(move || -> Result<()> {
        // Retry a few times to avoid indefinite blocking when contended.
        for _ in 0..MAX_RETRIES {
            match history_file.try_lock() {
                Ok(()) => {
                    // While holding the exclusive lock, write the full line.
                    history_file.write_all(line.as_bytes())?;
                    history_file.flush()?;
                    // Enforce max-bytes cap if configured. Done while the
                    // exclusive lock is held to avoid races with other
                    // appenders. Uses an atomic rewrite strategy.
                    if let Some(max_bytes) = max_bytes_override {
                        if max_bytes > 0 {
                            enforce_max_bytes_locked(&mut history_file, &path, max_bytes)?;
                        }
                    }
                    return Ok(());
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    std::thread::sleep(RETRY_SLEEP);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "could not acquire exclusive lock on history file after multiple attempts",
        ))
    })
    .await??;

    Ok(())
}

/// Asynchronously fetch the history file's *identifier* (inode on Unix) and
/// the current number of entries by counting newline characters.
pub(crate) async fn history_metadata(config: &Config) -> (u64, usize) {
    let path = history_filepath(config);

    #[cfg(unix)]
    let log_id = {
        use std::os::unix::fs::MetadataExt;
        // Obtain metadata (async) to get the identifier.
        let meta = match fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, 0),
            Err(_) => return (0, 0),
        };
        meta.ino()
    };
    #[cfg(not(unix))]
    let log_id = 0u64;

    // Open the file.
    let mut file = match fs::File::open(&path).await {
        Ok(f) => f,
        Err(_) => return (log_id, 0),
    };

    // Count newline bytes.
    let mut buf = [0u8; 8192];
    let mut count = 0usize;
    loop {
        match file.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                count += buf[..n].iter().filter(|&&b| b == b'\n').count();
            }
            Err(_) => return (log_id, 0),
        }
    }

    (log_id, count)
}

/// Given a `log_id` (on Unix this is the file's inode number) and a zero-based
/// `offset`, return the corresponding `HistoryEntry` if the identifier matches
/// the current history file **and** the requested offset exists. Any I/O or
/// parsing errors are logged and result in `None`.
///
/// Note this function is not async because it uses a sync advisory file
/// locking API.
#[cfg(unix)]
pub(crate) fn lookup(log_id: u64, offset: usize, config: &Config) -> Option<HistoryEntry> {
    use std::io::BufRead;
    use std::io::BufReader;
    use std::os::unix::fs::MetadataExt;

    let path = history_filepath(config);
    let file: File = match OpenOptions::new().read(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(error = %e, "failed to open history file");
            return None;
        }
    };

    let metadata = match file.metadata() {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "failed to stat history file");
            return None;
        }
    };

    if metadata.ino() != log_id {
        return None;
    }

    // Open & lock file for reading using a shared lock.
    // Retry a few times to avoid indefinite blocking.
    for _ in 0..MAX_RETRIES {
        let lock_result = file.try_lock_shared();

        match lock_result {
            Ok(()) => {
                let reader = BufReader::new(&file);
                for (idx, line_res) in reader.lines().enumerate() {
                    let line = match line_res {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to read line from history file");
                            return None;
                        }
                    };

                    if idx == offset {
                        match serde_json::from_str::<HistoryEntry>(&line) {
                            Ok(entry) => return Some(entry),
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to parse history entry");
                                return None;
                            }
                        }
                    }
                }
                // Not found at requested offset.
                return None;
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                std::thread::sleep(RETRY_SLEEP);
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to acquire shared lock on history file");
                return None;
            }
        }
    }

    None
}

/// Fallback stub for non-Unix systems: currently always returns `None`.
#[cfg(not(unix))]
pub(crate) fn lookup(log_id: u64, offset: usize, config: &Config) -> Option<HistoryEntry> {
    let _ = (log_id, offset, config);
    None
}

/// On Unix systems ensure the file permissions are `0o600` (rw-------). If the
/// permissions cannot be changed the error is propagated to the caller.
#[cfg(unix)]
async fn ensure_owner_only_permissions(file: &File) -> Result<()> {
    let metadata = file.metadata()?;
    let current_mode = metadata.permissions().mode() & 0o777;
    if current_mode != 0o600 {
        let mut perms = metadata.permissions();
        perms.set_mode(0o600);
        let perms_clone = perms.clone();
        let file_clone = file.try_clone()?;
        tokio::task::spawn_blocking(move || file_clone.set_permissions(perms_clone)).await??;
    }
    Ok(())
}

#[cfg(not(unix))]
async fn ensure_owner_only_permissions(_file: &File) -> Result<()> {
    // For now, on non-Unix, simply succeed.
    Ok(())
}

/// Determine the effective max-bytes cap for the history file.
///
/// Precedence:
/// - Environment variable `CODEX_HISTORY_MAX_BYTES` (if set and > 0)
/// - Config `history.max_bytes`
fn effective_max_bytes(config: &Config) -> Option<usize> {
    if let Ok(val) = std::env::var("CODEX_HISTORY_MAX_BYTES") {
        if let Ok(parsed) = val.trim().parse::<usize>() {
            if parsed > 0 {
                return Some(parsed);
            } else {
                return None; // zero/negative disables enforcement via env
            }
        }
    }
    config.history.max_bytes
}

/// While holding the exclusive file lock, enforce that the history file does
/// not exceed `max_bytes` by atomically rewriting it to keep only the trailing
/// bytes on a line boundary. If the trailing window does not contain a full
/// line (e.g., a single line longer than `max_bytes`), the file is truncated to
/// empty to preserve the cap and JSONL validity.
fn enforce_max_bytes_locked(file: &mut File, path: &Path, max_bytes: usize) -> Result<()> {
    let meta = file.metadata()?;
    let len = meta.len() as usize;
    if len <= max_bytes {
        return Ok(());
    }

    let start = len - max_bytes;
    // Read the last `max_bytes` bytes into memory.
    file.seek(SeekFrom::Start(start as u64))?;
    let mut buf = vec![0u8; max_bytes];
    let mut read_total = 0usize;
    while read_total < max_bytes {
        match file.read(&mut buf[read_total..]) {
            Ok(0) => break,
            Ok(n) => read_total += n,
            Err(e) => return Err(std::io::Error::other(format!("read tail: {e}"))),
        }
    }
    buf.truncate(read_total);

    // Drop any partial first line so we start on a newline boundary.
    let slice = if start > 0 {
        match buf.iter().position(|&b| b == b'\n') {
            Some(idx) => &buf[idx + 1..],
            None => &[][..],
        }
    } else {
        &buf[..]
    };

    // Write the trimmed tail to a temp file in the same directory and atomically
    // replace the original file.
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp_path = parent.join(".history.jsonl.tmp");

    // Create/truncate the tmp file with strict permissions.
    #[allow(unused_mut)]
    let mut tmp_opts = OpenOptions::new();
    tmp_opts.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        tmp_opts.mode(0o600);
    }
    let mut tmp_file = tmp_opts.open(&tmp_path)?;
    if !slice.is_empty() {
        tmp_file.write_all(slice)?;
        // Ensure the file ends with a newline so the last record is complete.
        if !slice.ends_with(b"\n") {
            tmp_file.write_all(b"\n")?;
        }
    } else {
        // Keep empty file if no complete lines fit in the window.
    }
    tmp_file.flush()?;
    tmp_file.sync_all().ok();

    // Atomically replace the original file path. The existing open file handle
    // (locked) continues to point at the old inode; subsequent opens will see
    // the new file.
    std::fs::rename(&tmp_path, path)?;

    Ok(())
}

#[cfg(test)]
mod history_max_bytes {
    use super::*;
    use crate::config::Config;
    use crate::config::ConfigOverrides;
    use crate::config::ConfigToml;
    use crate::config_types::History;
    use tempfile::TempDir;

    fn build_config_with_home(codex_home: &Path, max_bytes: Option<usize>) -> Config {
        let cfg = ConfigToml {
            history: Some(History {
                max_bytes,
                ..Default::default()
            }),
            ..Default::default()
        };
        Config::load_from_base_config_with_overrides(
            cfg,
            ConfigOverrides::default(),
            codex_home.to_path_buf(),
        )
        .expect("load config")
    }

    fn history_path(config: &Config) -> PathBuf {
        let mut p = config.codex_home.clone();
        p.push(HISTORY_FILENAME);
        p
    }

    #[tokio::test]
    async fn enforces_cap_after_append() -> Result<()> {
        let tmp = TempDir::new().unwrap();
        let cap = 256usize;
        let config = build_config_with_home(tmp.path(), Some(cap));
        let id = ConversationId::new();

        for i in 0..200u32 {
            let msg = format!("line {:04}", i);
            append_entry(&msg, &id, &config).await?;
        }

        let p = history_path(&config);
        let size = tokio::fs::metadata(&p).await?.len() as usize;
        assert!(size <= cap, "history size {} should be <= {}", size, cap);

        // Ensure file starts at a JSONL boundary (first byte of file is '{' when non-empty)
        if size > 0 {
            let data = tokio::fs::read(&p).await?;
            assert_eq!(data[0], b'{');
            assert!(data.ends_with(b"\n"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn trims_partial_prefix_and_preserves_lines() -> Result<()> {
        let tmp = TempDir::new().unwrap();
        let cap = 128usize;
        let config = build_config_with_home(tmp.path(), Some(cap));
        let p = history_path(&config);
        tokio::fs::create_dir_all(p.parent().unwrap()).await?;

        // Seed with a corrupted prefix that does not end on a newline plus some valid lines.
        let mut seed = b"{not-json".to_vec();
        for i in 0..50u32 {
            let entry = HistoryEntry {
                session_id: "s".into(),
                ts: 1,
                text: format!("seed-{i}"),
            };
            let mut line = serde_json::to_vec(&entry).unwrap();
            line.push(b'\n');
            seed.extend_from_slice(&line);
        }
        tokio::fs::write(&p, &seed).await?;

        // Append one more valid entry which will trigger trimming.
        let id = ConversationId::new();
        append_entry("final", &id, &config).await?;

        let data = tokio::fs::read(&p).await?;
        assert!(data.len() <= cap, "len {} > cap {}", data.len(), cap);
        if !data.is_empty() {
            assert_eq!(data[0], b'{', "file should start on JSONL boundary");
            assert!(data.ends_with(b"\n"));
        }
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn env_override_wins_over_config() -> Result<()> {
        // Set a very small cap via env and a large cap via config.
        unsafe { std::env::set_var("CODEX_HISTORY_MAX_BYTES", "64") };
        let tmp = TempDir::new().unwrap();
        let config = build_config_with_home(tmp.path(), Some(10_000));
        let id = ConversationId::new();

        for i in 0..200u32 {
            let msg = format!("event {i} - {}", "x".repeat(32));
            append_entry(&msg, &id, &config).await?;
        }

        let p = history_path(&config);
        let size = tokio::fs::metadata(&p).await?.len() as usize;
        assert!(size <= 64, "env override not enforced: size {}", size);

        unsafe { std::env::remove_var("CODEX_HISTORY_MAX_BYTES") };
        Ok(())
    }
}
