use dirs::home_dir;
use std::path::PathBuf;

/// This was copied from codex-core but codex-core depends on this crate.
/// TODO: move this to a shared crate lower in the dependency tree.
///
///
/// Returns the path to the Codex configuration directory, which can be
/// specified by the `CODEX_HOME` environment variable. If not set, defaults to
/// `~/.codex`.
///
/// The target directory is created if it does not already exist so that the
/// release binary can bootstrap itself without requiring manual setup.
pub(crate) fn find_codex_home() -> std::io::Result<PathBuf> {
    // Honor the `CODEX_HOME` environment variable when it is set to allow users
    // (and tests) to override the default location.
    if let Ok(val) = std::env::var("CODEX_HOME")
        && !val.is_empty()
    {
        let path = PathBuf::from(val);
        std::fs::create_dir_all(&path)?;
        return path.canonicalize().or_else(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Ok(path)
            } else {
                Err(err)
            }
        });
    }

    let mut p = home_dir().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not find home directory",
        )
    })?;
    p.push(".codex");
    std::fs::create_dir_all(&p)?;
    Ok(p)
}
