use codex_core::config::load_global_mcp_servers;
use codex_core::config::set_project_trusted;
use codex_core::config::set_windows_wsl_setup_acknowledged;
use codex_core::config::write_global_mcp_servers;
use codex_core::config_types::McpServerConfig;
use codex_core::config_types::McpServerTransportConfig;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::tempdir;

#[tokio::test]
async fn creates_home_and_writes_wsl_ack() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let codex_home = tmp.path().join("nested/dir/.codex");

    // Directory does not exist yet; function should create it and persist atomically.
    set_windows_wsl_setup_acknowledged(&codex_home, true)?;

    let contents = tokio::fs::read_to_string(codex_home.join("config.toml")).await?;
    assert!(contents.contains("windows_wsl_setup_acknowledged = true"));
    Ok(())
}

#[tokio::test]
async fn write_and_load_mcp_servers_roundtrip() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let codex_home = tmp.path();

    let servers = BTreeMap::from([(
        "docs".to_string(),
        McpServerConfig {
            transport: McpServerTransportConfig::Stdio {
                command: "docs-server".to_string(),
                args: vec!["--flag".into()],
                env: None,
            },
            enabled: true,
            startup_timeout_sec: None,
            tool_timeout_sec: None,
            keepalive_interval_sec: None,
        },
    )]);

    write_global_mcp_servers(codex_home, &servers)?;

    let loaded = load_global_mcp_servers(codex_home).await?;
    assert_eq!(loaded.len(), 1);
    let cfg = loaded.get("docs").expect("docs entry");
    match &cfg.transport {
        McpServerTransportConfig::Stdio { command, args, .. } => {
            assert_eq!(command, "docs-server");
            assert_eq!(args, &vec!["--flag".to_string()]);
        }
        _ => panic!("unexpected transport"),
    }
    Ok(())
}

#[test]
fn set_project_trusted_creates_tables() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let codex_home = tmp.path();
    let project = PathBuf::from("/tmp/project-x");

    set_project_trusted(codex_home, &project)?;

    let serialized = std::fs::read_to_string(codex_home.join("config.toml"))?;
    assert!(serialized.contains("trust_level = \"trusted\""));
    assert!(serialized.contains("projects."));
    Ok(())
}
