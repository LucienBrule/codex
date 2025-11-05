use anyhow::Result;
use tracing_subscriber::EnvFilter;

use crate::config::MailServerConfig;

static TRACING_INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();

pub struct TelemetryGuard;

impl TelemetryGuard {
    pub fn init(_config: &MailServerConfig) -> Result<Self> {
        if TRACING_INIT.get().is_none() {
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .compact()
                .try_init()
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            let _ = TRACING_INIT.set(());
        }
        Ok(Self)
    }
}
