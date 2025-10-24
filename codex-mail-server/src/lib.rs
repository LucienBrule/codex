pub mod config;
pub mod registry;
pub mod server;
pub mod telemetry;

use anyhow::Result;
use config::MailServerConfig;
use server::MailDispatcherServer;
use telemetry::TelemetryGuard;

pub async fn run(config: MailServerConfig) -> Result<()> {
    let _telemetry = TelemetryGuard::init(&config)?;
    let registry = registry::RegistryWatcher::new(&config).await?;
    let server = MailDispatcherServer::new(config, registry);
    server.run().await
}
