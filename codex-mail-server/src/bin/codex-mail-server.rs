use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let config = codex_mail_server::config::load_config()?;
    codex_mail_server::run(config).await
}
