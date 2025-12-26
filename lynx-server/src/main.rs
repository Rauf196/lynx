use anyhow::Result;
use lynx_server::{Config, Server};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let config = Config::load()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.loglevel)),
        )
        .with_target(true)
        .init();

    info!(
        host = %config.host,
        port = %config.port,
        maxconnections = %config.maxconnections,
        loglevel = %config.loglevel,
        "configuration loaded"
    );

    lynx_server::metrics::init(&config.metrics_address())
        .map_err(|e| anyhow::anyhow!("metrics init failed: {}", e))?;
    info!(metrics_address = %config.metrics_address(), "metrics server started");

    let (server, handle) = Server::bind(&config.address()).await?;
    info!(address = %handle.local_addr, "server bound");

    // spawn signal handler to trigger shutdown
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("ctrl+c received, initiating shutdown");
            handle.shutdown();
        }
    });

    // run server until shutdown completes
    server.run().await?;

    Ok(())
}
