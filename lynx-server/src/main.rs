use anyhow::Result;
use clap::Parser;
use lynx_server::{Config, HealthState, Server};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "lynx-server")]
#[command(about = "High-performance TCP chat server")]
struct Args {
    /// path to config file (default: config.toml if exists)
    #[arg(short, long)]
    config: Option<String>,

    /// server port (overrides config file and env var)
    #[arg(short, long)]
    port: Option<u16>,

    /// log level: trace, debug, info, warn, error (overrides config file and env var)
    #[arg(short, long)]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let _ = dotenvy::dotenv();

    let mut config = Config::load(args.config.as_deref())?;

    // CLI args override config
    if let Some(port) = args.port {
        config.port = port;
    }
    if let Some(ref level) = args.log_level {
        config.loglevel = level.clone();
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.loglevel)),
        )
        .with_target(true)
        .init();

    info!(
        host = %config.host,
        port = %config.port,
        maxconnections = %config.maxconnections,
        loglevel = %config.loglevel,
        slow_client_threshold = %config.slow_client_threshold,
        rate_limit_per_second = %config.rate_limit_per_second,
        rate_limit_burst = %config.rate_limit_burst,
        "configuration loaded"
    );

    let (server, handle) = Server::bind(&config.address(), config.clone()).await?;
    info!(address = %handle.local_addr, "server bound");

    // create health state for health endpoints
    let accepting = Arc::new(AtomicBool::new(true));
    let health_state = Arc::new(HealthState {
        active_connections: server.active_connections(),
        max_connections: server.max_connections(),
        accepting: accepting.clone(),
    });

    lynx_server::metrics::init_with_health(&config.metrics_address(), health_state)
        .await
        .map_err(|e| anyhow::anyhow!("metrics init failed: {}", e))?;
    info!(metrics_address = %config.metrics_address(), "metrics and health server started");

    // spawn signal handler to trigger shutdown
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("ctrl+c received, initiating shutdown");
            accepting.store(false, std::sync::atomic::Ordering::Relaxed);
            handle.shutdown();
        }
    });

    // run server until shutdown completes
    server.run().await?;

    Ok(())
}
