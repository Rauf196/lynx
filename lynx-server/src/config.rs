//! Server configuration with layered loading.
//!
//! Configuration is loaded in order of precedence:
//! 1. CLI arguments (highest priority)
//! 2. Environment variables (`LYNX_*`)
//! 3. Config file (`config.toml`)
//! 4. Default values (lowest priority)

use config::{Config as ConfigBuilder, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

/// Server configuration.
///
/// All fields have sensible defaults. Override via environment variables
/// (prefixed with `LYNX_`) or a TOML config file.
///
/// # Environment Variables
///
/// | Variable | Default | Description |
/// |----------|---------|-------------|
/// | `LYNX_HOST` | `127.0.0.1` | Bind address |
/// | `LYNX_PORT` | `6006` | TCP port |
/// | `LYNX_MAXCONNECTIONS` | `1000` | Max concurrent clients |
/// | `LYNX_LOGLEVEL` | `info` | Log level (trace/debug/info/warn/error) |
/// | `LYNX_METRICSHOST` | `127.0.0.1` | Metrics server bind address |
/// | `LYNX_METRICSPORT` | `9090` | Metrics server port |
/// | `LYNX_SLOW_CLIENT_THRESHOLD` | `50` | Dropped messages before disconnect |
/// | `LYNX_RATE_LIMIT_PER_SECOND` | `10.0` | Messages per second limit |
/// | `LYNX_RATE_LIMIT_BURST` | `20` | Burst capacity for rate limiter |
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    /// TCP bind address (e.g., "0.0.0.0" for all interfaces).
    #[serde(default = "default_host")]
    pub host: String,

    /// TCP port for client connections.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Maximum concurrent client connections.
    #[serde(default = "default_maxconnections")]
    pub maxconnections: usize,

    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_loglevel")]
    pub loglevel: String,

    /// Metrics HTTP server bind address.
    #[serde(default = "default_metricshost")]
    pub metricshost: String,

    /// Metrics HTTP server port (serves /metrics, /health, /ready).
    #[serde(default = "default_metricsport")]
    pub metricsport: u16,

    /// Number of dropped messages before disconnecting a slow client.
    #[serde(default = "default_slow_client_threshold")]
    pub slow_client_threshold: usize,

    /// Token bucket refill rate (messages per second).
    #[serde(default = "default_rate_limit_per_second")]
    pub rate_limit_per_second: f64,

    /// Token bucket burst capacity (max messages in burst).
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: usize,
}

// default value functions for serde
fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    6006
}

fn default_maxconnections() -> usize {
    1000
}

fn default_loglevel() -> String {
    "info".to_string()
}

fn default_metricshost() -> String {
    "127.0.0.1".to_string()
}

fn default_metricsport() -> u16 {
    9090
}

fn default_slow_client_threshold() -> usize {
    50
}

fn default_rate_limit_per_second() -> f64 {
    10.0
}

fn default_rate_limit_burst() -> usize {
    20
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            maxconnections: default_maxconnections(),
            loglevel: default_loglevel(),
            metricshost: default_metricshost(),
            metricsport: default_metricsport(),
            slow_client_threshold: default_slow_client_threshold(),
            rate_limit_per_second: default_rate_limit_per_second(),
            rate_limit_burst: default_rate_limit_burst(),
        }
    }
}

impl Config {
    /// Loads configuration from multiple sources.
    ///
    /// Sources are applied in order (later sources override earlier):
    /// 1. Default values
    /// 2. Config file (explicit path or `config.toml` if exists)
    /// 3. Environment variables (`LYNX_*`)
    ///
    /// # Arguments
    ///
    /// * `config_path` - Optional path to TOML config file. If `None`,
    ///   uses `config.toml` in the current directory if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the config file exists but cannot be parsed.
    pub fn load(config_path: Option<&str>) -> Result<Self, ConfigError> {
        let mut builder = ConfigBuilder::builder();

        // set defaults
        builder = builder
            .set_default("host", default_host())?
            .set_default("port", default_port() as i64)?
            .set_default("maxconnections", default_maxconnections() as i64)?
            .set_default("loglevel", default_loglevel())?
            .set_default("metricshost", default_metricshost())?
            .set_default("metricsport", default_metricsport() as i64)?
            .set_default(
                "slow_client_threshold",
                default_slow_client_threshold() as i64,
            )?
            .set_default("rate_limit_per_second", default_rate_limit_per_second())?
            .set_default("rate_limit_burst", default_rate_limit_burst() as i64)?;

        // load from config file
        match config_path {
            Some(path) => {
                // explicit path provided - must exist
                builder = builder.add_source(File::with_name(path));
            }
            None => {
                // default: use config.toml if it exists
                if Path::new("config.toml").exists() {
                    builder = builder.add_source(File::with_name("config"));
                }
            }
        }

        // override with env vars (LYNX_HOST, LYNX_PORT, etc.)
        builder = builder.add_source(
            Environment::with_prefix("LYNX")
                .separator("_")
                .try_parsing(true),
        );

        builder.build()?.try_deserialize()
    }

    /// Returns the server socket address as `"host:port"`.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Returns the metrics server address as `"metricshost:metricsport"`.
    pub fn metrics_address(&self) -> String {
        format!("{}:{}", self.metricshost, self.metricsport)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 6006);
        assert_eq!(config.maxconnections, 1000);
        assert_eq!(config.loglevel, "info");
        assert_eq!(config.metricshost, "127.0.0.1");
        assert_eq!(config.metricsport, 9090);
        assert_eq!(config.slow_client_threshold, 50);
        assert_eq!(config.rate_limit_per_second, 10.0);
        assert_eq!(config.rate_limit_burst, 20);
    }

    #[test]
    fn test_address() {
        let config = Config::default();
        assert_eq!(config.address(), "127.0.0.1:6006");
    }

    #[test]
    fn test_metrics_address() {
        let config = Config::default();
        assert_eq!(config.metrics_address(), "127.0.0.1:9090");
    }
}
