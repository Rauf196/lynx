use config::{Config as ConfigBuilder, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_maxconnections")]
    pub maxconnections: usize,

    #[serde(default = "default_loglevel")]
    pub loglevel: String,

    #[serde(default = "default_metricshost")]
    pub metricshost: String,

    #[serde(default = "default_metricsport")]
    pub metricsport: u16,

    #[serde(default = "default_slow_client_threshold")]
    pub slow_client_threshold: usize,

    #[serde(default = "default_rate_limit_per_second")]
    pub rate_limit_per_second: f64,

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
    // load config from: defaults -> config.toml -> env vars (LYNX_*)
    pub fn load() -> Result<Self, ConfigError> {
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

        // load from config.toml if it exists
        if Path::new("config.toml").exists() {
            builder = builder.add_source(File::with_name("config"));
        }

        // override with env vars (LYNX_HOST, LYNX_PORT, etc.)
        builder = builder.add_source(
            Environment::with_prefix("LYNX")
                .separator("_")
                .try_parsing(true),
        );

        builder.build()?.try_deserialize()
    }

    // returns socket address as "host:port"
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    // returns metrics server address as "metricshost:metricsport"
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
