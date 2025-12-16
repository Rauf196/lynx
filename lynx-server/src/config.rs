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

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            maxconnections: default_maxconnections(),
            loglevel: default_loglevel(),
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
            .set_default("loglevel", default_loglevel())?;

        // load from config.toml if it exists
        if Path::new("config.toml").exists() {
            builder = builder.add_source(File::with_name("config"));
        }

        // override with env vars (LYNX_HOST, LYNX_PORT, etc.)
        builder = builder.add_source(
            Environment::with_prefix("LYNX")
                .separator("_")
                .try_parsing(true)
        );

        builder.build()?.try_deserialize()
    }

    // returns socket address as "host:port"
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
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
    }

    #[test]
    fn test_address() {
        let config = Config::default();
        assert_eq!(config.address(), "127.0.0.1:6006");
    }
}
