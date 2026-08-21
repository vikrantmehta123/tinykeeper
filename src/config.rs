use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_host")]
    pub listen_host: String,
    #[serde(default = "default_port")]
    pub tcp_port: u16,
    #[serde(default = "default_storage_path")]
    pub storage_path: PathBuf,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    2181
}
fn default_storage_path() -> PathBuf {
    PathBuf::from("./tinykeeper-data")
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen_host: default_host(),
            tcp_port: default_port(),
            storage_path: default_storage_path(),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => Config::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_falls_back_to_defaults_when_file_missing() {
        let config = Config::load("does-not-exist.toml");
        assert_eq!(config.listen_host, "127.0.0.1");
        assert_eq!(config.tcp_port, 2181);
        assert_eq!(config.storage_path, PathBuf::from("./tinykeeper-data"));
    }
}
