use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub modules_dir: PathBuf,
    pub socket_path: PathBuf,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            modules_dir: PathBuf::from("./modules"),
            socket_path: PathBuf::from("/tmp/alpee-core.sock"),
            log_level: "info".to_string(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        // Позже добавим загрузку из файла config.toml
        let config = Self::default();
        tracing::info!("Loaded config: {:?}", config);
        Ok(config)
    }
}