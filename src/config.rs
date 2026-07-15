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
            port: 8001,
            modules_dir: PathBuf::from("./modules"),
            socket_path: PathBuf::from("/tmp/alpee-core.sock"),
            log_level: "info".to_string(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        const CONFIG_PATH: &str = "alpee.toml";

        
        if !std::path::Path::new(CONFIG_PATH).exists() {
            tracing::warn!("alpee.toml not found, using default config");
            let config = Self::default();
            tracing::info!("Loaded default config: {:?}", config);
            return Ok(config);
        }

        
        let content = std::fs::read_to_string(CONFIG_PATH)
            .map_err(|e| format!("Failed to read {}: {}", CONFIG_PATH, e))?;

        
        let mut config: Config = toml::from_str(&content)
            .map_err(|e| format!("Failed to parse {}: {}", CONFIG_PATH, e))?;

        tracing::info!("Loaded config from {}: {:?}", CONFIG_PATH, config);
        Ok(config)
    }
}