use serde::{Deserialize, Serialize};
use tokio::process::Child;   // штука из за которой васё не работало
use uuid::Uuid;
use std::path::Path;
use std::fs;
use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiComponent {
    pub name: String,
    pub component_type: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleInfo {
    pub id: Uuid,
    pub name: String,
    pub version: String,
    pub binary: String,
    pub capabilities: Vec<String>,
    pub ui_components: Vec<UiComponent>,
    pub status: ModuleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ModuleStatus {
    Starting,
    Running,
    Failed,
    Stopped,
}

pub struct Module {
    pub info: ModuleInfo,
    pub process: Option<Child>,   // теперь tokio::process::Child
}

impl Module {
    pub fn new(name: String, version: String) -> Self {
        let binary = name.clone();

        Self {
            info: ModuleInfo {
                id: Uuid::new_v4(),
                name,
                version,
                binary,
                capabilities: vec![],
                ui_components: vec![],
                status: ModuleStatus::Starting,
            },
            process: None,
        }
    }
}

impl Clone for Module {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            process: None,
        }
    }
}

#[derive(Deserialize)]
struct ModuleSpec {
    name: String,
    version: String,
    binary: Option<String>,
    capabilities: Option<Vec<String>>,
    ui_components: Option<Vec<UiComponent>>,
}

impl Module {
    pub fn from_dir(dir: &Path) -> Result<Self> {
        let toml_path = dir.join("module.toml");
        if !toml_path.exists() {
            return Err(crate::error::AppError::Module("module.toml not found".into()));
        }

        let contents = fs::read_to_string(&toml_path)?;
        let spec: ModuleSpec = toml::from_str(&contents)
            .map_err(|e| crate::error::AppError::Module(format!("toml parse error: {}", e)))?;

        let binary = spec.binary.unwrap_or_else(|| spec.name.clone());

        Ok(Self {
            info: ModuleInfo {
                id: Uuid::new_v4(),
                name: spec.name,
                version: spec.version,
                binary,
                capabilities: spec.capabilities.unwrap_or_default(),
                ui_components: spec.ui_components.unwrap_or_default(),
                status: ModuleStatus::Starting,
            },
            process: None,
        })
    }
}