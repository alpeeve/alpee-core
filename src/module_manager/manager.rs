use std::path::Path;
use tokio::process::Command;
use crate::module_manager::module::{Module, ModuleStatus};
use crate::module_manager::registry::ModuleRegistry;
use crate::error::Result;
use tracing::{info, error};

#[derive(Clone)]
pub struct ModuleManager {
    registry: ModuleRegistry,
    modules_dir: std::path::PathBuf,
}

impl ModuleManager {
    pub fn new(modules_dir: std::path::PathBuf) -> Self {
        Self {
            registry: ModuleRegistry::new(),
            modules_dir,
        }
    }

    pub async fn load_and_start_all(&mut self) -> Result<()> {
        info!("Scanning modules in: {:?}", self.modules_dir);

        if !self.modules_dir.exists() {
            info!("Modules directory not found. Creating...");
            std::fs::create_dir_all(&self.modules_dir)?;
        }

        let entries = std::fs::read_dir(&self.modules_dir)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                let module_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                info!("Found module: {}", module_name);
                
                // Try to load module metadata from module.toml; fall back to basic constructor.
                let mut module = match Module::from_dir(&path) {
                    Ok(m) => m,
                    Err(err) => {
                        error!("Failed to read module.toml for {}: {}", module_name, err);
                        Module::new(module_name.clone(), "0.1.0".to_string())
                    }
                };

                if let Err(e) = self.start_module(&mut module, &path).await {
                    error!("Failed to start module {}: {}", module_name, e);
                    module.info.status = ModuleStatus::Failed;
                } else {
                    module.info.status = ModuleStatus::Running;
                }
                
                self.registry.register(module);
            }
        }

        Ok(())
    }

    async fn start_module(&self, module: &mut Module, module_path: &Path) -> Result<()> {
        let binary_path = module_path.join(format!("target/release/{}", module.info.binary));

        if !binary_path.exists() {
            info!("Binary not found for {}, skipping execution for now", module.info.name);
            return Ok(());
        }

        info!("Starting module: {} (binary={})", module.info.name, module.info.binary);

        let child = Command::new(binary_path)
            .env("ALPEE_SOCKET", "/tmp/alpee-core.sock")
            .env("ALPEE_MODULE_NAME", &module.info.name)
            .spawn()?;

        module.process = Some(child);
        Ok(())
    }

    pub fn list_modules(&self) -> Vec<&Module> {
        self.registry.list_all()
    }

    pub fn register_or_update(&mut self, req: &crate::ipc::RegisterRequest) {
        if let Some(module) = self.registry.get_mut_by_name(&req.name) {
            module.info.version = req.version.clone();
            module.info.capabilities = req.capabilities.clone();
            module.info.ui_components = req.ui_components.clone();
            module.info.status = ModuleStatus::Running;
            info!("Updated registered module: {}", req.name);
        } else {
            let mut module = Module::new(req.name.clone(), req.version.clone());
            module.info.capabilities = req.capabilities.clone();
            module.info.ui_components = req.ui_components.clone();
            module.info.status = ModuleStatus::Running;
            self.registry.register(module);
            info!("Registered new dynamic module: {}", req.name);
        }
    }

    pub fn set_status(&mut self, name: &str, status: ModuleStatus) {
        if let Some(module) = self.registry.get_mut_by_name(name) {
            module.info.status = status;
        }
    }
}