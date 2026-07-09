use std::collections::HashMap;
use uuid::Uuid;
use crate::module_manager::module::Module;

#[derive(Clone)]
pub struct ModuleRegistry {
    modules: HashMap<Uuid, Module>,
    name_index: HashMap<String, Uuid>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
            name_index: HashMap::new(),
        }
    }

    pub fn register(&mut self, module: Module) {
        let id = module.info.id;
        self.name_index.insert(module.info.name.clone(), id);
        self.modules.insert(id, module);
    }

    pub fn get_by_name(&self, name: &str) -> Option<&Module> {
        self.name_index.get(name).and_then(|id| self.modules.get(id))
    }

    pub fn get_mut_by_name(&mut self, name: &str) -> Option<&mut Module> {
        let id = self.name_index.get(name)?;
        self.modules.get_mut(id)
    }

    pub fn list_all(&self) -> Vec<&Module> {
        self.modules.values().collect()
    }
}