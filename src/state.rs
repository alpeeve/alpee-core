use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use crate::module_manager::ModuleManager;
use crate::ipc::{Message, Response, ModuleEvent};

#[derive(Clone)]
pub struct AppState {
    pub module_manager: Arc<RwLock<ModuleManager>>,
    pub ipc_connections: Arc<RwLock<HashMap<String, mpsc::Sender<Message>>>>,
    pub pending_requests: Arc<RwLock<HashMap<String, oneshot::Sender<Response>>>>,
    pub event_tx: broadcast::Sender<ModuleEvent>,
}

impl AppState {
    pub fn new(module_manager: ModuleManager) -> Self {
        let (event_tx, _) = broadcast::channel(100);
        Self {
            module_manager: Arc::new(RwLock::new(module_manager)),
            ipc_connections: Arc::new(RwLock::new(HashMap::new())),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
        }
    }
}