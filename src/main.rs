use axum::{routing::get, Router};
use tracing_subscriber;

mod config;
mod error;
mod state;
mod module_manager;
mod ipc;
mod api;

use config::Config;
use state::AppState;
use module_manager::ModuleManager;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
   // logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let config = Config::load()?;

    // 1. Ранний бинд сокета для IPC, чтобы избежать Race Condition
    let ipc_server = crate::ipc::IpcServer::bind(config.socket_path.clone())?;

    // 2. Инициализация менеджера модулей и запуск процессов
    let mut module_manager = ModuleManager::new(config.modules_dir.clone());
    module_manager.load_and_start_all().await?;

    // 3. Создание общего состояния приложения
    let app_state = AppState::new(module_manager);

    // 4. Запуск IPC сервера с передачей состояния
    ipc_server.start(app_state.clone()).await?;
    
    let app = Router::new()
        .route("/", get(|| async { "Alpee Core is running!" }))
        .route("/api/modules", get(api::list_modules))
        .route("/api/modules/:name", get(api::get_module))
        .route("/api/modules/:name/command", axum::routing::post(api::send_command))
        .route("/api/ws", get(api::ws_handler))
        .with_state(app_state);

    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("Alpee Core listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}