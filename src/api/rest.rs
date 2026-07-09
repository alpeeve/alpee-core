use axum::{
    extract::{State, Path},
    Json,
    http::StatusCode,
};
use serde_json::{json, Value};
use uuid::Uuid;
use tokio::time::{timeout, Duration};
use crate::state::AppState;
use crate::ipc::{Message, Command, Response};

pub async fn list_modules(
    State(state): State<AppState>,
) -> Json<Value> {
    let manager = state.module_manager.read().await;
    let modules = manager.list_modules();
    Json(json!({
        "modules": modules.iter().map(|m| &m.info).collect::<Vec<_>>()
    }))
}

pub async fn get_module(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let manager = state.module_manager.read().await;
    if let Some(module) = manager.list_modules().iter().find(|m| m.info.name == name) {
        Ok(Json(json!({ "module": module.info })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Module '{}' not found", name) })),
        ))
    }
}

#[derive(serde::Deserialize)]
pub struct CommandPayload {
    pub action: String,
    pub payload: Value,
}

pub async fn send_command(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<CommandPayload>,
) -> Result<Json<Response>, (StatusCode, Json<Value>)> {
    // 1. Проверяем, подключен ли модуль по IPC
    let tx = {
        let conns = state.ipc_connections.read().await;
        conns.get(&name).cloned()
    };

    let tx = match tx {
        Some(t) => t,
        None => {
            return Err((
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("Module '{}' is not connected or not running", name) })),
            ));
        }
    };

    // 2. Генерируем уникальный request ID
    let request_id = Uuid::new_v4().to_string();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Response>();

    // 3. Регистрируем oneshot канал ожидания
    state.pending_requests.write().await.insert(request_id.clone(), reply_tx);

    // 4. Отправляем команду в IPC канал модуля
    let cmd = Message::Command(Command {
        id: request_id.clone(),
        action: payload.action,
        payload: payload.payload,
    });

    if let Err(e) = tx.send(cmd).await {
        state.pending_requests.write().await.remove(&request_id);
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to send IPC message to module: {}", e) })),
        ));
    }

    // 5. Ждем ответа с таймаутом (5 секунд)
    match timeout(Duration::from_secs(5), reply_rx).await {
        Ok(Ok(resp)) => Ok(Json(resp)),
        Ok(Err(_)) => {
            Err((
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "Connection to module closed before response was received" })),
            ))
        }
        Err(_) => {
            state.pending_requests.write().await.remove(&request_id);
            Err((
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({ "error": "Module did not respond within the timeout period" })),
            ))
        }
    }
}
