use axum::{
    extract::{ws::{WebSocket, Message as WsMessage}, State, WebSocketUpgrade},
    response::IntoResponse,
};
use futures::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};
use crate::state::AppState;
use crate::ipc::{Message as IpcMessage, Command as IpcCommand, ModuleEvent, Response as IpcResponse};

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum ClientWsMessage {
    #[serde(rename = "subscribe")]
    Subscribe { topic: String },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { topic: String },
    #[serde(rename = "command")]
    Command {
        id: String,
        module: String,
        action: String,
        payload: Value,
    },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type")]
pub enum ServerWsMessage {
    #[serde(rename = "event")]
    Event {
        module: String,
        topic: String,
        data: Value,
    },
    #[serde(rename = "response")]
    Response {
        id: String,
        module: String,
        success: bool,
        message: String,
        data: Option<Value>,
    },
    #[serde(rename = "error")]
    Error {
        message: String,
    },
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws_socket(socket, state))
}

async fn handle_ws_socket(socket: WebSocket, state: AppState) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Канал подписок для текущего WebSocket-клиента
    let subscriptions = Arc::new(RwLock::new(HashSet::<String>::new()));

    // Буфер/канал для отправки сообщений клиенту
    let (send_tx, mut send_rx) = tokio::sync::mpsc::channel::<ServerWsMessage>(64);

    // Фоновый таск записи в WebSocket
    let write_task = tokio::spawn(async move {
        while let Some(msg) = send_rx.recv().await {
            if let Ok(text) = serde_json::to_string(&msg) {
                if let Err(_) = ws_tx.send(WsMessage::Text(text)).await {
                    break;
                }
            }
        }
    });

    // Фоновый таск трансляции событий из глобального broadcast канала
    let mut event_rx = state.event_tx.subscribe();
    let subs_clone = subscriptions.clone();
    let send_tx_clone = send_tx.clone();
    let event_task = tokio::spawn(async move {
        loop {
            match event_rx.recv().await {
                Ok(evt) => {
                    let subs = subs_clone.read().await;
                    let specific_topic = format!("{}/{}", evt.module, evt.topic);
                    let wildcard_topic = format!("{}/*", evt.module);

                    if subs.contains(&specific_topic) || subs.contains(&wildcard_topic) || subs.contains("*") {
                        let ws_msg = ServerWsMessage::Event {
                            module: evt.module,
                            topic: evt.topic,
                            data: evt.data,
                        };
                        if let Err(_) = send_tx_clone.send(ws_msg).await {
                            break;
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Основной цикл чтения сообщений из WebSocket
    while let Some(Ok(msg)) = ws_rx.next().await {
        if let WsMessage::Text(text) = msg {
            match serde_json::from_str::<ClientWsMessage>(&text) {
                Ok(ws_msg) => match ws_msg {
                    ClientWsMessage::Subscribe { topic } => {
                        info!("WS client subscribed to: {}", topic);
                        subscriptions.write().await.insert(topic);
                    }
                    ClientWsMessage::Unsubscribe { topic } => {
                        info!("WS client unsubscribed from: {}", topic);
                        subscriptions.write().await.remove(&topic);
                    }
                    ClientWsMessage::Command { id, module, action, payload } => {
                        let tx = {
                            let conns = state.ipc_connections.read().await;
                            conns.get(&module).cloned()
                        };

                        if let Some(ipc_tx) = tx {
                            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<IpcResponse>();
                            state.pending_requests.write().await.insert(id.clone(), reply_tx);

                            let cmd = IpcMessage::Command(IpcCommand {
                                id: id.clone(),
                                action,
                                payload,
                            });

                            if let Err(e) = ipc_tx.send(cmd).await {
                                state.pending_requests.write().await.remove(&id);
                                let _ = send_tx.send(ServerWsMessage::Error {
                                    message: format!("Failed to send IPC message to module: {}", e),
                                }).await;
                                continue;
                            }

                            let send_tx_clone = send_tx.clone();
                            let module_clone = module.clone();
                            let pending_requests_clone = state.pending_requests.clone();
                            let id_clone = id.clone();
                            
                            // Асинхронно ждем ответа, чтобы не блокировать чтение сокета
                            tokio::spawn(async move {
                                match tokio::time::timeout(tokio::time::Duration::from_secs(5), reply_rx).await {
                                    Ok(Ok(resp)) => {
                                        let _ = send_tx_clone.send(ServerWsMessage::Response {
                                            id: id_clone,
                                            module: module_clone,
                                            success: resp.success,
                                            message: resp.message,
                                            data: resp.data,
                                        }).await;
                                    }
                                    Ok(Err(_)) => {
                                        let _ = send_tx_clone.send(ServerWsMessage::Error {
                                            message: format!("Module '{}' disconnected during request processing", module_clone),
                                        }).await;
                                    }
                                    Err(_) => {
                                        pending_requests_clone.write().await.remove(&id_clone);
                                        let _ = send_tx_clone.send(ServerWsMessage::Error {
                                            message: format!("Module '{}' response timeout", module_clone),
                                        }).await;
                                    }
                                }
                            });
                        } else {
                            let _ = send_tx.send(ServerWsMessage::Error {
                                message: format!("Module '{}' is not connected", module),
                            }).await;
                        }
                    }
                },
                Err(e) => {
                    let _ = send_tx.send(ServerWsMessage::Error {
                        message: format!("Invalid message format: {}", e),
                    }).await;
                }
            }
        }
    }

    event_task.abort();
    write_task.abort();
}
