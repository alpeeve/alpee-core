use tokio::net::UnixListener;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use futures::{StreamExt, SinkExt};
use tracing::{info, error};
use crate::error::Result;
use crate::ipc::protocol::{Message, Response, ModuleEvent};
use crate::state::AppState;
use crate::module_manager::module::ModuleStatus;

pub struct IpcServer {
    socket_path: std::path::PathBuf,
    listener: UnixListener,
}

impl IpcServer {
    pub fn bind(socket_path: std::path::PathBuf) -> Result<Self> {
        // Удаляем старый сокет если есть
        if socket_path.exists() {
            std::fs::remove_file(&socket_path).ok();
        }

        // Гарантируем наличие родительской папки для сокета
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let listener = UnixListener::bind(&socket_path)?;
        info!("IPC Unix Socket bound and listening on {:?}", socket_path);

        Ok(Self { socket_path, listener })
    }

    pub async fn start(self, app_state: AppState) -> Result<()> {
        let listener = self.listener;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        info!("New module connected via IPC");
                        tokio::spawn(handle_connection(stream, app_state.clone()));
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
        });

        Ok(())
    }
}

async fn handle_connection(stream: tokio::net::UnixStream, app_state: AppState) {
    let (rx, tx) = stream.into_split();
    let mut reader = FramedRead::new(rx, LengthDelimitedCodec::new());
    let mut writer = FramedWrite::new(tx, LengthDelimitedCodec::new());

    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<Message>(32);

    // Фоновая задача отправки сообщений в сокет
    let write_task = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match serde_json::to_vec(&msg) {
                Ok(bytes) => {
                    if let Err(e) = writer.send(bytes.into()).await {
                        error!("Failed to send message to module socket: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to serialize IPC message: {}", e);
                }
            }
        }
    });

    let mut registered_module_name: Option<String> = None;

    while let Some(Ok(frame)) = reader.next().await {
        match serde_json::from_slice::<Message>(&frame) {
            Ok(msg) => {
                match msg {
                    Message::Register(req) => {
                        info!("Received registration request from module: {} (version: {})", req.name, req.version);
                        
                        // 1. Обновляем статус и информацию в менеджере
                        {
                            let mut manager = app_state.module_manager.write().await;
                            manager.register_or_update(&req);
                        }

                        // 2. Сохраняем канал отправки
                        registered_module_name = Some(req.name.clone());
                        app_state.ipc_connections.write().await.insert(req.name.clone(), msg_tx.clone());

                        // 3. Отправляем ответ
                        let resp = Message::Response(Response {
                            id: "register".to_string(),
                            success: true,
                            message: "Registered successfully".to_string(),
                            data: None,
                        });
                        let _ = msg_tx.send(resp).await;
                    }
                    Message::Heartbeat(hb) => {
                        // Логируем или обновляем метку времени активности, если понадобится в будущем
                        tracing::debug!("Heartbeat from module: {}", hb.module_id);
                    }
                    Message::Event(evt) => {
                        if let Some(ref name) = registered_module_name {
                            // Оборачиваем во внешнее событие и рассылаем подписчикам
                            let mod_evt = ModuleEvent {
                                module: name.clone(),
                                topic: evt.topic,
                                data: evt.data,
                            };
                            let _ = app_state.event_tx.send(mod_evt);
                        }
                    }
                    Message::Response(resp) => {
                        // Ищем oneshot канал ожидания ответа
                        let mut pending = app_state.pending_requests.write().await;
                        if let Some(tx) = pending.remove(&resp.id) {
                            let _ = tx.send(resp);
                        }
                    }
                    Message::Command(cmd) => {
                        error!("Received command from module {:?}, core commands not supported yet", cmd);
                    }
                }
            }
            Err(e) => error!("Failed to parse message: {}", e),
        }
    }

    // Соединение закрыто — очищаем ресурсы
    if let Some(name) = registered_module_name {
        info!("Module connection closed: {}", name);
        app_state.ipc_connections.write().await.remove(&name);
        
        let mut manager = app_state.module_manager.write().await;
        manager.set_status(&name, ModuleStatus::Stopped);
    }

    write_task.abort();
}