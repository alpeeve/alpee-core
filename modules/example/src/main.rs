use std::env;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use futures::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Register(RegisterRequest),
    Heartbeat(Heartbeat),
    Event(Event),
    Command(Command),
    Response(Response),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub ui_components: Vec<UiComponent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiComponent {
    pub name: String,
    pub component_type: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub module_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub topic: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub id: String,
    pub action: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = env::var("ALPEE_SOCKET").unwrap_or_else(|_| "/tmp/alpee-core.sock".to_string());
    let module_name = env::var("ALPEE_MODULE_NAME").unwrap_or_else(|_| "example".to_string());
    let module_id = Uuid::new_v4();

    println!("Starting module '{}' (ID: {})...", module_name, module_id);
    println!("Connecting to core socket at: {}...", socket_path);

    // Подключение с ретраями
    let stream = loop {
        match UnixStream::connect(&socket_path).await {
            Ok(s) => break s,
            Err(e) => {
                eprintln!("Failed to connect to core: {}. Retrying in 1s...", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    println!("Connected successfully to core!");

    let (rx, tx) = stream.into_split();
    let mut reader = FramedRead::new(rx, LengthDelimitedCodec::new());
    let mut writer = FramedWrite::new(tx, LengthDelimitedCodec::new());

    // Создаем MPSC канал для отправки сообщений в сокет
    let (send_tx, mut send_rx) = tokio::sync::mpsc::channel::<Message>(32);

    // Фоновая задача записи в сокет
    let write_task = tokio::spawn(async move {
        while let Some(msg) = send_rx.recv().await {
            if let Ok(bytes) = serde_json::to_vec(&msg) {
                if let Err(e) = writer.send(bytes.into()).await {
                    eprintln!("Write error: {}", e);
                    break;
                }
            }
        }
    });

    // 1. Отправляем сообщение о регистрации
    let reg_msg = Message::Register(RegisterRequest {
        name: module_name.clone(),
        version: "0.1.0".to_string(),
        capabilities: vec!["ping".to_string(), "metrics".to_string()],
        ui_components: vec![
            UiComponent {
                name: "status_panel".to_string(),
                component_type: "panel".to_string(),
                endpoint: "/ui/status".to_string(),
            },
            UiComponent {
                name: "metric_chart".to_string(),
                component_type: "chart".to_string(),
                endpoint: "/ui/chart".to_string(),
            },
        ],
    });
    send_tx.send(reg_msg).await?;

    // 2. Фоновый таск Heartbeat
    let send_tx_hb = send_tx.clone();
    let hb_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let hb = Message::Heartbeat(Heartbeat { module_id });
            if let Err(_) = send_tx_hb.send(hb).await {
                break;
            }
        }
    });

    // 3. Фоновый таск отправки метрик (Event) каждые 2 секунды
    let send_tx_metrics = send_tx.clone();
    let metrics_task = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            count += 1;
            let val = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() % 100) as f64;
            let event = Message::Event(Event {
                topic: "random_metric".to_string(),
                data: json!({
                    "seq": count,
                    "value": val,
                }),
            });
            if let Err(_) = send_tx_metrics.send(event).await {
                break;
            }
        }
    });

    // Основной цикл чтения команд от ядра
    while let Some(Ok(frame)) = reader.next().await {
        if let Ok(msg) = serde_json::from_slice::<Message>(&frame) {
            if let Message::Command(cmd) = msg {
                println!("Received command: {:?}", cmd);
                
                let response = match cmd.action.as_str() {
                    "ping" => Response {
                        id: cmd.id,
                        success: true,
                        message: "pong".to_string(),
                        data: Some(json!({ "reply": "pong_data" })),
                    },
                    other => Response {
                        id: cmd.id,
                        success: false,
                        message: format!("Unknown action: {}", other),
                        data: None,
                    },
                };

                let _ = send_tx.send(Message::Response(response)).await;
            }
        }
    }

    // Завершаем фоновые задачи
    hb_task.abort();
    metrics_task.abort();
    write_task.abort();
    
    println!("Module shutdown.");
    Ok(())
}
