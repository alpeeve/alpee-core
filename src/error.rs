use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Module error: {0}")]
    Module(String),

    #[error("IPC error: {0}")]
    Ipc(String),

    #[error("Config error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, AppError>;