use thiserror::Error;

/// Domain-specific error types for polytrade.
/// All errors implement `std::error::Error` via thiserror.
/// Production code should use `anyhow::Result` (re-exported below) with `?`.
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum PolytradeError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Config error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Feed disconnected: {0}")]
    FeedDisconnected(String),
}

/// Convenience alias — all internal functions return anyhow::Result for ergonomic `?` chaining.
pub type Result<T> = anyhow::Result<T>;
