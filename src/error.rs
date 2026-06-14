use std::fmt::{Display, Formatter};

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, FastKError>;

/// Error type for FastK storage, chunk, and query operations.
#[derive(Debug)]
pub enum FastKError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    Serde(serde_json::Error),
    InvalidInput(String),
    InvalidData(String),
    NotFound(String),
}

impl Display for FastKError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Sqlite(err) => write!(f, "sqlite error: {err}"),
            Self::Serde(err) => write!(f, "serde error: {err}"),
            Self::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Self::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Self::NotFound(msg) => write!(f, "not found: {msg}"),
        }
    }
}

impl std::error::Error for FastKError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Sqlite(err) => Some(err),
            Self::Serde(err) => Some(err),
            Self::InvalidInput(_) | Self::InvalidData(_) | Self::NotFound(_) => None,
        }
    }
}

impl From<std::io::Error> for FastKError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<rusqlite::Error> for FastKError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

impl From<serde_json::Error> for FastKError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value)
    }
}
