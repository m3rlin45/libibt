use thiserror::Error;

/// Errors that can occur when reading IBT files.
#[derive(Error, Debug)]
pub enum IbtError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid header: {0}")]
    InvalidHeader(String),

    #[error("Out of bounds: {0}")]
    OutOfBounds(String),

    #[cfg(feature = "arrow")]
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("YAML error: {0}")]
    Yaml(String),
}

pub type Result<T> = std::result::Result<T, IbtError>;
