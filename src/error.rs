use prost::{DecodeError, EncodeError};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid varint: {0}")]
    DecodeError(#[from] DecodeError),

    #[error("invalid varint: {0}")]
    EncodeError(#[from] EncodeError),

    #[error("invalid key")]
    InvalidKey,

    #[error("invalid command type")]
    InvalidCommandType,

    #[error("invalid file path")]
    InvalidPath,

    #[error("read EOF")]
    ReadEOF,

    #[error("invalid crc")]
    InvalidCrc,
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvError>;
