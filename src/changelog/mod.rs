mod record;
mod segment_writer;
mod wal_store;

pub use wal_store::WalStore;

/// The errors describe what can go wrong inside the WAL.
/// ChecksumMismatch is discovered by record.rs. These are WAL knowledge.
/// So WalError lives at the changelog module. The caller (like keeper_server.rs)
/// receives it, but doesn't define it. Hence, those errors are defined here.
#[derive(Debug)]
pub enum WalError {
    ChecksumMismatch,
    Io(std::io::Error),
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::ChecksumMismatch => write!(f, "WAL checksum mismatch"),
            WalError::Io(e) => write!(f, "WAL IO error: {}", e),
        }
    }
}

impl std::error::Error for WalError {}
