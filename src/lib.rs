//! ArcRelay LAN file transfer bounded context.
//!
//! The crate keeps transfer rules in the domain layer, use-case orchestration
//! in the application layer, and discovery/QUIC/filesystem concerns in
//! infrastructure. Remote-control pairing is intentionally not required.

pub mod application;
pub mod domain;
pub mod infrastructure;
mod subscription;
pub use subscription::TransferSubscription;

pub use application::{TransferConfig, TransferManager};
pub use domain::{
    FileKind, NearbyPeer, ReceivePolicy, TransferDirection, TransferEvent, TransferFileProgress,
    TransferFileView, TransferProgress, TransferSnapshot, TransferStatus, TransferView,
};

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("transfer cancelled")]
    Cancelled,
    #[error("invalid transfer: {0}")]
    Invalid(String),
    #[error("peer not found: {0}")]
    PeerNotFound(String),
    #[error("transfer not found: {0}")]
    TransferNotFound(String),
    #[error("transfer state does not allow this operation: {0}")]
    InvalidState(String),
    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("network failed: {0}")]
    Network(String),
    #[error("integrity validation failed: {0}")]
    Integrity(String),
}

impl TransferError {
    /// Stable machine-readable category for transport and UI adapters.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Cancelled => "transfer.cancelled",
            Self::Invalid(_) => "transfer.invalid_argument",
            Self::PeerNotFound(_) => "transfer.peer_not_found",
            Self::TransferNotFound(_) => "transfer.not_found",
            Self::InvalidState(_) => "transfer.failed_precondition",
            Self::Io(_) | Self::Network(_) => "transfer.unavailable",
            Self::Serialization(_) => "transfer.internal",
            Self::Integrity(_) => "transfer.integrity",
        }
    }
}

pub type Result<T> = std::result::Result<T, TransferError>;
