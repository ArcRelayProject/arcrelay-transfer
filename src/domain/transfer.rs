use serde::{Deserialize, Serialize};

use crate::{Result, TransferError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum TransferDirection {
    Sending,
    Receiving,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum TransferStatus {
    Preparing,
    AwaitingApproval,
    Connecting,
    Transferring,
    Paused,
    Completed,
    Rejected,
    Cancelled,
    Failed,
}

impl TransferStatus {
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Rejected | Self::Cancelled | Self::Failed
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum FileKind {
    Image,
    Pdf,
    Archive,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct TransferFileView {
    pub id: u64,
    pub name: String,
    pub relative_path: String,
    /// Actual finalized local path, including collision renames. Never sent in a wire manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub local_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub receive_directory: Option<String>,
    pub size: u64,
    pub media_type: String,
    pub kind: FileKind,
    pub thumbnail_data_url: Option<String>,
    pub completed_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct TransferView {
    pub id: String,
    pub wire_id: u64,
    pub peer_id: String,
    pub peer_name: String,
    pub direction: TransferDirection,
    pub status: TransferStatus,
    pub files: Vec<TransferFileView>,
    pub total_bytes: u64,
    pub completed_bytes: u64,
    pub speed_bytes_per_second: u64,
    pub remaining_seconds: Option<u64>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl TransferView {
    pub fn transition(&mut self, next: TransferStatus, now_ms: i64) -> Result<()> {
        use TransferStatus::*;
        let allowed = matches!(
            (self.status, next),
            (Preparing, Connecting)
                | (Preparing, Cancelled)
                | (Preparing, Failed)
                | (AwaitingApproval, Connecting)
                | (AwaitingApproval, Rejected)
                | (AwaitingApproval, Cancelled)
                | (AwaitingApproval, Failed)
                | (Connecting, Transferring)
                | (Connecting, AwaitingApproval)
                | (Connecting, Cancelled)
                | (Connecting, Failed)
                | (Transferring, Paused)
                | (Transferring, Completed)
                | (Transferring, Cancelled)
                | (Transferring, Failed)
                | (Paused, Transferring)
                | (Paused, Cancelled)
                | (Paused, Failed)
        );
        if !allowed && self.status != next {
            return Err(TransferError::InvalidState(format!(
                "cannot transition {:?} to {:?}",
                self.status, next
            )));
        }
        self.status = next;
        self.updated_at_ms = now_ms;
        Ok(())
    }

    pub fn record_progress(&mut self, file_id: u64, completed: u64, now_ms: i64) -> Result<bool> {
        if !matches!(
            self.status,
            TransferStatus::Preparing | TransferStatus::Transferring | TransferStatus::Paused
        ) {
            return Err(TransferError::InvalidState("transfer is not active".into()));
        }
        let index = usize::try_from(file_id.saturating_sub(1))
            .ok()
            .filter(|&index| self.files.get(index).is_some_and(|file| file.id == file_id))
            .or_else(|| self.files.iter().position(|file| file.id == file_id))
            .ok_or_else(|| TransferError::Invalid("unknown file id".into()))?;
        let file = &mut self.files[index];
        if completed < file.completed_bytes || completed > file.size {
            return Err(TransferError::Invalid("invalid file progress".into()));
        }
        self.completed_bytes += completed - file.completed_bytes;
        file.completed_bytes = completed;
        self.updated_at_ms = now_ms;
        Ok(completed == file.size)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct TransferSnapshot {
    #[serde(default)]
    pub revision: u64,
    pub device_id: String,
    pub device_name: String,
    pub receive_directory: String,
    pub discoverable: bool,
    pub peers: Vec<super::NearbyPeer>,
    pub transfers: Vec<TransferView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "payload")]
pub enum TransferEvent {
    Snapshot {
        snapshot: TransferSnapshot,
        base_revision: u64,
    },
    Progress(TransferProgress),
    Error {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer() -> TransferView {
        TransferView {
            id: "t".into(),
            wire_id: 1,
            peer_id: "p".into(),
            peer_name: "peer".into(),
            direction: TransferDirection::Sending,
            status: TransferStatus::Preparing,
            files: vec![TransferFileView {
                id: 1,
                name: "a".into(),
                relative_path: "a".into(),
                local_path: None,
                receive_directory: None,
                size: 10,
                media_type: "application/octet-stream".into(),
                kind: FileKind::File,
                thumbnail_data_url: None,
                completed_bytes: 0,
            }],
            total_bytes: 10,
            completed_bytes: 0,
            speed_bytes_per_second: 0,
            remaining_seconds: None,
            error_message: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn state_machine_rejects_skipping_approval() {
        let mut value = transfer();
        assert!(value.transition(TransferStatus::Completed, 2).is_err());
        value.transition(TransferStatus::Connecting, 2).unwrap();
        value.transition(TransferStatus::Transferring, 3).unwrap();
        value.record_progress(1, 10, 4).unwrap();
        value.transition(TransferStatus::Completed, 5).unwrap();
    }

    #[test]
    fn old_history_with_security_code_remains_readable() {
        let mut value = serde_json::to_value(transfer()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("securityCode".into(), serde_json::json!("123 456"));

        assert!(serde_json::from_value::<TransferView>(value).is_ok());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct TransferProgress {
    pub revision: u64,
    pub base_revision: u64,
    pub transfer_id: String,
    pub completed_bytes: u64,
    pub speed_bytes_per_second: u64,
    pub remaining_seconds: Option<u64>,
    pub updated_at_ms: i64,
    pub files: Vec<TransferFileProgress>,
}
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct TransferFileProgress {
    pub id: u64,
    pub completed_bytes: u64,
}
