use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::application::ports::{
    ReceivePolicyRepository, TransferRepository, TransferSettingsRepository,
};
use crate::domain::{ReceivePolicy, TransferView};
use crate::Result;

const MAX_STORED_TRANSFERS: usize = 200;

#[derive(Clone)]
pub(crate) struct JsonTransferRepository {
    path: PathBuf,
    gate: Arc<Mutex<()>>,
}

impl JsonTransferRepository {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            gate: Arc::new(Mutex::new(())),
        }
    }
}

#[async_trait]
impl TransferRepository for JsonTransferRepository {
    async fn load(&self) -> Result<Vec<TransferView>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = tokio::fs::read(&self.path).await?;
        let mut transfers: Vec<TransferView> = serde_json::from_slice(&bytes)?;
        for transfer in &mut transfers {
            for file in &mut transfer.files {
                file.thumbnail_data_url = None;
            }
            if !transfer.status.terminal() {
                transfer.status = crate::domain::TransferStatus::Failed;
                transfer.error_message =
                    Some("transfer was still in progress when the application exited".into());
            }
        }
        transfers.sort_by_key(|transfer| std::cmp::Reverse(transfer.updated_at_ms));
        transfers.truncate(MAX_STORED_TRANSFERS);
        Ok(transfers)
    }

    async fn save(&self, transfers: &[TransferView]) -> Result<()> {
        let _guard = self.gate.lock().await;
        let compact: Vec<_> = transfers
            .iter()
            .take(MAX_STORED_TRANSFERS)
            .cloned()
            .map(|mut transfer| {
                transfer.speed_bytes_per_second = 0;
                transfer.remaining_seconds = None;
                for file in &mut transfer.files {
                    file.thumbnail_data_url = None;
                }
                transfer
            })
            .collect();
        atomic_write(&self.path, &serde_json::to_vec(&compact)?).await
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredTransferSettings {
    version: u32,
    receive_directory: PathBuf,
}

#[derive(Clone)]
pub(crate) struct JsonTransferSettingsRepository {
    path: PathBuf,
    gate: Arc<Mutex<()>>,
}

impl JsonTransferSettingsRepository {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            gate: Arc::new(Mutex::new(())),
        }
    }
}

#[async_trait]
impl TransferSettingsRepository for JsonTransferSettingsRepository {
    async fn load_receive_directory(&self) -> Result<Option<PathBuf>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = tokio::fs::read(&self.path).await?;
        let settings: StoredTransferSettings = serde_json::from_slice(&bytes)?;
        Ok(Some(settings.receive_directory))
    }

    async fn save_receive_directory(&self, path: &Path) -> Result<()> {
        let _guard = self.gate.lock().await;
        let settings = StoredTransferSettings {
            version: 1,
            receive_directory: path.to_path_buf(),
        };
        atomic_write(&self.path, &serde_json::to_vec_pretty(&settings)?).await
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredReceivePolicy {
    peer_id: String,
    receive_policy: ReceivePolicy,
}

#[derive(Clone)]
pub(crate) struct JsonReceivePolicyRepository {
    path: PathBuf,
    values: Arc<Mutex<HashMap<String, StoredReceivePolicy>>>,
}

impl JsonReceivePolicyRepository {
    pub fn load(path: PathBuf) -> Result<Self> {
        let values = if path.exists() {
            let items: Vec<StoredReceivePolicy> = serde_json::from_slice(&std::fs::read(&path)?)?;
            items
                .into_iter()
                .map(|item| (item.peer_id.clone(), item))
                .collect()
        } else {
            HashMap::new()
        };
        Ok(Self {
            path,
            values: Arc::new(Mutex::new(values)),
        })
    }

    async fn persist(&self) -> Result<()> {
        let values = self.values.lock().await;
        let mut items: Vec<_> = values.values().cloned().collect();
        items.sort_by(|left, right| left.peer_id.cmp(&right.peer_id));
        atomic_write(&self.path, &serde_json::to_vec_pretty(&items)?).await
    }
}

#[async_trait]
impl ReceivePolicyRepository for JsonReceivePolicyRepository {
    async fn policy(&self, peer_id: &str) -> Result<ReceivePolicy> {
        Ok(self
            .values
            .lock()
            .await
            .get(peer_id)
            .map(|item| item.receive_policy)
            .unwrap_or(ReceivePolicy::AskEveryTime))
    }

    async fn set_policy(&self, peer_id: &str, policy: ReceivePolicy) -> Result<()> {
        self.values.lock().await.insert(
            peer_id.to_owned(),
            StoredReceivePolicy {
                peer_id: peer_id.to_owned(),
                receive_policy: policy,
            },
        );
        self.persist().await
    }
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    tokio::fs::write(&temp, bytes).await?;
    #[cfg(target_os = "windows")]
    {
        let backup = path.with_extension(format!("bak-{}", uuid::Uuid::new_v4()));
        let had_previous = path.exists();
        if had_previous {
            tokio::fs::rename(path, &backup).await?;
        }
        if let Err(error) = tokio::fs::rename(&temp, path).await {
            if had_previous {
                let _ = tokio::fs::rename(&backup, path).await;
            }
            return Err(error.into());
        }
        if had_previous {
            let _ = tokio::fs::remove_file(backup).await;
        }
    }
    #[cfg(not(target_os = "windows"))]
    tokio::fs::rename(temp, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        FileKind, TransferDirection, TransferFileView, TransferStatus, TransferView,
    };

    fn transfer(index: usize) -> TransferView {
        TransferView {
            id: format!("transfer-{index}"),
            wire_id: index as u64 + 1,
            peer_id: "peer".into(),
            peer_name: "Peer".into(),
            direction: TransferDirection::Sending,
            status: TransferStatus::Completed,
            files: vec![TransferFileView {
                id: 1,
                name: "image.png".into(),
                relative_path: "image.png".into(),
                local_path: None,
                receive_directory: None,
                size: 1,
                media_type: "image/png".into(),
                kind: FileKind::Image,
                thumbnail_data_url: Some(format!("data:image/png;base64,{}", "A".repeat(64_000))),
                completed_bytes: 1,
            }],
            total_bytes: 1,
            completed_bytes: 1,
            speed_bytes_per_second: 10,
            remaining_seconds: Some(0),
            error_message: None,
            created_at_ms: index as i64,
            updated_at_ms: index as i64,
        }
    }

    #[tokio::test]
    async fn history_is_bounded_and_drops_preview_payloads() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("history.json");
        let repository = JsonTransferRepository::new(path.clone());
        let transfers: Vec<_> = (0..400).map(transfer).rev().collect();
        repository.save(&transfers).await.unwrap();

        assert!(tokio::fs::metadata(&path).await.unwrap().len() < 256_000);
        let loaded = repository.load().await.unwrap();
        assert_eq!(loaded.len(), MAX_STORED_TRANSFERS);
        assert!(loaded.iter().all(|transfer| transfer
            .files
            .iter()
            .all(|file| file.thumbnail_data_url.is_none())));
    }
}
