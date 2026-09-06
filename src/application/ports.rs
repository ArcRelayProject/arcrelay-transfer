use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::domain::{ReceivePolicy, TransferView};
use crate::Result;

#[async_trait]
pub trait TransferRepository: Send + Sync {
    async fn load(&self) -> Result<Vec<TransferView>>;
    async fn save(&self, transfers: &[TransferView]) -> Result<()>;
}

#[async_trait]
pub trait TransferSettingsRepository: Send + Sync {
    async fn load_receive_directory(&self) -> Result<Option<PathBuf>>;
    async fn save_receive_directory(&self, path: &Path) -> Result<()>;
}

#[async_trait]
pub trait ReceivePolicyRepository: Send + Sync {
    async fn policy(&self, peer_id: &str) -> Result<ReceivePolicy>;
    async fn set_policy(&self, peer_id: &str, policy: ReceivePolicy) -> Result<()>;
}

pub trait PreviewGenerator: Send + Sync {
    fn image_thumbnail(&self, path: &Path) -> Result<Option<(String, Vec<u8>)>>;
}
