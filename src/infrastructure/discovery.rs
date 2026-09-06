use std::collections::HashSet;
use std::sync::Arc;

use arcrelay_network::NetworkRuntime;
use base64::Engine as _;

use crate::application::manager::TransferManager;
use crate::domain::NearbyPeer;

/// Projects unified discovery into the transfer view without owning a
/// browser, advertiser, endpoint, or identity.
pub(crate) async fn observe(manager: Arc<TransferManager>, network: Arc<NetworkRuntime>) {
    let mut receiver = network.discovery().subscribe();
    let local_id = network.device_id();
    let initial_devices = receiver.borrow_and_update().clone();
    project_snapshot(&manager, &local_id, initial_devices).await;
    manager.tasks.clone().spawn(async move {
        loop {
            tokio::select! {
                _ = manager.stopping.cancelled() => break,
                result = receiver.changed() => if result.is_err() { break; },
            }
            let devices = receiver.borrow_and_update().clone();
            project_snapshot(&manager, &local_id, devices).await;
        }
    });
}

async fn project_snapshot(
    manager: &Arc<TransferManager>,
    local_id: &arcrelay_peer::DeviceId,
    devices: Arc<Vec<arcrelay_network::PeerAdvertisement>>,
) {
    let visible_ids = devices
        .iter()
        .filter(|device| device.device_id != *local_id)
        .map(|device| device.device_id.to_string())
        .collect::<HashSet<_>>();
    for device in devices
        .iter()
        .filter(|device| device.device_id != *local_id)
    {
        let Some(address) = device.addresses.first() else {
            continue;
        };
        if let Err(error) = manager
            .upsert_peer(NearbyPeer {
                id: device.device_id.to_string(),
                name: device.metadata.name.clone(),
                platform: device.metadata.platform.clone(),
                model: device.metadata.model.clone(),
                address: address.to_string(),
                addresses: device.addresses.iter().map(ToString::to_string).collect(),
                port: device.port,
                public_key: base64::engine::general_purpose::STANDARD
                    .encode(device.public_key.as_bytes()),
                certificate_sha256: device
                    .certificate_sha256
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                paired: false,
                automatic_receive: false,
                last_seen_at_ms: device.last_seen_at_ms,
            })
            .await
        {
            tracing::warn!(%error, "failed to update transfer peer");
        }
    }
    let current = manager.snapshot().await.peers;
    for peer in current {
        if !visible_ids.contains(&peer.id) {
            manager.remove_peer(&peer.id).await;
        }
    }
}
