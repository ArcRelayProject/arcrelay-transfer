use super::*;
use crate::application::TransferConfig;
use arcrelay_network::{NetworkRuntimeConfig, SqlitePeerRepository};
use arcrelay_peer::{Grant, GrantConstraints, GrantDirection, PeerRepository};

#[tokio::test]
async fn file_grant_expires_and_is_single_use() {
    let grants = tokio::sync::Mutex::new(HashMap::from([(
        1,
        FileGrant {
            ticket: vec![9; 32],
            expires_at_ms: 100,
            used: false,
        },
    )]));
    let open = proto::FileStreamOpen {
        transfer_id: 7,
        file_id: 1,
        ticket: vec![9; 32],
    };

    consume_file_grant(&grants, &open, 7, 100).await.unwrap();
    assert!(consume_file_grant(&grants, &open, 7, 100).await.is_err());

    let expired = tokio::sync::Mutex::new(HashMap::from([(
        1,
        FileGrant {
            ticket: vec![9; 32],
            expires_at_ms: 99,
            used: false,
        },
    )]));
    assert!(consume_file_grant(&expired, &open, 7, 100).await.is_err());
}

#[tokio::test]
async fn offer_preview_budget_never_exceeds_control_frame() {
    let prepared: Vec<_> = (0..MAX_FILE_COUNT)
        .map(|index| PreparedFile {
            source: None,
            source_modified: None,
            manifest: proto::FileManifestEntry {
                file_id: index as u64 + 1,
                relative_path: format!("image-{index}.png"),
                size: 1,
                media_type: "image/png".into(),

                modified_at_ms: 1,
            },
            preview: Some(proto::TransferPreview {
                file_id: index as u64 + 1,
                media_type: "image/jpeg".into(),
                data: vec![index as u8; MAX_PREVIEW_BYTES],
            }),
        })
        .collect();

    let offer = build_offer(7, &prepared).unwrap();
    assert!(encoded_offer_frame_len(&offer) <= MAX_CONTROL_FRAME_SIZE);
    assert!(
        offer
            .previews
            .iter()
            .map(|preview| preview.data.len())
            .sum::<usize>()
            <= MAX_TOTAL_PREVIEW_BYTES
    );
    assert!(offer.previews.len() < prepared.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_peers_transfer_a_file_over_real_quic() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Left".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Right".into(),
    ))
    .await
    .unwrap();
    let left_network = test_network(left_dir.path(), "Left").await;
    let right_network = test_network(right_dir.path(), "Right").await;
    pair_for_transfer(&left_network, &right_network).await;
    left.start_without_discovery(left_network.clone())
        .await
        .unwrap();
    right
        .start_without_discovery(right_network.clone())
        .await
        .unwrap();

    let left_id = left_network.device_id().to_string();
    let right_peer = network_peer(&right_network, "Right");
    right.set_receive_policy(&left_id, true).await.unwrap();
    left.upsert_peer(right_peer).await.unwrap();

    let source = left_dir.path().join("hello.txt");
    std::fs::create_dir_all(right_dir.path().join("inbox")).unwrap();
    std::fs::write(right_dir.path().join("inbox/hello.txt"), b"existing file").unwrap();
    std::fs::write(&source, b"hello from ArcRelay").unwrap();
    let transfer_id = left
        .send_files(right_network.device_id().to_string(), vec![source])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = left.snapshot().await;
            let status = snapshot
                .transfers
                .iter()
                .find(|value| value.id == transfer_id)
                .unwrap()
                .status;
            if status == TransferStatus::Completed {
                break;
            }
            if matches!(
                status,
                TransferStatus::Failed | TransferStatus::Rejected | TransferStatus::Cancelled
            ) {
                panic!(
                    "transfer ended in {status:?}: {:?}",
                    snapshot.transfers[0].error_message
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(right_dir.path().join("inbox/hello (1).txt")).unwrap(),
        b"hello from ArcRelay"
    );
    let received = right.snapshot().await;
    let file = &received.transfers[0].files[0];
    assert_eq!(
        file.local_path.as_deref(),
        right_dir.path().join("inbox/hello (1).txt").to_str()
    );
    assert_eq!(
        file.receive_directory.as_deref(),
        right_dir.path().join("inbox").to_str()
    );
    assert_eq!(
        std::fs::read(right_dir.path().join("inbox/hello.txt")).unwrap(),
        b"existing file"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpaired_nearby_peer_can_send_after_explicit_approval() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Unpaired Sender".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Unpaired Receiver".into(),
    ))
    .await
    .unwrap();
    let left_network = test_network(left_dir.path(), "Unpaired Sender").await;
    let right_network = test_network(right_dir.path(), "Unpaired Receiver").await;
    left.start_without_discovery(left_network.clone())
        .await
        .unwrap();
    right
        .start_without_discovery(right_network.clone())
        .await
        .unwrap();
    left.upsert_peer(network_peer(&right_network, "Unpaired Receiver"))
        .await
        .unwrap();
    assert!(!left.snapshot().await.peers[0].paired);

    let source = left_dir.path().join("one-off.txt");
    std::fs::write(&source, b"approved one-off transfer").unwrap();
    let outgoing_id = left
        .send_files(right_network.device_id().to_string(), vec![source])
        .await
        .unwrap();
    let incoming_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(transfer) = right
                .snapshot()
                .await
                .transfers
                .iter()
                .find(|transfer| transfer.status == TransferStatus::AwaitingApproval)
            {
                break transfer.id.clone();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(!right.snapshot().await.peers[0].paired);
    right
        .respond_incoming(&incoming_id, true, false)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = left.snapshot().await;
            let transfer = snapshot
                .transfers
                .iter()
                .find(|transfer| transfer.id == outgoing_id)
                .unwrap();
            if transfer.status == TransferStatus::Completed {
                break;
            }
            if transfer.status.terminal() {
                panic!("unpaired transfer failed: {:?}", transfer.error_message);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(right_dir.path().join("inbox/one-off.txt")).unwrap(),
        b"approved one-off transfer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sender_cancellation_closes_the_pending_request_on_the_receiver() {
    let (left_dir, right_dir, left, right, outgoing_id, incoming_id) =
        pending_transfer_pair().await;

    left.cancel(&outgoing_id).await.unwrap();
    wait_for_transfer_status(&right, &incoming_id, TransferStatus::Cancelled).await;

    assert_eq!(
        transfer_status(&left, &outgoing_id).await,
        TransferStatus::Cancelled
    );
    assert!(!right
        .pending_decisions
        .lock()
        .await
        .contains_key(&incoming_id));
    assert!(!right.jobs.lock().await.contains_key(&incoming_id));
    drop((left_dir, right_dir));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_cancellation_is_reported_as_cancelled_to_the_sender() {
    let (left_dir, right_dir, left, right, outgoing_id, incoming_id) =
        pending_transfer_pair().await;

    right.cancel(&incoming_id).await.unwrap();
    wait_for_transfer_status(&left, &outgoing_id, TransferStatus::Cancelled).await;

    assert_eq!(
        transfer_status(&right, &incoming_id).await,
        TransferStatus::Cancelled
    );
    drop((left_dir, right_dir));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_cancellation_stops_an_in_progress_transfer_on_both_sides() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Sender".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Receiver".into(),
    ))
    .await
    .unwrap();
    let left_network = test_network(left_dir.path(), "Sender").await;
    let right_network = test_network(right_dir.path(), "Receiver").await;
    pair_for_transfer(&left_network, &right_network).await;
    left.start_without_discovery(left_network.clone())
        .await
        .unwrap();
    right
        .start_without_discovery(right_network.clone())
        .await
        .unwrap();
    left.upsert_peer(network_peer(&right_network, "Receiver"))
        .await
        .unwrap();

    let source = left_dir.path().join("large.bin");
    std::fs::File::create(&source)
        .unwrap()
        .set_len(4 * 1024 * 1024)
        .unwrap();
    let outgoing_id = left
        .send_files(right_network.device_id().to_string(), vec![source])
        .await
        .unwrap();
    let incoming_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(transfer) = right.snapshot().await.transfers.iter().find(|transfer| {
                transfer.direction == TransferDirection::Receiving
                    && transfer.status == TransferStatus::AwaitingApproval
            }) {
                break transfer.id.clone();
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let incoming_id = match incoming_id {
        Ok(id) => id,
        Err(_) => panic!(
            "sender snapshot: {:?}; receiver snapshot: {:?}",
            left.snapshot().await.transfers,
            right.snapshot().await.transfers
        ),
    };

    left.jobs
        .lock()
        .await
        .get(&outgoing_id)
        .cloned()
        .unwrap()
        .pause()
        .await;
    right
        .respond_incoming(&incoming_id, true, false)
        .await
        .unwrap();
    wait_for_transfer_status(&right, &incoming_id, TransferStatus::Transferring).await;

    right.cancel(&incoming_id).await.unwrap();
    wait_for_transfer_status(&left, &outgoing_id, TransferStatus::Cancelled).await;
    assert_eq!(
        transfer_status(&right, &incoming_id).await,
        TransferStatus::Cancelled
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pairing_refreshes_an_already_visible_nearby_peer_as_trusted() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Left".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Right".into(),
    ))
    .await
    .unwrap();
    let left_network = test_network(left_dir.path(), "Left").await;
    let right_network = test_network(right_dir.path(), "Right").await;
    left.start(left_network.clone()).await.unwrap();
    right.start(right_network.clone()).await.unwrap();

    left_network
        .discovery()
        .observe_authenticated(network_advertisement(&right_network, "Right"))
        .await;
    right_network
        .discovery()
        .observe_authenticated(network_advertisement(&left_network, "Left"))
        .await;
    let left_peer_id = right_network.device_id().to_string();
    let right_peer_id = left_network.device_id().to_string();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if left
                .snapshot()
                .await
                .peers
                .iter()
                .any(|peer| peer.id == left_peer_id && !peer.paired)
                && right
                    .snapshot()
                    .await
                    .peers
                    .iter()
                    .any(|peer| peer.id == right_peer_id && !peer.paired)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    pair_for_transfer(&left_network, &right_network).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if left
                .snapshot()
                .await
                .peers
                .iter()
                .any(|peer| peer.id == left_peer_id && peer.paired)
                && right
                    .snapshot()
                    .await
                    .peers
                    .iter()
                    .any(|peer| peer.id == right_peer_id && peer.paired)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_image_offer_respects_receive_policy_and_contains_thumbnail() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Unpaired Sender".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Receiver".into(),
    ))
    .await
    .unwrap();
    let left_network = test_network(left_dir.path(), "Sender").await;
    let right_network = test_network(right_dir.path(), "Receiver").await;
    pair_for_transfer(&left_network, &right_network).await;
    left.start_without_discovery(left_network.clone())
        .await
        .unwrap();
    right
        .start_without_discovery(right_network.clone())
        .await
        .unwrap();
    left.upsert_peer(network_peer(&right_network, "Receiver"))
        .await
        .unwrap();
    let source = left_dir.path().join("preview.png");
    image::RgbaImage::from_pixel(8, 8, image::Rgba([80, 90, 220, 255]))
        .save(&source)
        .unwrap();
    let outgoing_id = left
        .send_files(right_network.device_id().to_string(), vec![source])
        .await
        .unwrap();

    let incoming_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = right.snapshot().await;
            if let Some(transfer) = snapshot
                .transfers
                .iter()
                .find(|value| value.status == TransferStatus::AwaitingApproval)
            {
                assert!(transfer.files[0]
                    .thumbnail_data_url
                    .as_deref()
                    .is_some_and(|value| value.starts_with("data:image/jpeg;base64,")));
                break transfer.id.clone();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    right
        .respond_incoming(&incoming_id, true, true)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = left.snapshot().await;
            if snapshot
                .transfers
                .iter()
                .find(|value| value.id == outgoing_id)
                .unwrap()
                .status
                == TransferStatus::Completed
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(right_network
        .paired_peers()
        .await
        .unwrap()
        .iter()
        .any(|peer| peer.device_id == left_network.device_id()));
    assert!(right_dir.path().join("inbox/preview.png").exists());
}

async fn test_network(root: &Path, name: &str) -> Arc<NetworkRuntime> {
    let repository: Arc<dyn PeerRepository> = Arc::new(
        SqlitePeerRepository::open(&root.join(format!("{name}-peers.sqlite3")))
            .await
            .unwrap(),
    );
    let mut config = NetworkRuntimeConfig::new(
        root.join(format!("{name}-identity")),
        DeviceMetadata {
            name: name.into(),
            platform: "test".into(),
            model: "test".into(),
        },
        repository,
    );
    config.listen_address = "127.0.0.1".parse().unwrap();
    NetworkRuntime::bind(config).await.unwrap()
}

async fn pending_transfer_pair() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Arc<TransferManager>,
    Arc<TransferManager>,
    String,
    String,
) {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Sender".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Receiver".into(),
    ))
    .await
    .unwrap();
    let left_network = test_network(left_dir.path(), "Sender").await;
    let right_network = test_network(right_dir.path(), "Receiver").await;
    left.start_without_discovery(left_network).await.unwrap();
    right
        .start_without_discovery(right_network.clone())
        .await
        .unwrap();
    left.upsert_peer(network_peer(&right_network, "Receiver"))
        .await
        .unwrap();

    let source = left_dir.path().join("pending.txt");
    std::fs::write(&source, b"cancel before approval").unwrap();
    let outgoing_id = left
        .send_files(right_network.device_id().to_string(), vec![source])
        .await
        .unwrap();
    let incoming_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(transfer) = right.snapshot().await.transfers.iter().find(|transfer| {
                transfer.direction == TransferDirection::Receiving
                    && transfer.status == TransferStatus::AwaitingApproval
            }) {
                break transfer.id.clone();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    (left_dir, right_dir, left, right, outgoing_id, incoming_id)
}

async fn transfer_status(manager: &TransferManager, id: &str) -> TransferStatus {
    manager
        .snapshot()
        .await
        .transfers
        .iter()
        .find(|transfer| transfer.id == id)
        .unwrap()
        .status
}

async fn wait_for_transfer_status(manager: &TransferManager, id: &str, expected: TransferStatus) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if transfer_status(manager, id).await == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

fn network_advertisement(network: &NetworkRuntime, name: &str) -> PeerAdvertisement {
    PeerAdvertisement {
        device_id: network.device_id(),
        public_key: network.public_key(),
        metadata: DeviceMetadata {
            name: name.into(),
            platform: "test".into(),
            model: "test".into(),
        },
        addresses: vec!["127.0.0.1".parse().unwrap()],
        connection_addresses: vec![(
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap(),
            network.local_port().unwrap(),
        )
            .into()],
        port: network.local_port().unwrap(),
        certificate_sha256: network.certificate_sha256(),
        last_seen_at_ms: now_ms(),
    }
}

fn network_peer(network: &NetworkRuntime, name: &str) -> NearbyPeer {
    use base64::Engine as _;
    NearbyPeer {
        id: network.device_id().to_string(),
        name: name.into(),
        platform: "test".into(),
        model: "test".into(),
        address: "127.0.0.1".into(),
        addresses: vec!["127.0.0.1".into()],
        port: network.local_port().unwrap(),
        public_key: base64::engine::general_purpose::STANDARD
            .encode(network.public_key().as_bytes()),
        certificate_sha256: network
            .certificate_sha256()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        paired: true,
        automatic_receive: false,
        last_seen_at_ms: now_ms(),
    }
}

async fn pair_for_transfer(left: &Arc<NetworkRuntime>, right: &Arc<NetworkRuntime>) {
    let mut incoming = right.subscribe();
    let left_session = left
        .connect(&network_advertisement(right, "Right"), SessionKind::Pairing)
        .await
        .unwrap();
    let right_session = tokio::time::timeout(Duration::from_secs(2), incoming.recv())
        .await
        .unwrap()
        .unwrap();
    left.confirm_pairing(&left_session).await.unwrap();
    right.confirm_pairing(&right_session).await.unwrap();
    left.grant(Grant {
        peer_id: right.device_id(),
        capability: CapabilityId::NearbyTransferSend,
        direction: GrantDirection::Outbound,
        constraints: GrantConstraints::None,
        granted_at_ms: now_ms(),
    })
    .await
    .unwrap();
    right
        .grant(Grant {
            peer_id: left.device_id(),
            capability: CapabilityId::NearbyTransferSend,
            direction: GrantDirection::Inbound,
            constraints: GrantConstraints::None,
            granted_at_ms: now_ms(),
        })
        .await
        .unwrap();
    left_session.close("test pairing complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_bidirectional_jobs_share_session_without_sharing_cancellation() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = TransferManager::new(TransferConfig::new(
        left_dir.path().join("config"),
        left_dir.path().join("inbox"),
        "Left".into(),
    ))
    .await
    .unwrap();
    let right = TransferManager::new(TransferConfig::new(
        right_dir.path().join("config"),
        right_dir.path().join("inbox"),
        "Right".into(),
    ))
    .await
    .unwrap();
    let ln = test_network(left_dir.path(), "Left").await;
    let rn = test_network(right_dir.path(), "Right").await;
    pair_for_transfer(&ln, &rn).await;
    for (network, peer, direction) in [
        (&ln, rn.device_id(), GrantDirection::Inbound),
        (&rn, ln.device_id(), GrantDirection::Outbound),
    ] {
        network
            .grant(Grant {
                peer_id: peer,
                capability: CapabilityId::NearbyTransferSend,
                direction,
                constraints: GrantConstraints::None,
                granted_at_ms: now_ms(),
            })
            .await
            .unwrap();
    }
    left.start_without_discovery(ln.clone()).await.unwrap();
    right.start_without_discovery(rn.clone()).await.unwrap();
    left.upsert_peer(network_peer(&rn, "Right")).await.unwrap();
    right.upsert_peer(network_peer(&ln, "Left")).await.unwrap();
    left.set_receive_policy(rn.device_id().as_str(), true)
        .await
        .unwrap();
    let a_path = left_dir.path().join("cancelled.txt");
    let b_path = left_dir.path().join("survivor.txt");
    let d_path = right_dir.path().join("reverse.txt");
    std::fs::write(&a_path, b"cancel me").unwrap();
    std::fs::write(&b_path, b"keep me").unwrap();
    std::fs::write(&d_path, b"reverse direction").unwrap();
    let a = left
        .send_files(rn.device_id().to_string(), vec![a_path])
        .await
        .unwrap();
    let b = left
        .send_files(rn.device_id().to_string(), vec![b_path])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while right.snapshot().await.transfers.len() != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let session = ln
        .connect(
            &network_advertisement(&rn, "Right"),
            SessionKind::FileTransfer,
        )
        .await
        .unwrap();
    left.cancel(&a).await.unwrap();
    let incoming_b = right
        .snapshot()
        .await
        .transfers
        .into_iter()
        .find(|t| t.files[0].name == "survivor.txt")
        .unwrap()
        .id;
    assert!(session.transport_handle().close_reason().is_none());
    right
        .respond_incoming(&incoming_b, true, false)
        .await
        .unwrap();
    let d = right
        .send_files(ln.device_id().to_string(), vec![d_path])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let bs = left
                .snapshot()
                .await
                .transfers
                .into_iter()
                .find(|t| t.id == b)
                .unwrap()
                .status;
            let ds = right
                .snapshot()
                .await
                .transfers
                .into_iter()
                .find(|t| t.id == d)
                .unwrap()
                .status;
            assert!(!matches!(
                bs,
                TransferStatus::Failed | TransferStatus::Cancelled
            ));
            assert!(!matches!(
                ds,
                TransferStatus::Failed | TransferStatus::Cancelled
            ));
            if bs == TransferStatus::Completed && ds == TransferStatus::Completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(session.transport_handle().close_reason().is_none());
    assert_eq!(
        std::fs::read(right_dir.path().join("inbox/survivor.txt")).unwrap(),
        b"keep me"
    );
    assert_eq!(
        std::fs::read(left_dir.path().join("inbox/reverse.txt")).unwrap(),
        b"reverse direction"
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(left.shutdown(), right.shutdown());
    })
    .await
    .unwrap();
    ln.shutdown("test complete");
    rn.shutdown("test complete");
}
