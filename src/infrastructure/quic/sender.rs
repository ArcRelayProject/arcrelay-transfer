use super::*;

pub(super) fn peer_advertisement(peer: &NearbyPeer) -> Result<PeerAdvertisement> {
    let device_id = arcrelay_peer::DeviceId::parse(&peer.id)
        .map_err(|_| TransferError::Invalid("invalid device identity".into()))?;
    let public_key = DevicePublicKey::from_bytes(decode_key(&peer.public_key)?)
        .map_err(|_| TransferError::Invalid("invalid device public key".into()))?;
    device_id.verify_key(&public_key).map_err(|_| {
        TransferError::Invalid("device identifier does not match its public key".into())
    })?;
    let addresses = peer
        .addresses
        .iter()
        .filter_map(|address| address.parse().ok())
        .collect::<Vec<_>>();
    if addresses.is_empty() || peer.port == 0 {
        return Err(TransferError::Invalid(
            "device network address is unavailable".into(),
        ));
    }
    let connection_addresses = addresses
        .iter()
        .map(|address| std::net::SocketAddr::new(*address, peer.port))
        .collect();
    let certificate_sha256: [u8; 32] = decode_hex(&peer.certificate_sha256)?
        .try_into()
        .map_err(|_| TransferError::Invalid("invalid device certificate digest".into()))?;
    Ok(PeerAdvertisement {
        device_id,
        public_key,
        metadata: DeviceMetadata {
            name: peer.name.clone(),
            platform: peer.platform.clone(),
            model: peer.model.clone(),
        },
        addresses,
        connection_addresses,
        port: peer.port,
        certificate_sha256,
        last_seen_at_ms: peer.last_seen_at_ms,
    })
}

pub(super) fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(TransferError::Invalid(
            "invalid device certificate digest".into(),
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| TransferError::Invalid("invalid device certificate digest".into()))
        })
        .collect()
}

pub(super) async fn send_one_file(
    manager: &TransferManager,
    id: &str,
    session: &Arc<Session>,
    file: &PreparedFile,
    grant: &proto::FileTransferGrant,
    chunk_size: usize,
    started: Instant,
) -> Result<()> {
    let _slot = manager
        .send_slots
        .acquire()
        .await
        .map_err(|_| TransferError::Cancelled)?;
    let control = manager.job(id).await?;
    let open = proto::FileStreamOpen {
        transfer_id: manager
            .state
            .read()
            .await
            .transfers
            .iter()
            .find(|t| t.id == id)
            .ok_or_else(|| TransferError::TransferNotFound(id.into()))?
            .wire_id,
        file_id: file.manifest.file_id,
        ticket: grant.ticket.clone(),
    };
    let stream = session
        .open_feature_stream_versioned("arcrelay.transfer", 1, 0, 0, "file", &open.encode_to_vec())
        .await
        .map_err(|e| TransferError::Network(e.to_string()))?;
    let mut send = stream.send;
    let mut recv = stream.receive;
    let header: proto::FileStreamHeader = tokio::time::timeout(
        FILE_STREAM_IDLE_TIMEOUT,
        read_message(&mut recv, MAX_CONTROL_FRAME_SIZE),
    )
    .await
    .map_err(|_| {
        TransferError::Network("timed out waiting for file stream confirmation".into())
    })??;
    require_ok(header.status.as_ref())?;
    let mut source = file
        .source
        .as_ref()
        .ok_or_else(|| TransferError::InvalidState("prepared source handle is unavailable".into()))?
        .try_clone()?;
    source.seek(std::io::SeekFrom::Start(0))?;
    let mut source = tokio::fs::File::from_std(source);
    let mut offset = 0_u64;
    let mut sha = Sha256::new();
    let mut buffer = vec![0_u8; chunk_size];
    while offset < file.manifest.size {
        control.wait_ready().await?;
        let wanted = (file.manifest.size - offset).min(buffer.len() as u64) as usize;
        let count = source.read(&mut buffer[..wanted]).await?;
        if count == 0 {
            return Err(TransferError::Integrity("source file ended early".into()));
        }
        sha.update(&buffer[..count]);
        tokio::time::timeout(FILE_STREAM_IDLE_TIMEOUT, send.write_all(&buffer[..count]))
            .await
            .map_err(|_| TransferError::Network("file send timeout".into()))?
            .map_err(|e| TransferError::Network(e.to_string()))?;
        offset += count as u64;
        update_progress(manager, id, file.manifest.file_id, offset, started).await?;
    }
    let metadata = source.metadata().await?;
    if metadata.len() != file.manifest.size || metadata.modified().ok() != file.source_modified {
        return Err(TransferError::Integrity(
            "source file changed during transfer".into(),
        ));
    }
    let digest = sha.finalize().to_vec();
    send.write_all(&digest)
        .await
        .map_err(|e| TransferError::Network(e.to_string()))?;
    send.finish()
        .map_err(|e| TransferError::Network(e.to_string()))?;
    let result: proto::FileStreamResult = tokio::time::timeout(
        FILE_STREAM_IDLE_TIMEOUT,
        read_message(&mut recv, MAX_CONTROL_FRAME_SIZE),
    )
    .await
    .map_err(|_| {
        TransferError::Network("timed out waiting for file verification result".into())
    })??;
    require_ok(result.status.as_ref())?;
    if result.received_size != file.manifest.size || result.sha256 != digest {
        return Err(TransferError::Integrity(
            "receiver returned a mismatched verification result".into(),
        ));
    }
    Ok(())
}
