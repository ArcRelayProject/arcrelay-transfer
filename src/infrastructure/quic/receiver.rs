use super::*;

pub(super) async fn handle_incoming_offer(
    manager: Arc<TransferManager>,
    router: Arc<SessionRouter>,
    mut stream: arcrelay_network::FeatureStream,
) -> Result<()> {
    let session = router.session.clone();
    let network = manager
        .network
        .get()
        .ok_or_else(|| TransferError::Network("runtime unavailable".into()))?;
    if network
        .is_paired_session(&session)
        .await
        .map_err(|e| TransferError::Network(e.to_string()))?
    {
        network
            .require(&session, CapabilityId::NearbyTransferSend)
            .await
            .map_err(|e| TransferError::Network(e.to_string()))?;
    }
    let local_id = uuid::Uuid::new_v4().to_string();
    let control = manager.register_job(&local_id).await;
    control.attach(session.transport_handle()).await;
    let result = tokio::select! {
        _ = control.cancelled() => Err(TransferError::Cancelled),
        _ = manager.stopping.cancelled() => Err(TransferError::Cancelled),
        result = process_incoming_offer(&manager, &router, &local_id, &mut stream.send, &mut stream.receive) => result,
    };
    manager.pending_decisions.lock().await.remove(&local_id);
    manager.jobs.lock().await.remove(&local_id);
    if let Err(error) = &result {
        let code = if matches!(error, TransferError::Cancelled) {
            let _ = manager
                .transition(&local_id, TransferStatus::Cancelled)
                .await;
            TRANSFER_CANCELLED_CLOSE_CODE
        } else {
            manager.fail_transfer(&local_id, error.to_string()).await;
            FAILED_STREAM_CODE
        };
        let _ = stream.send.reset(code.into());
        let _ = stream.receive.stop(code.into());
    } else {
        let _ = stream.send.finish();
    }
    result
}

async fn process_incoming_offer(
    manager: &Arc<TransferManager>,
    router: &Arc<SessionRouter>,
    local_id: &str,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<()> {
    let session = &router.session;
    let connection = session.transport_handle();
    let frame: proto::ClientControlFrame =
        tokio::time::timeout(CONNECT_TIMEOUT, read_message(recv, MAX_CONTROL_FRAME_SIZE))
            .await
            .map_err(|_| TransferError::Network("transfer offer timeout".into()))??;
    let offer = match frame.body {
        Some(proto::client_control_frame::Body::TransferOffer(value)) => value,
        _ => {
            return Err(TransferError::Network(
                "transfer control stream is missing an offer".into(),
            ))
        }
    };
    validate_offer(&offer)?;
    let sender_id = session.peer().device_id.to_string();
    let sender_name = session.peer().metadata.name.clone();
    let sender_public_key = session.peer().public_key.as_bytes().to_vec();
    manager
        .observe_incoming_peer(
            &sender_id,
            &sender_name,
            &sender_public_key,
            connection.remote_address().ip(),
        )
        .await?;
    let paired = manager
        .is_paired_peer(&sender_id, Some(&sender_public_key))
        .await?;
    let automatic = paired
        && manager.policies.policy(&sender_id).await? == crate::domain::ReceivePolicy::Automatic;
    let now = now_ms();
    let files: Vec<_> = offer
        .files
        .iter()
        .map(|file| {
            let path = Path::new(&file.relative_path);
            let preview = offer
                .previews
                .iter()
                .find(|value| value.file_id == file.file_id)
                .and_then(|value| preview_data_url(&value.media_type, &value.data));
            TransferFileView {
                id: file.file_id,
                name: path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or(&file.relative_path)
                    .to_string(),
                relative_path: file.relative_path.clone(),
                local_path: None,
                receive_directory: None,
                size: file.size,
                media_type: file.media_type.clone(),
                kind: file_kind(path),
                thumbnail_data_url: preview,
                completed_bytes: 0,
            }
        })
        .collect();
    let decision_rx = if automatic {
        None
    } else {
        let (decision_tx, decision_rx) = tokio::sync::oneshot::channel();
        manager
            .pending_decisions
            .lock()
            .await
            .insert(local_id.to_string(), decision_tx);
        Some(decision_rx)
    };
    if let Err(error) = manager
        .push_transfer(TransferView {
            id: local_id.to_string(),
            wire_id: offer.transfer_id,
            peer_id: sender_id.clone(),
            peer_name: sender_name.clone(),
            direction: TransferDirection::Receiving,
            status: if automatic {
                TransferStatus::Connecting
            } else {
                TransferStatus::AwaitingApproval
            },
            files,
            total_bytes: offer.total_bytes,
            completed_bytes: 0,
            speed_bytes_per_second: 0,
            remaining_seconds: None,
            error_message: None,
            created_at_ms: now,
            updated_at_ms: now,
        })
        .await
    {
        manager.pending_decisions.lock().await.remove(local_id);
        manager.jobs.lock().await.remove(local_id);
        return Err(error);
    }
    let decision = if automatic {
        ReceiveDecision {
            accept: true,
            automatic_receive: false,
            cancelled: false,
        }
    } else {
        let decision_rx = decision_rx.expect("manual receive has a decision channel");
        tokio::select! {
            result = tokio::time::timeout(OFFER_TIMEOUT, decision_rx) => match result {
                Ok(Ok(value)) => value,
                _ => ReceiveDecision {
                    accept: false,
                    automatic_receive: false,
                    cancelled: false,
                },
            },
            error = peer_stopped(recv) => return Err(error),
        }
    };
    if !decision.accept {
        let (status, transfer_status, message) = if decision.cancelled {
            (
                proto::ErrorCode::Cancelled,
                TransferStatus::Cancelled,
                "receiver canceled the transfer",
            )
        } else {
            (
                proto::ErrorCode::PermissionDenied,
                TransferStatus::Rejected,
                "receiver rejected the transfer",
            )
        };
        let cancel = proto::TransferCancel {
            request_id: offer.request_id,
            transfer_id: offer.transfer_id,
            status: Some(error_status(status, message)),
        };
        write_message(
            send,
            &proto::ServerControlFrame {
                body: Some(proto::server_control_frame::Body::TransferCancel(cancel)),
            },
            MAX_CONTROL_FRAME_SIZE,
        )
        .await?;
        manager.transition(local_id, transfer_status).await?;
        manager.jobs.lock().await.remove(local_id);
        return Ok(());
    }
    if decision.automatic_receive && paired {
        manager.set_receive_policy(&sender_id, true).await?;
    }
    let receive_root = manager.config.read().await.receive_directory.clone();
    if let Err(error) = ensure_receive_capacity(&receive_root, offer.total_bytes) {
        let cancel = proto::TransferCancel {
            request_id: offer.request_id,
            transfer_id: offer.transfer_id,
            status: Some(error_status(
                proto::ErrorCode::ResourceExhausted,
                error.to_string(),
            )),
        };
        write_message(
            send,
            &proto::ServerControlFrame {
                body: Some(proto::server_control_frame::Body::TransferCancel(cancel)),
            },
            MAX_CONTROL_FRAME_SIZE,
        )
        .await?;
        manager.fail_transfer(local_id, error.to_string()).await;
        manager.jobs.lock().await.remove(local_id);
        return Ok(());
    }
    if !automatic {
        manager
            .transition(local_id, TransferStatus::Connecting)
            .await?;
    }
    let mut grants_by_file = HashMap::new();
    let grants = offer
        .files
        .iter()
        .map(|file| {
            let mut ticket = vec![0_u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut ticket);
            let expires_at_ms = now_ms().saturating_add(FILE_GRANT_TTL.as_millis() as i64);
            grants_by_file.insert(
                file.file_id,
                FileGrant {
                    ticket: ticket.clone(),
                    expires_at_ms,
                    used: false,
                },
            );
            proto::FileTransferGrant {
                file_id: file.file_id,
                ticket,
                expires_at_ms,
            }
        })
        .collect();
    let mut route = router.register(offer.transfer_id)?;
    let accept = proto::TransferAccept {
        request_id: offer.request_id,
        transfer_id: offer.transfer_id,
        grants,
        chunk_size: MAX_CHUNK_SIZE as u32,
    };
    write_message(
        send,
        &proto::ServerControlFrame {
            body: Some(proto::server_control_frame::Body::TransferAccept(accept)),
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;
    manager
        .transition(local_id, TransferStatus::Transferring)
        .await?;
    let started = Instant::now();
    tokio::select! {
        result = receive_files(manager.clone(), local_id.to_string(), &mut route, offer.clone(),
            Arc::new(tokio::sync::Mutex::new(grants_by_file)), started) => result?,
        error = peer_stopped(recv) => return Err(error),
    }
    manager
        .transition(local_id, TransferStatus::Completed)
        .await?;
    write_message(
        send,
        &proto::ServerControlFrame {
            body: Some(proto::server_control_frame::Body::TransferProgress(
                proto::TransferProgress {
                    transfer_id: offer.transfer_id,
                    file_id: 0,
                    completed_bytes: offer.total_bytes,
                    total_bytes: offer.total_bytes,
                    status: Some(ok_status()),
                },
            )),
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;
    Ok(())
}

fn ensure_receive_capacity(root: &Path, incoming_bytes: u64) -> Result<()> {
    let available = fs2::available_space(root)?;
    let reserve = MIN_RECEIVE_FREE_SPACE_RESERVE.max(incoming_bytes / 20);
    let required = incoming_bytes
        .checked_add(reserve)
        .ok_or_else(|| TransferError::Invalid("required receive space overflow".into()))?;
    if available < required {
        return Err(TransferError::Invalid(format!(
            "not enough free space to receive files (need {required} bytes, have {available})"
        )));
    }
    Ok(())
}

pub(super) async fn receive_files(
    manager: Arc<TransferManager>,
    local_id: String,
    route: &mut router::FileRoute,
    offer: proto::TransferOffer,
    grants: Arc<tokio::sync::Mutex<HashMap<u64, FileGrant>>>,
    started: Instant,
) -> Result<()> {
    let manifests: HashMap<_, _> = offer
        .files
        .iter()
        .cloned()
        .map(|manifest| (manifest.file_id, manifest))
        .collect();
    let mut received_ids = std::collections::HashSet::new();
    let mut receives = JoinSet::new();
    for _ in 0..offer.files.len() {
        while receives.len() >= FILE_STREAM_CONCURRENCY {
            join_transfer_task(&mut receives).await?;
        }
        let incoming = tokio::time::timeout(FILE_STREAM_OPEN_TIMEOUT, route.incoming.recv())
            .await
            .map_err(|_| TransferError::Network("timed out waiting for file stream".into()))?
            .ok_or_else(|| TransferError::Network("file stream router stopped".into()))?;
        let open = incoming.open;
        let send = incoming.stream.send;
        let recv = incoming.stream.receive;
        let manifest = manifests
            .get(&open.file_id)
            .cloned()
            .ok_or_else(|| TransferError::Invalid("file is not present in the manifest".into()))?;
        if !received_ids.insert(open.file_id) {
            return Err(TransferError::Invalid("duplicate file stream".into()));
        }
        consume_file_grant(&grants, &open, offer.transfer_id, now_ms()).await?;
        let manager = manager.clone();
        let local_id = local_id.clone();
        receives.spawn(async move {
            tokio::time::timeout(
                FILE_TRANSFER_TIMEOUT,
                receive_one_file(
                    manager,
                    local_id,
                    send,
                    recv,
                    offer.transfer_id,
                    manifest,
                    started,
                ),
            )
            .await
            .map_err(|_| TransferError::Network("timed out while receiving a file".into()))?
        });
    }
    while !receives.is_empty() {
        join_transfer_task(&mut receives).await?;
    }
    Ok(())
}

pub(super) async fn consume_file_grant(
    grants: &tokio::sync::Mutex<HashMap<u64, FileGrant>>,
    open: &proto::FileStreamOpen,
    transfer_id: u64,
    current_time_ms: i64,
) -> Result<()> {
    if open.transfer_id != transfer_id {
        return Err(TransferError::Invalid(
            "invalid or expired file authorization".into(),
        ));
    }
    let mut grants = grants.lock().await;
    let grant = grants
        .get_mut(&open.file_id)
        .ok_or_else(|| TransferError::Invalid("invalid or expired file authorization".into()))?;
    if grant.used || current_time_ms > grant.expires_at_ms || grant.ticket != open.ticket {
        return Err(TransferError::Invalid(
            "invalid or expired file authorization".into(),
        ));
    }
    grant.used = true;
    Ok(())
}

pub(super) async fn receive_one_file(
    manager: Arc<TransferManager>,
    local_id: String,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    transfer_id: u64,
    manifest: proto::FileManifestEntry,
    started: Instant,
) -> Result<()> {
    let relative = sanitize_relative_path(&manifest.relative_path)?;
    let _slot = manager
        .receive_slots
        .acquire()
        .await
        .map_err(|_| TransferError::Cancelled)?;
    let root = manager.config.read().await.receive_directory.clone();
    let staging = root.join(".arcrelay-staging");
    tokio::fs::create_dir_all(&staging).await?;
    let staging_meta = tokio::fs::symlink_metadata(&staging).await?;
    if !staging_meta.is_dir() || staging_meta.file_type().is_symlink() {
        return Err(TransferError::Invalid(
            "unsafe temporary receive directory".into(),
        ));
    }
    let temporary = staging.join(format!(
        "{}-{}-{}.part",
        transfer_id,
        manifest.file_id,
        uuid::Uuid::new_v4()
    ));
    let _cleanup = StagingCleanup(temporary.clone());
    let mut target = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await?;
    write_message(
        &mut send,
        &proto::FileStreamHeader {
            status: Some(ok_status()),
            transfer_id,
            file_id: manifest.file_id,
            chunk_size: MAX_CHUNK_SIZE as u32,
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;
    let control = manager.job(&local_id).await?;
    let mut expected_offset = 0_u64;
    let mut sha = Sha256::new();
    let receive_result: Result<()> = async {
        let mut buffer = vec![0_u8; MAX_CHUNK_SIZE];
        while expected_offset < manifest.size {
            control.wait_ready().await?;
            let wanted = (manifest.size - expected_offset).min(buffer.len() as u64) as usize;
            let count =
                tokio::time::timeout(FILE_STREAM_IDLE_TIMEOUT, recv.read(&mut buffer[..wanted]))
                    .await
                    .map_err(|_| TransferError::Network("file stream idle timeout".into()))?
                    .map_err(|e| TransferError::Network(e.to_string()))?
                    .ok_or_else(|| TransferError::Integrity("file stream ended early".into()))?;
            if count == 0 {
                return Err(TransferError::Integrity("empty file read".into()));
            }
            target.write_all(&buffer[..count]).await?;
            sha.update(&buffer[..count]);
            expected_offset += count as u64;
            update_progress(
                &manager,
                &local_id,
                manifest.file_id,
                expected_offset,
                started,
            )
            .await?;
        }
        let mut expected_digest = [0_u8; 32];
        tokio::time::timeout(
            FILE_STREAM_IDLE_TIMEOUT,
            recv.read_exact(&mut expected_digest),
        )
        .await
        .map_err(|_| TransferError::Network("file digest timeout".into()))?
        .map_err(|e| TransferError::Network(e.to_string()))?;
        let digest = sha.finalize().to_vec();
        if digest != expected_digest {
            return Err(TransferError::Integrity(
                "received file hash does not match".into(),
            ));
        }
        let trailing = tokio::time::timeout(FILE_STREAM_IDLE_TIMEOUT, recv.read(&mut [0_u8; 1]))
            .await
            .map_err(|_| TransferError::Network("file stream completion timeout".into()))?
            .map_err(|e| TransferError::Network(e.to_string()))?;
        if trailing.is_some() {
            return Err(TransferError::Integrity(
                "unexpected trailing file data".into(),
            ));
        }
        target.flush().await?;
        target.sync_all().await?;
        drop(target);
        let root_for_finalize = root.clone();
        let relative_for_finalize = relative.clone();
        let temporary_for_finalize = temporary.clone();
        let destination = tokio::task::spawn_blocking(move || {
            finalize_staged(
                &root_for_finalize,
                &relative_for_finalize,
                &temporary_for_finalize,
            )
        })
        .await
        .map_err(|error| TransferError::Io(std::io::Error::other(error)))??;
        manager
            .mutate_transfer(&local_id, |transfer| {
                let file = transfer
                    .files
                    .iter_mut()
                    .find(|f| f.id == manifest.file_id)
                    .ok_or_else(|| {
                        TransferError::Invalid("received file record is missing".into())
                    })?;
                file.local_path = Some(destination.to_string_lossy().into_owned());
                file.receive_directory = Some(root.to_string_lossy().into_owned());
                Ok(())
            })
            .await?;
        write_message(
            &mut send,
            &proto::FileStreamResult {
                status: Some(ok_status()),
                transfer_id,
                file_id: manifest.file_id,
                received_size: expected_offset,
                sha256: digest,
            },
            MAX_CONTROL_FRAME_SIZE,
        )
        .await?;
        Ok(())
    }
    .await;
    if receive_result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    receive_result
}

struct StagingCleanup(PathBuf);
impl Drop for StagingCleanup {
    fn drop(&mut self) {
        let path = self.0.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = tokio::fs::remove_file(path).await;
            });
        }
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::*;

    #[test]
    fn receive_capacity_keeps_a_free_space_reserve() {
        let directory = tempfile::tempdir().unwrap();
        let available = fs2::available_space(directory.path()).unwrap();
        assert!(ensure_receive_capacity(directory.path(), available).is_err());
    }
}
