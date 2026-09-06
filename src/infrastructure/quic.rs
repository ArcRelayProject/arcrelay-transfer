use std::collections::HashMap;
#[cfg(test)]
use std::io::Read;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arcrelay_network::{DeviceMetadata, NetworkRuntime, PeerAdvertisement, Session, SessionKind};
use arcrelay_peer::{CapabilityId, DevicePublicKey};
use prost::Message;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;

use arcrelay_transport::{read_frame, write_frame};
use arcrelay_wire::proto;
use arcrelay_wire::MAX_CONTROL_FRAME_SIZE;

use crate::application::manager::{
    decode_key, file_name, now_ms, ReceiveDecision, TransferManager,
};
use crate::application::ports::PreviewGenerator;
use crate::domain::{
    NearbyPeer, TransferDirection, TransferFileView, TransferStatus, TransferView,
};
use crate::infrastructure::filesystem::{
    file_kind, finalize_staged, media_type, portable_path_key, preview_data_url,
    sanitize_relative_path, ImagePreviewGenerator, MAX_CHUNK_SIZE, MAX_FILE_COUNT,
    MAX_PREVIEW_BYTES, MAX_TOTAL_PREVIEW_BYTES,
};
use crate::{Result, TransferError};

const OFFER_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
// A grant is still single-use and bound to one authenticated transfer. Keep it
// valid for the complete transfer window so a large earlier file cannot make a
// later file's authorization expire before its stream is opened.
const FILE_GRANT_TTL: Duration = Duration::from_secs(65 * 60 * 60);
const FILE_STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_TRANSFER_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MIN_RECEIVE_FREE_SPACE_RESERVE: u64 = 64 * 1024 * 1024;

const FILE_STREAM_CONCURRENCY: usize = 4;
pub(crate) const TRANSFER_CANCELLED_CLOSE_CODE: u32 = 0x4152;
pub(crate) const FAILED_STREAM_CODE: u32 = 0x4153;

mod preparation;
pub(crate) mod router;
use router::*;
mod receiver;
mod sender;

use preparation::*;
use receiver::*;
use sender::*;

#[derive(Debug)]
struct PreparedFile {
    // Keep the prepared source open until the offer has been accepted. This
    // prevents a path replacement from silently changing which file is sent.
    // The sender clones and rewinds this handle instead of reopening `path`.
    source: Option<std::fs::File>,
    manifest: proto::FileManifestEntry,
    preview: Option<proto::TransferPreview>,
    source_modified: Option<std::time::SystemTime>,
}

#[derive(Debug)]
struct FileGrant {
    ticket: Vec<u8>,
    expires_at_ms: i64,
    used: bool,
}

pub(crate) async fn start(
    manager: Arc<TransferManager>,
    network: Arc<NetworkRuntime>,
    observe_discovery: bool,
) -> Result<()> {
    if observe_discovery {
        crate::infrastructure::discovery::observe(manager.clone(), network.clone()).await;
    }
    let mut incoming = network.subscribe();
    let tasks = manager.tasks.clone();
    tasks.spawn(async move {
        loop {
            let session = tokio::select! {
                _ = manager.stopping.cancelled() => break,
                incoming = incoming.recv() => match incoming {
                    Ok(session) => session,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                },
            };
            if session.kind() == SessionKind::FileTransfer {
                ensure_router(&manager, session).await;
            }
        }
    });
    Ok(())
}

pub(crate) async fn insert_preparing_transfer(
    manager: &TransferManager,
    id: &str,
    peer: &NearbyPeer,
    paths: &[PathBuf],
) -> Result<()> {
    let mut files = Vec::with_capacity(paths.len());
    let mut total = 0_u64;
    for (index, path) in paths.iter().enumerate() {
        let size = tokio::fs::metadata(path).await?.len();
        total = total
            .checked_add(size)
            .ok_or_else(|| TransferError::Invalid("transfer file is too large".into()))?;
        let name = file_name(path)?;
        files.push(TransferFileView {
            id: index as u64 + 1,
            name: name.clone(),
            relative_path: name,
            local_path: None,
            receive_directory: None,
            size,
            media_type: media_type(path).into(),
            kind: file_kind(path),
            thumbnail_data_url: None,
            completed_bytes: 0,
        });
    }
    let now = now_ms();
    manager
        .push_transfer(TransferView {
            id: id.into(),
            wire_id: random_nonzero_u64(),
            peer_id: peer.id.clone(),
            peer_name: peer.name.clone(),
            direction: TransferDirection::Sending,
            status: TransferStatus::Preparing,
            files,
            total_bytes: total,
            completed_bytes: 0,
            speed_bytes_per_second: 0,
            remaining_seconds: None,
            error_message: None,
            created_at_ms: now,
            updated_at_ms: now,
        })
        .await
}

pub(crate) async fn send_files(
    manager: Arc<TransferManager>,
    id: String,
    peer: NearbyPeer,
    paths: Vec<PathBuf>,
) -> Result<()> {
    let prepared = prepare_files(manager.clone(), id.clone(), paths).await?;
    manager
        .mutate_transfer(&id, |transfer| {
            for file in &mut transfer.files {
                file.completed_bytes = 0;
            }
            transfer.completed_bytes = 0;
            transfer.speed_bytes_per_second = 0;
            transfer.remaining_seconds = None;
            Ok(())
        })
        .await?;
    manager.transition(&id, TransferStatus::Connecting).await?;
    let wire_id = manager
        .state
        .read()
        .await
        .transfers
        .iter()
        .find(|value| value.id == id)
        .ok_or_else(|| TransferError::TransferNotFound(id.clone()))?
        .wire_id;
    manager
        .mutate_transfer(&id, |transfer| {
            for file in &mut transfer.files {
                if let Some(prepared) = prepared
                    .iter()
                    .find(|value| value.manifest.file_id == file.id)
                {
                    file.thumbnail_data_url = prepared
                        .preview
                        .as_ref()
                        .and_then(|preview| preview_data_url(&preview.media_type, &preview.data));
                }
            }
            Ok(())
        })
        .await?;
    let network =
        manager.network.get().cloned().ok_or_else(|| {
            TransferError::InvalidState("transfer service has not started".into())
        })?;
    let advertisement = peer_advertisement(&peer)?;
    // Prefer the live discovery record because it retains the interface scope
    // required to route IPv6 link-local addresses. Persisted transfer views
    // intentionally contain only display-friendly IP strings.
    let session = if network.discovery().peer(&advertisement.device_id).is_some() {
        network
            .connect_discovered(&advertisement.device_id, SessionKind::FileTransfer)
            .await
    } else {
        network
            .connect(&advertisement, SessionKind::FileTransfer)
            .await
    }
    .map_err(|error| TransferError::Network(error.to_string()))?;
    ensure_router(&manager, session.clone()).await;
    let control = manager.job(&id).await?;
    control.attach(session.transport_handle()).await;
    let mut stream = session
        .open_feature_stream_versioned("arcrelay.transfer", 1, 0, 0, "offer", &[])
        .await
        .map_err(|error| TransferError::Network(error.to_string()))?;
    let result = tokio::select! {
        _ = control.cancelled() => Err(TransferError::Cancelled),
        _ = manager.stopping.cancelled() => Err(TransferError::Cancelled),
        result = send_offer_and_files(&manager, &id, session, prepared, wire_id, &mut stream.send, &mut stream.receive) => result,
    };
    if result.is_err() {
        let code = if matches!(result, Err(TransferError::Cancelled)) {
            TRANSFER_CANCELLED_CLOSE_CODE
        } else {
            FAILED_STREAM_CODE
        };
        let _ = stream.send.reset(code.into());
        let _ = stream.receive.stop(code.into());
    } else {
        let _ = stream.send.finish();
        manager.transition(&id, TransferStatus::Completed).await?;
    }
    result
}

async fn send_offer_and_files(
    manager: &Arc<TransferManager>,
    id: &str,
    session: Arc<Session>,
    prepared: Vec<PreparedFile>,
    wire_id: u64,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<()> {
    let offer = build_offer(wire_id, &prepared)?;
    write_message(
        send,
        &proto::ClientControlFrame {
            body: Some(proto::client_control_frame::Body::TransferOffer(
                offer.clone(),
            )),
        },
        MAX_CONTROL_FRAME_SIZE,
    )
    .await?;
    manager
        .transition(id, TransferStatus::AwaitingApproval)
        .await?;
    let response: proto::ServerControlFrame =
        match tokio::time::timeout(OFFER_TIMEOUT, read_message(recv, MAX_CONTROL_FRAME_SIZE)).await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(TransferError::Network(
                    "timed out waiting for receiver confirmation".into(),
                ))
            }
        };
    let accept = match response.body {
        Some(proto::server_control_frame::Body::TransferAccept(accept)) => accept,
        Some(proto::server_control_frame::Body::TransferCancel(cancel)) => {
            let cancelled = cancel
                .status
                .as_ref()
                .is_some_and(|status| status.code == proto::ErrorCode::Cancelled as i32);
            let message = cancel
                .status
                .map(|value| value.message)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "receiver rejected the transfer".into());
            manager
                .transition(
                    id,
                    if cancelled {
                        TransferStatus::Cancelled
                    } else {
                        TransferStatus::Rejected
                    },
                )
                .await?;
            return Err(TransferError::InvalidState(message));
        }
        _ => {
            return Err(TransferError::Network(
                "receiver returned an invalid response".into(),
            ))
        }
    };
    verify_accept(&accept, wire_id, offer.request_id, &prepared)?;
    manager.transition(id, TransferStatus::Connecting).await?;
    manager.transition(id, TransferStatus::Transferring).await?;
    let grants: HashMap<u64, proto::FileTransferGrant> = accept
        .grants
        .into_iter()
        .map(|grant| (grant.file_id, grant))
        .collect();
    let chunk_size = (accept.chunk_size as usize).clamp(16 * 1024, MAX_CHUNK_SIZE);
    let started = Instant::now();
    let sending = async {
        let prepared: Vec<_> = prepared.into_iter().map(Arc::new).collect();
        let mut sends = JoinSet::new();
        for file in prepared {
            let grant = grants.get(&file.manifest.file_id).cloned().ok_or_else(|| {
                TransferError::Network("receiver did not authorize the file".into())
            })?;
            while sends.len() >= FILE_STREAM_CONCURRENCY {
                join_transfer_task(&mut sends).await?;
            }
            let manager = manager.clone();
            let id = id.to_string();
            let session = session.clone();
            sends.spawn(async move {
                tokio::time::timeout(
                    FILE_TRANSFER_TIMEOUT,
                    send_one_file(&manager, &id, &session, &file, &grant, chunk_size, started),
                )
                .await
                .map_err(|_| TransferError::Network("timed out while sending a file".into()))?
            });
        }
        while !sends.is_empty() {
            join_transfer_task(&mut sends).await?;
        }
        Ok::<(), TransferError>(())
    };
    let completion = async {
        let frame: proto::ServerControlFrame = read_message(recv, MAX_CONTROL_FRAME_SIZE).await?;
        match frame.body {
            Some(proto::server_control_frame::Body::TransferProgress(progress))
                if progress.transfer_id == wire_id
                    && progress.completed_bytes == offer.total_bytes
                    && progress.total_bytes == offer.total_bytes =>
            {
                require_ok(progress.status.as_ref())
            }
            _ => Err(TransferError::Network("invalid transfer completion".into())),
        }
    };
    tokio::try_join!(sending, completion)?;
    Ok(())
}

pub(super) async fn peer_stopped(recv: &mut quinn::RecvStream) -> TransferError {
    match recv.read(&mut [0_u8; 1]).await {
        Err(quinn::ReadError::Reset(code)) if code == TRANSFER_CANCELLED_CLOSE_CODE.into() => {
            TransferError::Cancelled
        }
        Ok(_) => TransferError::Network("transfer control stream ended unexpectedly".into()),
        Err(error) => TransferError::Network(error.to_string()),
    }
}

fn encoded_offer_frame_len(offer: &proto::TransferOffer) -> usize {
    proto::ClientControlFrame {
        body: Some(proto::client_control_frame::Body::TransferOffer(
            offer.clone(),
        )),
    }
    .encoded_len()
}

async fn join_transfer_task(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    tasks
        .join_next()
        .await
        .ok_or_else(|| TransferError::InvalidState("transfer task ended unexpectedly".into()))?
        .map_err(|error| TransferError::Network(format!("transfer task failed: {error}")))?
}

fn validate_offer(offer: &proto::TransferOffer) -> Result<()> {
    if offer.request_id == 0 || offer.transfer_id == 0 {
        return Err(TransferError::Invalid(
            "offer is missing required fields".into(),
        ));
    }
    if offer.files.is_empty()
        || offer.files.len() > MAX_FILE_COUNT
        || offer.total_bytes > MAX_TOTAL_BYTES
    {
        return Err(TransferError::Invalid(
            "offer has an invalid file count or size".into(),
        ));
    }
    if offer.created_at_ms <= 0 {
        return Err(TransferError::Invalid(
            "offer has an invalid timestamp".into(),
        ));
    }
    let mut file_ids = std::collections::HashSet::new();
    let mut portable_paths = std::collections::HashSet::new();
    let total = offer.files.iter().try_fold(0_u64, |total, file| {
        sanitize_relative_path(&file.relative_path)?;
        if file.file_id == 0
            || !file_ids.insert(file.file_id)
            || !portable_paths.insert(portable_path_key(&file.relative_path))
        {
            return Err(TransferError::Invalid("invalid manifest".into()));
        }
        total
            .checked_add(file.size)
            .ok_or_else(|| TransferError::Invalid("manifest size overflow".into()))
    })?;
    if total != offer.total_bytes {
        return Err(TransferError::Invalid(
            "manifest total size does not match".into(),
        ));
    }
    let total_preview_bytes = offer
        .previews
        .iter()
        .try_fold(0_usize, |total, preview| {
            total.checked_add(preview.data.len())
        })
        .ok_or_else(|| TransferError::Invalid("total file preview size overflow".into()))?;
    let mut preview_ids = std::collections::HashSet::new();
    if total_preview_bytes > MAX_TOTAL_PREVIEW_BYTES
        || offer.previews.iter().any(|preview| {
            preview.data.len() > MAX_PREVIEW_BYTES
                || !preview.media_type.starts_with("image/")
                || !preview_ids.insert(preview.file_id)
                || !offer
                    .files
                    .iter()
                    .any(|file| file.file_id == preview.file_id)
        })
    {
        return Err(TransferError::Invalid("invalid file preview".into()));
    }
    Ok(())
}

fn verify_accept(
    accept: &proto::TransferAccept,
    transfer_id: u64,
    request_id: u64,
    prepared: &[PreparedFile],
) -> Result<()> {
    if accept.transfer_id != transfer_id
        || accept.request_id != request_id
        || accept.chunk_size < 16 * 1024
        || accept.chunk_size as usize > MAX_CHUNK_SIZE
    {
        return Err(TransferError::Network(
            "receiver returned invalid authorization".into(),
        ));
    }
    let expected = prepared
        .iter()
        .map(|file| file.manifest.file_id)
        .collect::<std::collections::HashSet<_>>();
    let granted = accept
        .grants
        .iter()
        .map(|grant| grant.file_id)
        .collect::<std::collections::HashSet<_>>();
    if granted.len() != accept.grants.len()
        || granted != expected
        || accept
            .grants
            .iter()
            .any(|grant| grant.ticket.len() != 32 || grant.expires_at_ms <= 0)
    {
        return Err(TransferError::Network(
            "receiver returned incomplete file authorization".into(),
        ));
    }
    Ok(())
}

async fn update_progress(
    manager: &TransferManager,
    id: &str,
    file_id: u64,
    completed: u64,
    started: Instant,
) -> Result<()> {
    manager
        .record_progress_live(id, file_id, completed, started)
        .await
}

async fn write_message<W: tokio::io::AsyncWrite + Unpin, M: Message>(
    writer: &mut W,
    message: &M,
    maximum: usize,
) -> Result<()> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes).map_err(|error| {
        TransferError::Network(format!("failed to encode transfer frame: {error}"))
    })?;
    write_frame(writer, &bytes, maximum)
        .await
        .map_err(|error| TransferError::Network(error.to_string()))
}

async fn read_message<R: tokio::io::AsyncRead + Unpin, M: Message + Default>(
    reader: &mut R,
    maximum: usize,
) -> Result<M> {
    let bytes = read_frame(reader, maximum)
        .await
        .map_err(|error| match error {
            arcrelay_transport::FrameError::Io(error) if error.get_ref().and_then(|e| e.downcast_ref::<quinn::ReadError>())
                .is_some_and(|e| matches!(e, quinn::ReadError::Reset(code) if *code == TRANSFER_CANCELLED_CLOSE_CODE.into())) => TransferError::Cancelled,
            error => TransferError::Network(error.to_string()),
        })?;
    M::decode(bytes.as_slice()).map_err(|error| {
        TransferError::Network(format!("failed to decode transfer frame: {error}"))
    })
}

fn ok_status() -> proto::Status {
    proto::Status {
        code: proto::ErrorCode::Ok as i32,
        message: String::new(),
        retryable: false,
        retry_after_ms: 0,
        recovery_action: proto::RecoveryAction::None as i32,
        error_id: String::new(),
    }
}
fn error_status(code: proto::ErrorCode, message: impl Into<String>) -> proto::Status {
    let recovery_action = match code {
        proto::ErrorCode::Busy
        | proto::ErrorCode::ResourceExhausted
        | proto::ErrorCode::DeadlineExceeded
        | proto::ErrorCode::Unavailable => proto::RecoveryAction::RetryWithBackoff,
        proto::ErrorCode::Conflict => proto::RecoveryAction::Refresh,
        proto::ErrorCode::InvalidArgument | proto::ErrorCode::FailedPrecondition => {
            proto::RecoveryAction::ChangeRequest
        }
        _ => proto::RecoveryAction::None,
    };
    proto::Status {
        code: code as i32,
        message: message.into(),
        retryable: matches!(
            recovery_action,
            proto::RecoveryAction::Retry | proto::RecoveryAction::RetryWithBackoff
        ),
        retry_after_ms: 0,
        recovery_action: recovery_action as i32,
        error_id: String::new(),
    }
}
fn require_ok(status: Option<&proto::Status>) -> Result<()> {
    match status {
        Some(value) if value.code == proto::ErrorCode::Ok as i32 => Ok(()),
        Some(value) => Err(TransferError::Network(value.message.clone())),
        None => Err(TransferError::Network(
            "response is missing a status".into(),
        )),
    }
}

fn random_nonzero_u64() -> u64 {
    loop {
        let value = rand::random();
        if value != 0 {
            return value;
        }
    }
}

#[cfg(test)]
mod tests;
