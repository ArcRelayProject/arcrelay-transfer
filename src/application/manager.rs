use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arcrelay_network::NetworkRuntime;
use arcrelay_peer::DeviceId;
use tokio::sync::{broadcast, oneshot, watch, Mutex, Notify, OnceCell, RwLock, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::ports::{ReceivePolicyRepository, TransferRepository, TransferSettingsRepository};
use crate::domain::{NearbyPeer, ReceivePolicy, TransferSnapshot, TransferStatus, TransferView};
use crate::infrastructure::persistence::{
    JsonReceivePolicyRepository, JsonTransferRepository, JsonTransferSettingsRepository,
};
use crate::{Result, TransferError};

#[derive(Debug, Clone)]
pub struct TransferConfig {
    pub config_directory: PathBuf,
    pub receive_directory: PathBuf,
    pub device_name: String,
    pub listen_port: u16,
    pub platform: String,
    pub model: String,
}

impl TransferConfig {
    pub fn new(config_directory: PathBuf, receive_directory: PathBuf, device_name: String) -> Self {
        Self {
            config_directory,
            receive_directory,
            device_name,
            listen_port: 0,
            platform: std::env::consts::OS.into(),
            model: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceiveDecision {
    pub accept: bool,
    pub automatic_receive: bool,
    pub cancelled: bool,
}

pub(crate) struct JobControl {
    paused: AtomicBool,
    cancelled: CancellationToken,
    changed: Notify,
    connection: Mutex<Option<quinn::Connection>>,
}

impl JobControl {
    fn new() -> Self {
        Self {
            paused: AtomicBool::new(false),
            cancelled: CancellationToken::new(),
            changed: Notify::new(),
            connection: Mutex::new(None),
        }
    }

    pub async fn attach(&self, connection: quinn::Connection) {
        *self.connection.lock().await = Some(connection.clone());
        self.changed.notify_waiters();
    }

    pub async fn wait_ready(&self) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            if self.cancelled.is_cancelled() {
                return Err(TransferError::Cancelled);
            }
            if !self.paused.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(connection) = self.connection.lock().await.clone() {
                tokio::select! {
                    () = changed => {}
                    error = connection.closed() => {
                        return Err(TransferError::Network(error.to_string()));
                    }
                }
            } else {
                changed.await;
            }
        }
    }

    pub(crate) async fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }
    async fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }
    pub(crate) async fn cancel(&self) {
        self.cancelled.cancel();
        self.changed.notify_waiters();
    }

    pub(crate) async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }
}

pub struct TransferManager {
    pub(crate) config: RwLock<TransferConfig>,
    pub(crate) state: RwLock<TransferSnapshot>,
    pub(crate) policies: Arc<dyn ReceivePolicyRepository>,
    settings: Arc<dyn TransferSettingsRepository>,
    pub(crate) snapshots: watch::Sender<Arc<TransferSnapshot>>,
    pub(crate) errors: broadcast::Sender<String>,
    history_updates: watch::Sender<Option<Arc<Vec<TransferView>>>>,
    history_stopping: CancellationToken,
    history_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) jobs: Mutex<HashMap<String, Arc<JobControl>>>,
    pub(crate) pending_decisions: Mutex<HashMap<String, oneshot::Sender<ReceiveDecision>>>,
    pub(crate) network: OnceCell<Arc<NetworkRuntime>>,
    pub(crate) discoverable: AtomicBool,
    started: AtomicBool,
    pub(crate) resources: Arc<arcrelay_content::ContentResources>,
    pub(crate) job_slots: Arc<Semaphore>,
    pub(crate) send_slots: Arc<Semaphore>,
    pub(crate) receive_slots: Arc<Semaphore>,
    pub(crate) routers:
        Mutex<HashMap<usize, std::sync::Weak<crate::infrastructure::quic::router::SessionRouter>>>,
    pub(crate) stopping: CancellationToken,
    pub(crate) tasks: TaskTracker,
    progress: broadcast::Sender<crate::domain::TransferProgress>,
    last_full_revision: std::sync::atomic::AtomicU64,
    progress_times: Mutex<HashMap<String, ProgressEmission>>,
}

#[derive(Default)]
struct ProgressEmission {
    last: Option<std::time::Instant>,
    dirty: HashMap<u64, u64>,
}

impl TransferManager {
    pub async fn new(config: TransferConfig) -> Result<Arc<Self>> {
        Self::with_resources(
            config,
            Arc::new(arcrelay_content::ContentResources::default()),
        )
        .await
    }

    pub async fn with_resources(
        mut config: TransferConfig,
        resources: Arc<arcrelay_content::ContentResources>,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.config_directory)?;
        let settings: Arc<dyn TransferSettingsRepository> =
            Arc::new(JsonTransferSettingsRepository::new(
                config.config_directory.join("transfer-settings.json"),
            ));
        if let Some(receive_directory) = settings.load_receive_directory().await? {
            config.receive_directory = receive_directory;
        }
        ensure_receive_directory(&config.receive_directory)?;
        let history: Arc<dyn TransferRepository> = Arc::new(JsonTransferRepository::new(
            config.config_directory.join("transfer-history.json"),
        ));
        let policies: Arc<dyn ReceivePolicyRepository> =
            Arc::new(JsonReceivePolicyRepository::load(
                config.config_directory.join("receive-policies.json"),
            )?);
        let mut transfers = history.load().await?;
        prune_transfer_history(&mut transfers);
        let initial_snapshot = TransferSnapshot {
            revision: 0,
            device_id: String::new(),
            device_name: config.device_name.clone(),
            receive_directory: config.receive_directory.to_string_lossy().into_owned(),
            discoverable: true,
            peers: Vec::new(),
            transfers,
        };
        let (snapshots, _) = watch::channel(Arc::new(initial_snapshot.clone()));
        let (errors, _) = broadcast::channel(32);
        let (history_updates, history_receiver) = watch::channel(None);
        let manager = Arc::new(Self {
            state: RwLock::new(initial_snapshot),
            config: RwLock::new(config),
            policies,
            settings,
            snapshots,
            errors: errors.clone(),
            history_updates,
            history_stopping: CancellationToken::new(),
            history_task: Mutex::new(None),
            jobs: Mutex::new(HashMap::new()),
            pending_decisions: Mutex::new(HashMap::new()),
            network: OnceCell::new(),
            discoverable: AtomicBool::new(true),
            started: AtomicBool::new(false),
            resources,
            job_slots: Arc::new(Semaphore::new(16)),
            send_slots: Arc::new(Semaphore::new(4)),
            receive_slots: Arc::new(Semaphore::new(4)),
            routers: Mutex::new(HashMap::new()),
            stopping: CancellationToken::new(),
            tasks: TaskTracker::new(),
            progress: broadcast::channel(256).0,
            last_full_revision: std::sync::atomic::AtomicU64::new(0),
            progress_times: Mutex::new(HashMap::new()),
        });
        *manager.history_task.lock().await = Some(tokio::spawn(persist_history_updates(
            history,
            history_receiver,
            errors,
            manager.history_stopping.clone(),
        )));
        Ok(manager)
    }

    pub async fn start(self: &Arc<Self>, network: Arc<NetworkRuntime>) -> Result<()> {
        self.start_inner(network, true).await
    }

    #[cfg(test)]
    pub(crate) async fn start_without_discovery(
        self: &Arc<Self>,
        network: Arc<NetworkRuntime>,
    ) -> Result<()> {
        self.start_inner(network, false).await
    }

    async fn start_inner(
        self: &Arc<Self>,
        network: Arc<NetworkRuntime>,
        observe_discovery: bool,
    ) -> Result<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if self.network.get().is_none() {
            self.network.set(network.clone()).map_err(|_| {
                TransferError::InvalidState("transfer runtime is already bound".into())
            })?;
        }
        network.set_discoverable(self.discoverable.load(Ordering::Acquire));
        {
            let metadata = network.metadata();
            let mut state = self.state.write().await;
            state.device_id = network.device_id().to_string();
            state.device_name = metadata.name;
        }
        let start_result =
            crate::infrastructure::quic::start(self.clone(), network, observe_discovery).await;
        if let Err(error) = start_result {
            self.started.store(false, Ordering::Release);
            return Err(error);
        }
        self.emit_snapshot().await;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.stopping.cancel();
        self.job_slots.close();
        let jobs = self.jobs.lock().await.values().cloned().collect::<Vec<_>>();
        for job in jobs {
            job.cancel().await;
        }
        self.tasks.close();
        self.tasks.wait().await;
        self.history_stopping.cancel();
        if let Some(task) = self.history_task.lock().await.take() {
            let _ = task.await;
        }
    }

    pub async fn snapshot(&self) -> TransferSnapshot {
        self.state.read().await.clone()
    }

    pub fn events(self: &Arc<Self>) -> crate::TransferSubscription {
        crate::TransferSubscription::new(self.clone())
    }

    pub(crate) async fn event_baseline(&self) -> (TransferSnapshot, u64) {
        let state = self.state.read().await;
        (
            state.clone(),
            self.last_full_revision.load(Ordering::Acquire),
        )
    }

    pub fn device_id(&self) -> String {
        self.network
            .get()
            .map(|network| network.device_id().to_string())
            .unwrap_or_default()
    }

    pub async fn device_name(&self) -> String {
        self.config.read().await.device_name.clone()
    }

    pub fn public_key(&self) -> Vec<u8> {
        self.network
            .get()
            .map(|network| network.public_key().as_bytes().to_vec())
            .unwrap_or_default()
    }

    pub fn public_key_base64(&self) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(self.public_key())
    }

    pub fn sign(&self, bytes: &[u8]) -> Vec<u8> {
        self.network
            .get()
            .map(|network| network.sign_feature_payload(bytes).as_bytes().to_vec())
            .unwrap_or_default()
    }
    pub fn subscribe_progress(&self) -> broadcast::Receiver<crate::domain::TransferProgress> {
        self.progress.subscribe()
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<TransferSnapshot>> {
        self.snapshots.subscribe()
    }

    pub fn subscribe_errors(&self) -> broadcast::Receiver<String> {
        self.errors.subscribe()
    }

    pub async fn refresh_devices(&self) -> Result<TransferSnapshot> {
        if let Some(network) = self.network.get() {
            network
                .scan_local_ipv4()
                .await
                .map_err(|error| TransferError::Network(error.to_string()))?;
        }
        self.emit_snapshot().await;
        Ok(self.snapshot().await)
    }

    pub fn is_discoverable(&self) -> bool {
        self.discoverable.load(Ordering::Acquire)
    }

    pub async fn set_discoverable(&self, discoverable: bool) {
        self.discoverable.store(discoverable, Ordering::Release);
        if let Some(network) = self.network.get() {
            network.set_discoverable(discoverable);
        }
        self.state.write().await.discoverable = discoverable;
        self.emit_snapshot().await;
    }

    pub async fn set_device_name(&self, device_name: String) -> Result<()> {
        self.config.write().await.device_name = device_name.clone();
        self.state.write().await.device_name = device_name.clone();
        self.emit_snapshot().await;
        if let Some(network) = self.network.get() {
            let mut metadata = network.metadata();
            metadata.name = device_name;
            network
                .update_metadata(metadata)
                .map_err(|error| TransferError::Network(error.to_string()))?;
        }
        Ok(())
    }

    pub fn certificate_sha256(&self) -> String {
        self.network
            .get()
            .map(|network| {
                network
                    .certificate_sha256()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn network_runtime(&self) -> Option<Arc<NetworkRuntime>> {
        self.network.get().cloned()
    }

    pub async fn send_files(
        self: &Arc<Self>,
        peer_id: String,
        paths: Vec<PathBuf>,
    ) -> Result<String> {
        if self.stopping.is_cancelled() {
            return Err(TransferError::InvalidState(
                "transfer runtime is stopping".into(),
            ));
        }
        let slot = self
            .job_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransferError::InvalidState("transfer capacity exhausted".into()))?;
        if paths.is_empty() {
            return Err(TransferError::Invalid("select at least one file".into()));
        }
        if paths.len() > crate::infrastructure::filesystem::MAX_FILE_COUNT {
            return Err(TransferError::Invalid(
                "no more than 256 files can be sent at once".into(),
            ));
        }
        for path in &paths {
            let metadata = tokio::fs::symlink_metadata(path).await?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(TransferError::Invalid(format!(
                    "not a regular file that can be sent: {}",
                    path.display()
                )));
            }
        }
        let peer = self
            .state
            .read()
            .await
            .peers
            .iter()
            .find(|peer| peer.id == peer_id)
            .cloned()
            .ok_or_else(|| TransferError::PeerNotFound(peer_id.clone()))?;
        let id = uuid::Uuid::new_v4().to_string();
        let control = Arc::new(JobControl::new());
        crate::infrastructure::quic::insert_preparing_transfer(self, &id, &peer, &paths).await?;
        self.jobs.lock().await.insert(id.clone(), control);
        let manager = self.clone();
        let task_id = id.clone();
        self.tasks.spawn(async move {
            let _slot = slot;
            if let Err(error) = crate::infrastructure::quic::send_files(
                manager.clone(),
                task_id.clone(),
                peer,
                paths,
            )
            .await
            {
                if matches!(error, TransferError::Cancelled) {
                    let _ = manager
                        .transition(&task_id, TransferStatus::Cancelled)
                        .await;
                } else {
                    manager.fail_transfer(&task_id, error.to_string()).await;
                }
            }
            manager.jobs.lock().await.remove(&task_id);
        });
        Ok(id)
    }

    pub async fn respond_incoming(
        &self,
        transfer_id: &str,
        accept: bool,
        automatic_receive: bool,
    ) -> Result<()> {
        let sender = self
            .pending_decisions
            .lock()
            .await
            .remove(transfer_id)
            .ok_or_else(|| {
                TransferError::InvalidState("receive request was already handled or expired".into())
            })?;
        sender
            .send(ReceiveDecision {
                accept,
                automatic_receive,
                cancelled: false,
            })
            .map_err(|_| TransferError::InvalidState("receive connection is already closed".into()))
    }

    pub async fn pause(&self, transfer_id: &str) -> Result<()> {
        let control = self
            .jobs
            .lock()
            .await
            .get(transfer_id)
            .cloned()
            .ok_or_else(|| TransferError::TransferNotFound(transfer_id.into()))?;
        control.pause().await;
        self.transition(transfer_id, TransferStatus::Paused).await
    }

    pub async fn resume(&self, transfer_id: &str) -> Result<()> {
        let control = self
            .jobs
            .lock()
            .await
            .get(transfer_id)
            .cloned()
            .ok_or_else(|| TransferError::TransferNotFound(transfer_id.into()))?;
        control.resume().await;
        self.transition(transfer_id, TransferStatus::Transferring)
            .await
    }

    pub async fn cancel(&self, transfer_id: &str) -> Result<()> {
        if let Some(sender) = self.pending_decisions.lock().await.remove(transfer_id) {
            let _ = sender.send(ReceiveDecision {
                accept: false,
                automatic_receive: false,
                cancelled: true,
            });
        } else if let Some(control) = self.jobs.lock().await.get(transfer_id).cloned() {
            control.cancel().await;
        }
        self.transition(transfer_id, TransferStatus::Cancelled)
            .await
    }

    pub async fn set_receive_directory(&self, path: PathBuf) -> Result<TransferSnapshot> {
        ensure_receive_directory(&path)?;
        self.settings.save_receive_directory(&path).await?;
        self.config.write().await.receive_directory = path.clone();
        self.state.write().await.receive_directory = path.to_string_lossy().into_owned();
        self.emit_snapshot().await;
        Ok(self.snapshot().await)
    }

    pub async fn set_receive_policy(
        &self,
        peer_id: &str,
        automatic: bool,
    ) -> Result<TransferSnapshot> {
        let policy = if automatic {
            ReceivePolicy::Automatic
        } else {
            ReceivePolicy::AskEveryTime
        };
        if !self.is_paired_peer(peer_id, None).await? {
            return Err(TransferError::InvalidState(
                "device has not completed unified pairing".into(),
            ));
        }
        self.policies.set_policy(peer_id, policy).await?;
        if let Some(peer) = self
            .state
            .write()
            .await
            .peers
            .iter_mut()
            .find(|peer| peer.id == peer_id)
        {
            peer.automatic_receive = automatic;
        }
        self.emit_snapshot().await;
        Ok(self.snapshot().await)
    }

    pub(crate) async fn job(&self, id: &str) -> Result<Arc<JobControl>> {
        self.jobs
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| TransferError::TransferNotFound(id.into()))
    }

    pub(crate) async fn register_job(&self, id: &str) -> Arc<JobControl> {
        let control = Arc::new(JobControl::new());
        self.jobs.lock().await.insert(id.into(), control.clone());
        control
    }

    pub(crate) async fn upsert_peer(&self, mut peer: NearbyPeer) -> Result<()> {
        if peer.id == self.device_id() {
            return Ok(());
        }
        let key = decode_key(&peer.public_key)?;
        peer.paired = self.is_paired_peer(&peer.id, Some(&key)).await?;
        peer.automatic_receive =
            peer.paired && self.policies.policy(&peer.id).await? == ReceivePolicy::Automatic;
        let mut state = self.state.write().await;
        if let Some(existing) = state.peers.iter_mut().find(|value| value.id == peer.id) {
            *existing = peer;
        } else {
            state.peers.push(peer);
            state.peers.sort_by(|a, b| a.name.cmp(&b.name));
        }
        drop(state);
        self.emit_snapshot().await;
        Ok(())
    }

    pub(crate) async fn observe_incoming_peer(
        &self,
        peer_id: &str,
        peer_name: &str,
        public_key: &[u8],
        observed_ip: std::net::IpAddr,
    ) -> Result<()> {
        if peer_id == self.device_id() {
            return Ok(());
        }
        use base64::Engine as _;
        let public_key_base64 = base64::engine::general_purpose::STANDARD.encode(public_key);
        let paired = self.is_paired_peer(peer_id, Some(public_key)).await?;
        let automatic = paired && self.policies.policy(peer_id).await? == ReceivePolicy::Automatic;
        let observed_ip = observed_ip.to_string();
        let mut state = self.state.write().await;
        if let Some(existing) = state.peers.iter_mut().find(|peer| peer.id == peer_id) {
            existing.name = peer_name.to_string();
            existing.public_key = public_key_base64;
            existing.address = observed_ip.clone();
            if !existing.addresses.contains(&observed_ip) {
                existing.addresses.push(observed_ip);
            }
            existing.paired = paired;
            existing.automatic_receive = automatic;
            existing.last_seen_at_ms = now_ms();
        } else {
            state.peers.push(NearbyPeer {
                id: peer_id.to_string(),
                name: peer_name.to_string(),
                platform: "ArcRelay".into(),
                model: "LAN device".into(),
                address: observed_ip.clone(),
                addresses: vec![observed_ip],
                // An inbound connection only reveals an ephemeral source port. Keep this
                // endpoint unusable until service discovery supplies the listening port
                // and certificate pin.
                port: 0,
                public_key: public_key_base64,
                certificate_sha256: String::new(),
                paired,
                automatic_receive: automatic,
                last_seen_at_ms: now_ms(),
            });
            state.peers.sort_by(|a, b| a.name.cmp(&b.name));
        }
        drop(state);
        self.emit_snapshot().await;
        Ok(())
    }

    pub(crate) async fn remove_peer(&self, peer_id: &str) {
        let mut state = self.state.write().await;
        let has_active_transfer = state
            .transfers
            .iter()
            .any(|transfer| transfer.peer_id == peer_id && !transfer.status.terminal());
        if !has_active_transfer {
            state.peers.retain(|peer| peer.id != peer_id);
            drop(state);
            self.emit_snapshot().await;
        }
    }

    pub(crate) async fn mutate_transfer<F>(&self, id: &str, mutate: F) -> Result<()>
    where
        F: FnOnce(&mut TransferView) -> Result<()>,
    {
        {
            let mut state = self.state.write().await;
            let transfer = state
                .transfers
                .iter_mut()
                .find(|value| value.id == id)
                .ok_or_else(|| TransferError::TransferNotFound(id.into()))?;
            mutate(transfer)?;
            prune_transfer_history(&mut state.transfers);
        }
        self.schedule_history_persist().await;
        self.emit_snapshot().await;
        Ok(())
    }

    pub(crate) async fn push_transfer(&self, transfer: TransferView) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.transfers.insert(0, transfer);
            prune_transfer_history(&mut state.transfers);
        }
        self.schedule_history_persist().await;
        self.emit_snapshot().await;
        Ok(())
    }

    pub(crate) async fn record_progress_live(
        &self,
        id: &str,
        file_id: u64,
        completed: u64,
        started: std::time::Instant,
    ) -> Result<()> {
        let now = std::time::Instant::now();
        let mut times = self.progress_times.lock().await;
        let mut state = self.state.write().await;
        let revision = state.revision.saturating_add(1);
        let transfer = state
            .transfers
            .iter_mut()
            .find(|value| value.id == id)
            .ok_or_else(|| TransferError::TransferNotFound(id.into()))?;
        let finished = transfer.record_progress(file_id, completed, now_ms())?;
        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        transfer.speed_bytes_per_second = (transfer.completed_bytes as f64 / elapsed) as u64;
        transfer.remaining_seconds = (transfer.speed_bytes_per_second > 0).then(|| {
            transfer
                .total_bytes
                .saturating_sub(transfer.completed_bytes)
                / transfer.speed_bytes_per_second
        });
        let emission = times.entry(id.into()).or_default();
        emission.dirty.insert(file_id, completed);
        let should_emit = finished
            || emission.last.is_none_or(|last| {
                now.duration_since(last) >= std::time::Duration::from_millis(100)
            });
        if should_emit {
            emission.last = Some(now);
            let progress = crate::domain::TransferProgress {
                revision,
                base_revision: self.last_full_revision.load(Ordering::Acquire),
                transfer_id: id.into(),
                completed_bytes: transfer.completed_bytes,
                speed_bytes_per_second: transfer.speed_bytes_per_second,
                remaining_seconds: transfer.remaining_seconds,
                updated_at_ms: transfer.updated_at_ms,
                files: emission
                    .dirty
                    .drain()
                    .map(
                        |(id, completed_bytes)| crate::domain::TransferFileProgress {
                            id,
                            completed_bytes,
                        },
                    )
                    .collect(),
            };
            state.revision = revision;
            let _ = self.progress.send(progress);
        }
        Ok(())
    }

    pub(crate) async fn transition(&self, id: &str, status: TransferStatus) -> Result<()> {
        if status.terminal() {
            self.progress_times.lock().await.remove(id);
        }
        self.mutate_transfer(id, |transfer| transfer.transition(status, now_ms()))
            .await
    }

    pub(crate) async fn fail_transfer(&self, id: &str, message: String) {
        self.progress_times.lock().await.remove(id);
        let _ = self
            .mutate_transfer(id, |transfer| {
                if !transfer.status.terminal() {
                    transfer.transition(TransferStatus::Failed, now_ms())?;
                    transfer.error_message = Some(message);
                }
                Ok(())
            })
            .await;
    }

    pub(crate) async fn is_paired_peer(
        &self,
        peer_id: &str,
        public_key: Option<&[u8]>,
    ) -> Result<bool> {
        let id = match DeviceId::parse(peer_id) {
            Ok(id) => id,
            Err(_) => return Ok(false),
        };
        let Some(network) = self.network.get() else {
            return Ok(false);
        };
        let peers = network
            .paired_peers()
            .await
            .map_err(|error| TransferError::Network(error.to_string()))?;
        Ok(peers.into_iter().any(|peer| {
            peer.device_id == id && public_key.is_none_or(|key| peer.public_key.as_bytes() == key)
        }))
    }

    pub(crate) async fn emit_snapshot(&self) {
        let mut state = self.state.write().await;
        state.revision = state.revision.saturating_add(1);
        self.last_full_revision
            .store(state.revision, Ordering::Release);
        self.snapshots.send_replace(Arc::new(state.clone()));
    }

    async fn schedule_history_persist(&self) {
        let persistent = self
            .state
            .read()
            .await
            .transfers
            .iter()
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
        self.history_updates
            .send_replace(Some(Arc::new(persistent)));
    }
}

const MAX_TERMINAL_HISTORY: usize = 200;

fn prune_transfer_history(transfers: &mut Vec<TransferView>) {
    let mut terminal_count = 0;
    transfers.retain(|transfer| {
        if transfer.status.terminal() {
            terminal_count += 1;
            terminal_count <= MAX_TERMINAL_HISTORY
        } else {
            true
        }
    });
}

async fn persist_history_updates(
    history: Arc<dyn TransferRepository>,
    mut receiver: watch::Receiver<Option<Arc<Vec<TransferView>>>>,
    errors: broadcast::Sender<String>,
    stopping: CancellationToken,
) {
    loop {
        let stop = tokio::select! {
            _ = stopping.cancelled() => true,
            result = receiver.changed() => result.is_err(),
        };
        if !stop {
            tokio::select! {
                _ = stopping.cancelled() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {},
            }
        }
        let Some(transfers) = receiver.borrow_and_update().clone() else {
            if stop || stopping.is_cancelled() {
                break;
            }
            continue;
        };
        if let Err(error) = history.save(&transfers).await {
            let _ = errors.send(format!("failed to save transfer history: {error}"));
        }
        if stop || stopping.is_cancelled() {
            break;
        }
    }
}

fn ensure_receive_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(TransferError::Invalid(
            "receive directory must be a regular directory".into(),
        ));
    }
    Ok(())
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub(crate) fn decode_key(value: &str) -> Result<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| TransferError::Invalid("invalid peer public key".into()))
}

pub(crate) fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| TransferError::Invalid(format!("invalid file name: {}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discovered_peer(peer_id: &str, public_key: &[u8]) -> NearbyPeer {
        use base64::Engine as _;
        NearbyPeer {
            id: peer_id.into(),
            name: "Discovered Peer".into(),
            platform: "macOS".into(),
            model: "Mac".into(),
            address: "192.0.2.10".into(),
            addresses: vec!["192.0.2.10".into()],
            port: 18_765,
            public_key: base64::engine::general_purpose::STANDARD.encode(public_key),
            certificate_sha256: "pinned-certificate".into(),
            paired: false,
            automatic_receive: false,
            last_seen_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn independent_subscribers_recover_after_progress_lag_and_stop() {
        let root = tempfile::tempdir().unwrap();
        let manager = TransferManager::new(TransferConfig::new(
            root.path().join("config"),
            root.path().join("Downloads"),
            "Test".into(),
        ))
        .await
        .unwrap();
        manager
            .push_transfer(TransferView {
                id: "t".into(),
                wire_id: 1,
                peer_id: "p".into(),
                peer_name: "Peer".into(),
                direction: crate::domain::TransferDirection::Sending,
                status: TransferStatus::Transferring,
                files: vec![crate::domain::TransferFileView {
                    id: 1,
                    name: "a".into(),
                    relative_path: "a".into(),
                    local_path: None,
                    receive_directory: None,
                    size: 1024,
                    media_type: "text/plain".into(),
                    kind: crate::domain::FileKind::File,
                    thumbnail_data_url: Some("expensive-preview".into()),
                    completed_bytes: 0,
                }],
                total_bytes: 1024,
                completed_bytes: 0,
                speed_bytes_per_second: 0,
                remaining_seconds: None,
                error_message: None,
                created_at_ms: 0,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
        let mut fast = manager.events();
        let mut slow = manager.events();
        assert!(matches!(
            fast.next().await,
            Some(crate::TransferEvent::Snapshot { .. })
        ));
        assert!(matches!(
            slow.next().await,
            Some(crate::TransferEvent::Snapshot { .. })
        ));
        for bytes in 1..=300 {
            if let Some(emission) = manager.progress_times.lock().await.get_mut("t") {
                emission.last = None;
            }
            manager
                .record_progress_live("t", 1, bytes, std::time::Instant::now())
                .await
                .unwrap();
            match fast.next().await.unwrap() {
                crate::TransferEvent::Progress(delta) => assert_eq!(delta.completed_bytes, bytes),
                _ => panic!("the current subscriber must receive a contiguous delta"),
            }
        }
        match slow.next().await.unwrap() {
            crate::TransferEvent::Snapshot { snapshot, .. } => {
                assert_eq!(snapshot.transfers[0].completed_bytes, 300)
            }
            _ => panic!("a lagged subscriber needs a fresh baseline"),
        }
        manager.shutdown().await;
        assert!(fast.next().await.is_none());
        assert!(slow.next().await.is_none());
    }

    #[tokio::test]
    async fn small_progress_uses_deltas_and_terminal_state_is_persisted() {
        let root = tempfile::tempdir().unwrap();
        let manager = TransferManager::new(TransferConfig::new(
            root.path().join("config"),
            root.path().join("Downloads"),
            "Test".into(),
        ))
        .await
        .unwrap();
        manager
            .push_transfer(TransferView {
                id: "t".into(),
                wire_id: 1,
                peer_id: "p".into(),
                peer_name: "Peer".into(),
                direction: crate::domain::TransferDirection::Sending,
                status: TransferStatus::Transferring,
                files: vec![crate::domain::TransferFileView {
                    id: 1,
                    name: "a".into(),
                    relative_path: "a".into(),
                    local_path: None,
                    receive_directory: None,
                    size: 1024,
                    media_type: "text/plain".into(),
                    kind: crate::domain::FileKind::File,
                    thumbnail_data_url: Some("expensive-preview".into()),
                    completed_bytes: 0,
                }],
                total_bytes: 1024,
                completed_bytes: 0,
                speed_bytes_per_second: 0,
                remaining_seconds: None,
                error_message: None,
                created_at_ms: 0,
                updated_at_ms: 0,
            })
            .await
            .unwrap();
        let mut full = manager.subscribe();
        full.borrow_and_update();
        let baseline = manager.snapshot().await.revision;
        let mut progress = manager.subscribe_progress();
        manager
            .record_progress_live("t", 1, 512, std::time::Instant::now())
            .await
            .unwrap();
        let delta = progress.try_recv().unwrap();
        assert_eq!(delta.completed_bytes, 512);
        assert_eq!(delta.base_revision, baseline);
        assert!(!serde_json::to_string(&delta)
            .unwrap()
            .contains("expensive-preview"));
        assert!(!full.has_changed().unwrap());
        manager
            .record_progress_live("t", 1, 1024, std::time::Instant::now())
            .await
            .unwrap();
        manager
            .transition("t", TransferStatus::Completed)
            .await
            .unwrap();
        assert!(full.has_changed().unwrap());
        assert!(manager.progress_times.lock().await.is_empty());
        manager.shutdown().await;
        let history = root.path().join("config/transfer-history.json");
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let stored = tokio::fs::read(&history)
                    .await
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Vec<TransferView>>(&bytes).ok());
                if stored.is_some_and(|items| {
                    items.iter().any(|item| {
                        item.id == "t"
                            && item.status == TransferStatus::Completed
                            && item.completed_bytes == 1024
                    })
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("long-lived history worker must persist terminal state");
    }

    #[tokio::test]
    async fn missing_settings_uses_supplied_receive_directory() {
        let root = tempfile::tempdir().unwrap();
        let supplied = root.path().join("Downloads");
        let manager = TransferManager::new(TransferConfig::new(
            root.path().join("config"),
            supplied.clone(),
            "Test Device".into(),
        ))
        .await
        .unwrap();

        assert_eq!(manager.config.read().await.receive_directory, supplied);
        assert!(!root.path().join("config/transfer-settings.json").exists());
    }

    #[tokio::test]
    async fn persisted_receive_directory_overrides_next_supplied_default() {
        let root = tempfile::tempdir().unwrap();
        let config_directory = root.path().join("config");
        let selected = root.path().join("Selected");
        let manager = TransferManager::new(TransferConfig::new(
            config_directory.clone(),
            root.path().join("Downloads"),
            "Test Device".into(),
        ))
        .await
        .unwrap();
        manager
            .set_receive_directory(selected.clone())
            .await
            .unwrap();
        drop(manager);

        let restarted = TransferManager::new(TransferConfig::new(
            config_directory,
            root.path().join("Different Default"),
            "Test Device".into(),
        ))
        .await
        .unwrap();

        assert_eq!(restarted.config.read().await.receive_directory, selected);
    }

    #[tokio::test]
    async fn incoming_connection_does_not_downgrade_discovered_endpoint() {
        let root = tempfile::tempdir().unwrap();
        let manager = TransferManager::new(TransferConfig::new(
            root.path().join("config"),
            root.path().join("Downloads"),
            "Test Device".into(),
        ))
        .await
        .unwrap();
        let peer_id = "paired-peer";
        let public_key = [7_u8; 32];
        manager
            .upsert_peer(discovered_peer(peer_id, &public_key))
            .await
            .unwrap();

        manager
            .observe_incoming_peer(
                peer_id,
                "Incoming Peer",
                &public_key,
                "192.0.2.11".parse().unwrap(),
            )
            .await
            .unwrap();

        let peer = manager
            .snapshot()
            .await
            .peers
            .into_iter()
            .find(|peer| peer.id == peer_id)
            .unwrap();
        assert_eq!(peer.port, 18_765);
        assert_eq!(peer.certificate_sha256, "pinned-certificate");
        assert_eq!(peer.platform, "macOS");
        assert_eq!(peer.model, "Mac");
        assert!(!peer.paired);
        assert!(!peer.automatic_receive);
        assert!(peer.addresses.contains(&"192.0.2.10".to_string()));
        assert!(peer.addresses.contains(&"192.0.2.11".to_string()));
    }
}
