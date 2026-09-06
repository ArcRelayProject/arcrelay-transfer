use crate::{TransferEvent, TransferManager, TransferProgress, TransferSnapshot};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

/// Each consumer owns a cancellable cursor. Slow consumers recover from current
/// state instead of applying deltas whose baseline was dropped.
pub struct TransferSubscription {
    manager: Arc<TransferManager>,
    snapshots: watch::Receiver<Arc<TransferSnapshot>>,
    progress: broadcast::Receiver<TransferProgress>,
    errors: broadcast::Receiver<String>,
    initialized: bool,
    revision: u64,
    base_revision: u64,
}

impl TransferSubscription {
    pub(crate) fn new(manager: Arc<TransferManager>) -> Self {
        Self {
            snapshots: manager.subscribe(),
            progress: manager.subscribe_progress(),
            errors: manager.subscribe_errors(),
            manager,
            initialized: false,
            revision: 0,
            base_revision: 0,
        }
    }

    async fn baseline(&mut self) -> TransferEvent {
        self.snapshots.borrow_and_update();
        let (snapshot, base_revision) = self.manager.event_baseline().await;
        self.initialized = true;
        self.revision = snapshot.revision;
        self.base_revision = base_revision;
        TransferEvent::Snapshot {
            snapshot,
            base_revision,
        }
    }

    /// Cancelling this future retains the receivers and their positions.
    pub async fn next(&mut self) -> Option<TransferEvent> {
        if self.manager.stopping.is_cancelled() {
            return None;
        }
        if !self.initialized {
            return Some(self.baseline().await);
        }
        loop {
            tokio::select! {
                biased;
                _ = self.manager.stopping.cancelled() => return None,
                changed = self.snapshots.changed() => {
                    if changed.is_err() { return None; }
                    return Some(self.baseline().await);
                }
                progress = self.progress.recv() => match progress {
                    Ok(progress) if progress.revision <= self.revision => continue,
                    Ok(progress) if progress.base_revision == self.base_revision && progress.revision == self.revision + 1 => {
                        self.revision = progress.revision;
                        return Some(TransferEvent::Progress(progress));
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => return Some(self.baseline().await),
                    Err(broadcast::error::RecvError::Closed) => return None,
                },
                error = self.errors.recv() => match error {
                    Ok(message) => return Some(TransferEvent::Error { message }),
                    Err(broadcast::error::RecvError::Lagged(count)) => return Some(TransferEvent::Error { message: format!("{count} transfer diagnostics were dropped") }),
                    Err(broadcast::error::RecvError::Closed) => return None,
                },
            }
        }
    }
}
