//! One accept loop per authenticated session, including locally dialed sessions.
//! Offers and files use feature headers; only the owning task may stop its streams.
use super::*;
use arcrelay_network::FeatureStream;
use std::sync::{Mutex as SyncMutex, Weak};
use tokio::sync::mpsc;

pub(crate) struct IncomingFile {
    pub stream: FeatureStream,
    pub open: proto::FileStreamOpen,
}

pub(crate) struct SessionRouter {
    pub session: Arc<Session>,
    routes: SyncMutex<HashMap<u64, mpsc::Sender<IncomingFile>>>,
}

pub(super) struct FileRoute {
    router: Arc<SessionRouter>,
    id: u64,
    pub incoming: mpsc::Receiver<IncomingFile>,
}

impl Drop for FileRoute {
    fn drop(&mut self) {
        self.router
            .routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

impl SessionRouter {
    pub(super) fn register(self: &Arc<Self>, id: u64) -> Result<FileRoute> {
        let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        if routes.contains_key(&id) {
            return Err(TransferError::Invalid(
                "duplicate active transfer id".into(),
            ));
        }
        let (sender, incoming) = mpsc::channel(FILE_STREAM_CONCURRENCY);
        routes.insert(id, sender);
        Ok(FileRoute {
            router: self.clone(),
            id,
            incoming,
        })
    }

    async fn run(self: Arc<Self>, manager: Weak<TransferManager>) {
        let connection = self.session.transport_handle();
        loop {
            let Some(owner) = manager.upgrade() else {
                break;
            };
            let stream = tokio::select! {
                _ = owner.stopping.cancelled() => break,
                _ = connection.closed() => break,
                stream = self.session.accept_feature_stream() => match stream {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::debug!(%error, "transfer stream header rejected");
                        continue;
                    }
                },
            };
            if stream.feature_id != "arcrelay.transfer" || stream.negotiate_minor(1, 0, 0).is_err()
            {
                reject(stream, FAILED_STREAM_CODE);
                continue;
            }
            match stream.operation.as_str() {
                "offer" if stream.opening_payload.is_empty() => {
                    let Ok(slot) = owner.job_slots.clone().try_acquire_owned() else {
                        reject(stream, FAILED_STREAM_CODE);
                        continue;
                    };
                    let router = self.clone();
                    let tasks = owner.tasks.clone();
                    tasks.spawn(async move {
                        let _slot = slot;
                        if let Err(error) = handle_incoming_offer(owner, router, stream).await {
                            tracing::debug!(%error, "incoming transfer ended");
                        }
                    });
                }
                "file" => {
                    let Ok(open) = proto::FileStreamOpen::decode(stream.opening_payload.clone())
                    else {
                        reject(stream, FAILED_STREAM_CODE);
                        continue;
                    };
                    let sender = self
                        .routes
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get(&open.transfer_id)
                        .cloned();
                    let incoming = IncomingFile { stream, open };
                    if let Some(sender) = sender {
                        if let Err(error) = sender.try_send(incoming) {
                            reject(error.into_inner().stream, FAILED_STREAM_CODE);
                        }
                    } else {
                        reject(incoming.stream, FAILED_STREAM_CODE);
                    }
                }
                _ => reject(stream, FAILED_STREAM_CODE),
            }
        }
        if let Some(owner) = manager.upgrade() {
            owner.routers.lock().await.remove(&connection.stable_id());
        }
    }
}

fn reject(mut stream: FeatureStream, code: u32) {
    let _ = stream.send.reset(code.into());
    let _ = stream.receive.stop(code.into());
}

pub(super) async fn ensure_router(
    manager: &Arc<TransferManager>,
    session: Arc<Session>,
) -> Arc<SessionRouter> {
    let mut routers = manager.routers.lock().await;
    let key = session.transport_handle().stable_id();
    if let Some(router) = routers.get(&key).and_then(Weak::upgrade) {
        return router;
    }
    let router = Arc::new(SessionRouter {
        session,
        routes: SyncMutex::new(HashMap::new()),
    });
    routers.insert(key, Arc::downgrade(&router));
    manager
        .tasks
        .spawn(router.clone().run(Arc::downgrade(manager)));
    router
}
