// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Block "attachments" provide the plumbing between emulated devices and the
//! backends which execute the IO requests from said devices.
//!
//! Each emulated block device will contain a [DeviceAttachment] to which it
//! will associate one or more [DeviceQueue] instances.  The queue(s) is the
//! source of [super::Request]s, which are to be processed by an attached
//! backend.
//!
//! Block backends will each contain a [BackendAttachment] which they will
//! request worker contexts from ([SyncWorkerCtx] or [AsyncWorkerCtx]).  It is
//! through the worker context that the backend will fetch [super::Request]s
//! from the associated device in order to process them.

use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomPinned;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

use super::minder::{NoneInFlight, QueueMinder};
use super::{
    devq_id, probes, DeviceId, DeviceInfo, DeviceQueue, DeviceRequest,
    MetricConsumer, QueueId, WorkerId,
};
use crate::accessors::MemAccessor;

use futures::stream::FuturesUnordered;
use futures::Stream;
use pin_project_lite::pin_project;
use strum::IntoStaticStr;
use thiserror::Error;
use tokio::sync::futures::Notified;
use tokio::sync::Notify;

/// Static for generating unique block [DeviceId]s with a process
static NEXT_DEVICE_ID: AtomicU32 = AtomicU32::new(0);

pub const MAX_WORKERS: NonZeroUsize = NonZeroUsize::new(64).unwrap();

pub type ReqCountHint = Option<NonZeroUsize>;

struct MinderRefs {
    values: Vec<Arc<QueueMinder>>,
    _pinned: PhantomPinned,
}
pin_project! {
    pub struct NoneProcessing {
        #[pin]
        minders: MinderRefs,
        #[pin]
        unordered: FuturesUnordered<NoneInFlight<'static>>,
        loaded: bool,
    }
    impl PinnedDrop for NoneProcessing {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();

            // Ensure that all references into `minders` held by NoneInFlight
            // futures are dropped before the `minders` contents themselves.
            this.unordered.clear();
        }
    }
}
impl Future for NoneProcessing {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !*this.loaded {
            for minder in this.minders.values.iter().map(Arc::as_ref) {
                // # SAFETY
                //
                // With the Vec<Arc<QueueMinder>> pinned (and barred via marker
                // from Unpin), it should not be possible to remove them for the
                // lifetime of this future.  With that promised to us, we can
                // extend the lifetime of the QueueMinder references long enough
                // to run the NoneInFlight futures.
                //
                // The contents of `minders` will remain pinned and untouched
                // until NoneProcessing is dropped.  At that point, any
                // lingering references held by the FuturesUnordered will be
                // explicitly released in PinnedDrop::drop(), ensuring they do
                // not outlive MinderRefs.
                let extended: &'static QueueMinder =
                    unsafe { std::mem::transmute(minder) };

                this.unordered.push(extended.none_in_flight());
            }
            *this.loaded = true;
        }
        loop {
            match Stream::poll_next(this.unordered.as_mut(), cx) {
                Poll::Ready(None) => {
                    return Poll::Ready(());
                }
                Poll::Ready(Some(_)) => {
                    continue;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

pub type OnAttachFn = Box<dyn Fn(DeviceInfo) + Send + Sync + 'static>;

#[derive(Default)]
struct DeviceState {
    on_attach: Option<OnAttachFn>,
    queues: BTreeMap<QueueId, Arc<QueueMinder>>,
    paused: bool,
}

struct DeviceAttachInner {
    dev_state: Mutex<DeviceState>,
    acc_mem: MemAccessor,
    device_id: DeviceId,
    max_queues: NonZeroUsize,
    workers: Arc<WorkerCollection>,
    backend: Arc<dyn super::Backend>,
    notify_tx: std::sync::mpsc::Sender<DeviceAttachmentNotify>,
}

enum DeviceAttachmentNotify {
    NewRequests { queue_id: QueueId, hint: ReqCountHint },

    Pause,

    Resume,
}

fn device_notify_worker(
    inner: DeviceAttachment,
    notify_rx: std::sync::mpsc::Receiver<DeviceAttachmentNotify>,
) {
    let mut paused = false;

    while let Ok(notification) = notify_rx.recv() {
        match notification {
            DeviceAttachmentNotify::NewRequests { queue_id, hint } => {
                if paused {
                    // Don't check for new requests?
                } else {
                    inner.notify_impl(queue_id, hint);
                }
            }

            DeviceAttachmentNotify::Pause => {
                paused = true;
            }

            DeviceAttachmentNotify::Resume => {
                paused = false;
            }
        }
    }
}

/// Main "attachment point" for a block device.
#[derive(Clone)]
pub struct DeviceAttachment(Arc<DeviceAttachInner>);

impl DeviceAttachment {
    /// Create a [DeviceAttachment] for a given device. The maximum number of
    /// queues which the device will ever expose is set via `max_queues`.  DMA
    /// done by attached backend workers will be through the provided `acc_mem`.
    pub fn new(
        max_queues: NonZeroUsize,
        backend: Arc<dyn super::Backend>,
        acc_mem: MemAccessor,
    ) -> Self {
        let (notify_tx, notify_rx) = std::sync::mpsc::channel(); // XXX max queues bound
        let max_workers = backend.worker_count();

        let inner = Arc::new(DeviceAttachInner {
            dev_state: Mutex::new(DeviceState::default()),
            acc_mem,
            device_id: NEXT_DEVICE_ID.fetch_add(1, Ordering::Relaxed),
            max_queues,
            workers: WorkerCollection::new(max_workers),
            backend,
            notify_tx,
        });

        let device_id = inner.device_id;
        let myself = Self(inner);

        let also_myself = myself.clone();

        // XXX get stuff off main thread, minimize sq interrupt time
        std::thread::Builder::new()
            .name(format!("device {device_id} notify"))
            .spawn(move || {
                device_notify_worker(also_myself, notify_rx);
            });

        myself
    }

    /// Associate a [DeviceQueue] with this device.
    ///
    /// Once associated, any attached backend will process requests emitted from
    /// that queue.
    ///
    /// # Panics
    ///
    /// If `queue_id` is >= the max queues specified for this device, or if
    /// an existing queue is associated with that ID.
    pub fn queue_associate(
        &self,
        queue_id: QueueId,
        queue: Arc<impl DeviceQueue>,
    ) {
        let minder = QueueMinder::new(queue, self.0.device_id, queue_id);
        let mut state = self.0.dev_state.lock().unwrap();

        let old = state.queues.insert(queue_id, minder);

        assert!(
            old.is_none(),
            "queue slot should not have been occupied"
        );
    }

    /// Dissociate a [DeviceQueue] from this device
    ///
    /// After dissociation, any attached backend will cease processing requests
    /// from that queue.
    ///
    /// # Panics
    ///
    /// if `queue_id` is >= the max queues specified for this device, or if
    /// there is not queue associated with that ID.
    pub fn queue_dissociate(&self, queue_id: QueueId) {
        let mut state = self.0.dev_state.lock().unwrap();

        let minder =
            state.queues.remove(&queue_id).expect("queue slot should be occupied");

        // XXX minder.abandon()
    }

    /// Notify attached backend (if any) that `queue_id` may have new IO
    /// requests to process.  If the number of available requests is known, it
    /// can be communicated via `hint` in order to optimize worker waking.
    pub fn notify(&self, queue_id: QueueId, hint: ReqCountHint) {
        self.0.notify_tx.send(DeviceAttachmentNotify::NewRequests { queue_id, hint }).unwrap();
    }

    fn notify_impl(&self, queue_id: QueueId, hint: ReqCountHint) {
        let mut state = self.0.dev_state.lock().unwrap();

        let Some(minder) = state.queues.get(&queue_id) else {
            // XXX queue disassociated?
            return;
        };

        while let Some(req) = minder.next_req() {
            // XXX send it!
        }
    }

    pub fn device_id(&self) -> DeviceId {
        self.0.device_id
    }

    /// Get the maximum queues configured for this device.
    pub fn max_queues(&self) -> NonZeroUsize {
        self.0.max_queues
    }

    pub fn info(&self) -> DeviceInfo {
        self.0.backend.info()
    }

    // XXX move these into impl Lifecycle?
    pub async fn start(&self) -> anyhow::Result<()> {
        self.0.backend.start(&self.0.workers).await
    }

    /// Pause the device, preventing workers from an attached backend (if any)
    /// from fetching new IO requests to process.  Outstanding requests will
    /// proceed as normal.
    pub fn pause(&self) {
        self.0.notify_tx.send(DeviceAttachmentNotify::Pause).unwrap();
    }

    /// Resume the device, allowing workers from an attached backend (if any) to
    /// once again fetch new IO requests to process.
    pub fn resume(&self) {
        self.0.notify_tx.send(DeviceAttachmentNotify::Resume).unwrap();
    }

    pub fn halt(&self) {
        // XXX specific order: stop threads, wait for threads to join
        self.0.workers.stop();
        self.0.backend.stop();
    }

    /// Emit a [Future] which will resolve when there are no request being
    /// actively processed by an attached backend.
    pub fn none_processing(&self) -> NoneProcessing {
        let minders = self
            .0
            .dev_state
            .lock()
            .unwrap()
            .queues
            .iter()
            .map(|(_, minder)| {
                minder.clone()
            })
            .collect::<Vec<_>>();

        NoneProcessing {
            minders: MinderRefs { values: minders, _pinned: PhantomPinned },
            unordered: FuturesUnordered::new(),
            loaded: false,
        }
    }

    /// Set the [MetricConsumer] to be informed of all request completions
    /// processed by this device.
    pub fn set_metric_consumer(&self, consumer: Arc<dyn MetricConsumer>) {
        let state = self.0.dev_state.lock().unwrap();
        for (_, minder) in &state.queues {
            minder.set_metric_consumer(consumer.clone());
        }
    }
}

impl Drop for DeviceAttachment {
    fn drop(&mut self) {
        // XXX what to do? stop?
        //self.detach();
    }
}

enum WorkerMessage {
    Request { req: DeviceRequest },

    Stop,
}

#[derive(Default)]
struct WorkerState {
    /// Has the worker associated with this slot indicated that it is active?
    active_type: Option<WorkerType>,
}

pub(crate) struct WorkerSlot {
    state: Mutex<WorkerState>,
    acc_mem: MemAccessor,
    id: WorkerId,
}

impl WorkerSlot {
    fn new(id: WorkerId) -> Self {
        Self {
            state: Mutex::new(Default::default()),
            acc_mem: MemAccessor::new_orphan(),
            id,
        }
    }

    /// Called by Sync workers to block for a request
    fn block_for_req(&self) -> Option<DeviceRequest> {
        let mut state = self.state.lock().unwrap();
        assert!(state.active_type.is_some());

        let Some(WorkerType::Sync { rx, .. }) = &state.active_type else {
            panic!("wrong worker type!");
        };

        match rx.recv() {
            Ok(message) => match message {
                WorkerMessage::Request { req } => Some(req),
                WorkerMessage::Stop => None,
            },

            Err(_) => {
                // no more messages and channel disconnected
                None
            }
        }
    }

    /// Called by asynchronous workers to wait for a request
    async fn next_req(
        &self,
    ) -> Option<DeviceRequest> {
        let rx = {
            let state = self.state.lock().unwrap();
            assert!(state.active_type.is_some());

            let Some(WorkerType::Async { rx, .. }) = &state.active_type else {
                panic!("wrong worker type!");
            };

            rx.clone()
        };

        match rx.recv().await {
            Ok(message) => match message {
                WorkerMessage::Request { req } => Some(req),
                WorkerMessage::Stop => None,
            },

            Err(_) => {
                // no more messages and channel disconnected
                None
            }
        }
    }

    // XXX halt processing once they have completed any in-flight work.
    /// Causes all worker threads to stop their loops and terminate
    fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        let Some(active_type) = &state.active_type else {
            panic!("halting non-active worker!");
        };
        match active_type {
            WorkerType::Sync { tx, .. } => {
                if let Err(_) = tx.send(WorkerMessage::Stop) {
                    // channel is already closed - is the worker halted already,
                    // it's the only one with rx?
                }
            }

            WorkerType::Async { tx, .. } => {
                if let Err(_) = tx.send_blocking(WorkerMessage::Stop) {
                    // channel is already closed - is the worker halted already,
                    // it's the only one with rx?
                }
            }
        }
    }
}

pub(crate) struct WorkerCollection {
    workers: Vec<WorkerSlot>,

    sync_tx: crossbeam::channel::Sender<WorkerMessage>,
    sync_rx: crossbeam::channel::Receiver<WorkerMessage>,

    async_tx: async_channel::Sender<WorkerMessage>,
    async_rx: async_channel::Receiver<WorkerMessage>,
}

impl WorkerCollection {
    fn new(max_workers: NonZeroUsize) -> Arc<Self> {
        let max_workers = max_workers.get();
        assert!(max_workers <= MAX_WORKERS.get());
        let workers: Vec<_> = (0..max_workers)
            .map(|id| WorkerSlot::new(WorkerId::from(id)))
            .collect();

        let channel_depth = 65536 * 64; // XXX max queue depth * max queues
        let (sync_tx, sync_rx) = crossbeam::channel::bounded(channel_depth);
        let (async_tx, async_rx) = async_channel::bounded(channel_depth);

        Arc::new(Self {
            workers,
            sync_tx,
            sync_rx,
            async_tx,
            async_rx,
        })
    }

    fn set_active_sync(&self, id: WorkerId, active: bool) -> bool {
        self.set_active(
            id,
            if active {
                Some(WorkerType::Sync { tx: self.sync_tx.clone(), rx: self.sync_rx.clone() })
            } else {
                None
            }
        )
    }

    fn set_active_async(&self, id: WorkerId, active: bool) -> bool {
        self.set_active(
            id,
            if active {
                Some(WorkerType::Async { tx: self.async_tx.clone(), rx: self.async_rx.clone() })
            } else {
                None
            }
        )
    }

    /// Returns true if the worker state changed
    fn set_active(&self, id: WorkerId, new_type: Option<WorkerType>) -> bool {
        if let Some(slot) = self.workers.get(id) {
            let mut wstate = slot.state.lock().unwrap();

            match (wstate.active_type.is_some(), new_type.is_some()) {
                (true, true) => {
                    // already active, do not replace
                    // XXX why? panic?
                    false
                }

                (false, true) => {
                    // activating
                    wstate.active_type = new_type;
                    true
                }

                (true, false) => {
                    // deactivating
                    wstate.active_type = new_type;
                    true
                }

                (false, false) => {
                    // already inactive, do not replace
                    false
                }
            }
        } else {
            false
        }
    }

    fn slot(&self, id: WorkerId) -> &WorkerSlot {
        self.workers.get(id).expect("valid worker id for slot")
    }

    pub fn inactive_worker(self: &Arc<Self>, id: WorkerId) -> InactiveWorkerCtx {
        assert!(id < self.workers.len());
        InactiveWorkerCtx { workers: self.clone(), id }
    }

    /// Causes all workers to stop their threads
    fn stop(&self) {
        for slot in self.workers.iter() {
            slot.stop();
        }
    }
}

#[derive(Clone)]
pub enum WorkerType {
    Sync {
        tx: crossbeam::channel::Sender<WorkerMessage>,
        rx: crossbeam::channel::Receiver<WorkerMessage>,
    },

    Async {
        tx: async_channel::Sender<WorkerMessage>,
        rx: async_channel::Receiver<WorkerMessage>,
    },
}

pub struct InactiveWorkerCtx {
    workers: Arc<WorkerCollection>,
    id: WorkerId,
}

impl InactiveWorkerCtx {
    /// Activate this worker for synchronous operation.
    ///
    /// Returns [None] if there is already an active worker in the slot
    /// associated with this [WorkerId].
    pub fn activate_sync(self) -> Option<SyncWorkerCtx> {
        if self.workers.set_active_sync(self.id, true) {
            Some(SyncWorkerCtx(self.into()))
        } else {
            None
        }
    }

    /// Activate this worker for asynchronous operation.
    ///
    /// Returns [None] if there is already an active worker in the slot
    /// associated with this [WorkerId].
    pub fn activate_async(self) -> Option<AsyncWorkerCtx> {
        if self.workers.set_active_async(self.id, true)
        {
            Some(AsyncWorkerCtx(self.into()))
        } else {
            None
        }
    }
}

/// Worker context for synchronous (blocking) request processing.
///
/// Note: When the context is dropped, the slot for this [WorkerId] will become
/// vacant, and available to be activated again.
pub struct SyncWorkerCtx(WorkerCtxInner);
impl SyncWorkerCtx {
    /// Block (synchronously) in order to retrieve the next
    /// [request](DeviceRequest) from the device.  Will return [None] if no
    /// device is attached, or the backend is stopped, otherwise it will block
    /// until a request is available.
    pub fn block_for_req(&self) -> Option<DeviceRequest> {
        self.0.workers.slot(self.0.id).block_for_req()
    }

    /// Get the [MemAccessor] required to do DMA for request processing
    pub fn acc_mem(&self) -> &MemAccessor {
        self.0.acc_mem()
    }
}

/// Worker context for asynchronous request processing
///
/// Note: When the context is dropped, the slot for this [WorkerId] will become
/// vacant, and available to be activated again.
pub struct AsyncWorkerCtx(WorkerCtxInner);
impl AsyncWorkerCtx {
    /// Get a [Future] which will wait for a [request](DeviceRequest) to be made
    /// available from an attached device.
    pub async fn next_req(&self) -> Option<DeviceRequest> {
        self.0.workers.slot(self.0.id).next_req().await
    }

    /// Get the [MemAccessor] required to do DMA for request processing
    pub fn acc_mem(&self) -> &MemAccessor {
        self.0.acc_mem()
    }
}

struct WorkerCtxInner {
    workers: Arc<WorkerCollection>,
    id: WorkerId,
}
impl From<InactiveWorkerCtx> for WorkerCtxInner {
    fn from(value: InactiveWorkerCtx) -> Self {
        let InactiveWorkerCtx { workers, id } = value;
        WorkerCtxInner { workers, id }
    }
}
impl WorkerCtxInner {
    fn acc_mem(&self) -> &MemAccessor {
        &self.workers.slot(self.id).acc_mem
    }
}
impl Drop for WorkerCtxInner {
    /// Deactivate the worker when it is dropped
    fn drop(&mut self) {
        assert!(
            self.workers.set_active(self.id, None),
            "active worker is valid during deactivation"
        );
    }
}
