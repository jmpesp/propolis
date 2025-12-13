// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms required to implement a block device

use std::any::Any;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use pin_project_lite::pin_project;
use tokio::sync::futures::Notified;
use tokio::sync::Notify;

use crate::block::{self, devq_id, probes, Operation, Request};
use crate::block::{DeviceId, MetricConsumer, QueueId};

/// Each emulated block device will have one or more [DeviceQueue]s which can be
/// polled through [next_req()](DeviceQueue::next_req()) to emit IO requests.
/// The completions for those requests are then processed through
/// [complete()](DeviceQueue::complete()) calls.
pub trait DeviceQueue: Send + Sync + 'static {
    /// Requests emitted from a [DeviceQueue] may require some associated state
    /// in order to communicate their completion to the guest.  The `Token` type
    /// represents that state.
    type Token: Send + Sync + 'static;

    /// Get the next [Request] (if any) from this queue.  Supporting data
    /// included with the request consists of the necessary [Self::Token] as
    /// well an optional [queued-time](Instant).
    fn next_reqs(&self) -> Vec<(Request, Self::Token, Option<Instant>)>;

    /// Emit a completion for a processed request, identified by its
    /// [token](Self::Token).
    fn complete(
        &self,
        op: block::Operation,
        result: block::Result,
        token: Self::Token,
    );

    fn flush_notifications(&self);
}

/// A wrapper for an IO [Request] bearing necessary tracking information to
/// issue its completion back to the [queue](DeviceQueue) from which it came.
///
/// A panic will occur a `DeviceRequest` instance is dropped without calling
/// [complete()](DeviceRequest::complete()).
pub struct DeviceRequest<DQ: DeviceQueue> {
    req: Request,
    id: ReqId,
    source: Weak<QueueMinder<DQ>>,
    _nodrop: NoDropDevReq,
}
impl<DQ: DeviceQueue> DeviceRequest<DQ> {
    fn new(id: ReqId, req: Request, source: Weak<QueueMinder<DQ>>) -> Self {
        Self { req, id, source, _nodrop: NoDropDevReq }
    }

    /// Get the underlying block [Request]
    pub fn req(&self) -> &Request {
        &self.req
    }

    /// Issue a completion for this [Request].
    pub fn complete(self, result: super::Result) {
        let DeviceRequest { id, source, _nodrop, .. } = self;
        std::mem::forget(_nodrop);

        if let Some(src) = source.upgrade() {
            src.complete(id, result);
        }
    }
}

/// Marker struct to ensure that [DeviceRequest] consumers call
/// [complete()](DeviceRequest::complete()), rather than silently dropping it.
struct NoDropDevReq;
impl Drop for NoDropDevReq {
    fn drop(&mut self) {
        panic!("DeviceRequest should be complete()-ed before drop");
    }
}

struct QmEntry<T: Send + Sync + 'static> {
    token: T,
    op: Operation,
    when_queued: Instant,
    when_started: Instant,
}

struct QmInner<T: Send + Sync + 'static> {
    next_id: ReqId,
    in_flight: BTreeMap<ReqId, QmEntry<T>>,
    metric_consumer: Option<Arc<dyn MetricConsumer>>,
    /// Number of [Request] completions which are currently being processed by
    /// the device.  This is tracked only for requests which are the last entry
    /// removed from `in_flight`, as a means providing accurate results from
    /// [NoneInFlight].
    processing_last: usize,
}

impl<T: Send + Sync + 'static> Default for QmInner<T> {
    fn default() -> Self {
        Self {
            next_id: ReqId::START,
            processing_last: 0,
            in_flight: BTreeMap::new(),
            metric_consumer: None,
        }
    }
}

pub(super) struct QueueMinder<DQ: DeviceQueue> {
    queue: Arc<DQ>,
    pub queue_id: QueueId,
    pub device_id: DeviceId,
    state: Mutex<QmInner<DQ::Token>>,
    self_ref: Weak<Self>,
    notify: Notify,
}

impl<DQ: DeviceQueue> QueueMinder<DQ> {
    pub fn new(
        queue: Arc<DQ>,
        device_id: DeviceId,
        queue_id: QueueId,
    ) -> Arc<Self> { // XXX needs to be Arc for NoneProcessing?
        Arc::new_cyclic(|self_ref| Self {
            queue,
            queue_id,
            device_id,
            state: Mutex::new(QmInner::default()),
            self_ref: self_ref.clone(),
            notify: Notify::new(),
            //next_req_fn,
            //complete_req_fn,
        })
    }

    /// Attempt to fetch the next IO request from this queue for a worker.
    pub fn next_reqs(&self) -> Vec<DeviceRequest<DQ>> {
        let mut dreqs = Vec::with_capacity(65536);
        let reqs = self.queue.next_reqs();

        let mut state = self.state.lock().unwrap();
        for (req, token, when_queued) in reqs {
            let id = state.next_id;
            state.next_id.advance();

            let devqid = devq_id(self.device_id, self.queue_id);
            match req.op {
                Operation::Read(off, len) => {
                    probes::block_begin_read!(|| {
                        (devqid, id, off as u64, len as u64)
                    });
                }
                Operation::Write(off, len) => {
                    probes::block_begin_write!(|| {
                        (devqid, id, off as u64, len as u64)
                    });
                }
                Operation::Flush => {
                    probes::block_begin_flush!(|| { (devqid, id) });
                }
                Operation::Discard(off, len) => {
                    probes::block_begin_discard!(|| {
                        (devqid, id, off as u64, len as u64)
                    });
                }
            }
            let when_started = Instant::now();
            let old = state.in_flight.insert(
                id,
                QmEntry {
                    token,
                    op: req.op,
                    when_queued: when_queued.unwrap_or(when_started),
                    when_started,
                },
            );
            assert!(old.is_none(), "request IDs should not overlap");

            dreqs.push(DeviceRequest::new(id, req, self.self_ref.clone()))
        }
        dreqs
    }

    /// Process a completion for an in-flight IO request on this queue.
    pub fn complete(&self, id: ReqId, result: block::Result) {
        let mut state = self.state.lock().unwrap();
        let ent =
            state.in_flight.remove(&id).expect("state for request not lost");
        let metric_consumer = state.metric_consumer.as_ref().map(Arc::clone);
        let is_last_req = state.in_flight.is_empty();
        if is_last_req {
            state.processing_last += 1;
        }
        drop(state);

        let when_done = Instant::now();
        let time_queued = ent.when_started.duration_since(ent.when_queued);
        let time_processed = when_done.duration_since(ent.when_started);

        let ns_queued = time_queued.as_nanos() as u64;
        let ns_processed = time_processed.as_nanos() as u64;
        let rescode = result as u8;
        let devqid = devq_id(self.device_id, self.queue_id);
        match ent.op {
            Operation::Read(..) => {
                probes::block_complete_read!(|| {
                    (devqid, id, rescode, ns_processed, ns_queued)
                });
            }
            Operation::Write(..) => {
                probes::block_complete_write!(|| {
                    (devqid, id, rescode, ns_processed, ns_queued)
                });
            }
            Operation::Flush => {
                probes::block_complete_flush!(|| {
                    (devqid, id, rescode, ns_processed, ns_queued)
                });
            }
            Operation::Discard(..) => {
                probes::block_complete_discard!(|| {
                    (devqid, id, rescode, ns_processed, ns_queued)
                });
            }
        }

        self.queue.complete(ent.op, result, ent.token);

        probes::block_completion_sent!(|| {
            (devqid, id, when_done.elapsed().as_nanos() as u64)
        });

        // Report the completion to the metrics consumer, if one exists
        if let Some(consumer) = metric_consumer {
            consumer.request_completed(
                self.queue_id,
                ent.op,
                result,
                time_queued,
                time_processed,
            );
        }

        self.queue.flush_notifications();

        // We must track how many completions are being processed by the device,
        // since they are done outside the state lock, in order to present a
        // reliably accurate accurate signal of when the device has no more
        // in-flight requests.
        if is_last_req {
            let mut state = self.state.lock().unwrap();
            state.processing_last -= 1;
            if state.in_flight.is_empty() && state.processing_last == 0 {
                self.notify.notify_waiters();
            }
        }
    }

    /// Associate a [MetricConsumer] with this queue.
    ///
    /// It will be notified about each IO completion as they occur.
    pub(crate) fn set_metric_consumer(
        &self,
        consumer: Arc<dyn MetricConsumer>,
    ) {
        self.state.lock().unwrap().metric_consumer = Some(consumer);
    }

    pub(crate) fn none_in_flight(&self) -> NoneInFlight<'_, DQ> {
        NoneInFlight { minder: self, wait: self.notify.notified() }
    }
}

pin_project! {
    /// A [Future] which resolves to [Ready](Poll::Ready) when there are no
    /// requests being processed by an attached backend.
    pub(crate) struct NoneInFlight<'a, DQ: DeviceQueue> {
        minder: &'a QueueMinder<DQ>,
        #[pin]
        wait: Notified<'a>
    }
}
impl<DQ: DeviceQueue> Future for NoneInFlight<'_, DQ> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            let state = this.minder.state.lock().unwrap();
            if state.in_flight.is_empty() && state.processing_last == 0 {
                return Poll::Ready(());
            }
            // Keep the minder `state` lock held while polling the Notified
            // instance.  While it may not be strictly necessary, it matches the
            // conventions we expect from similar sync primitives such as CVs.
            if let Poll::Ready(_) = Notified::poll(this.wait.as_mut(), cx) {
                // Refresh fused future from Notify
                this.wait.set(this.minder.notify.notified());
            } else {
                return Poll::Pending;
            }
        }
    }
}

/// Unique ID assigned to a given block [Request].
#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct ReqId(u64);
impl ReqId {
    const START: Self = ReqId(0);

    fn advance(&mut self) {
        self.0 += 1;
    }
}
impl Borrow<u64> for ReqId {
    fn borrow(&self) -> &u64 {
        &self.0
    }
}
impl From<ReqId> for u64 {
    fn from(value: ReqId) -> Self {
        value.0
    }
}
