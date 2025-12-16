// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use super::{cmds::NvmCmd, queue::Permit, PciNvme};
use crate::accessors::MemAccessor;
use crate::block::{self, Operation, Request};
use crate::block::minder::DeviceQueueNextReqs;
use crate::hw::nvme::{bits, cmds::Completion, queue::SubQueue, CompQueue};
use crate::hw::nvme::queue::SubQueuePop;

#[usdt::provider(provider = "propolis")]
mod probes {
    fn nvme_read_enqueue(qid: u16, idx: u16, cid: u16, off: u64, sz: u64) {}
    fn nvme_read_complete(qid: u16, cid: u16, res: u8) {}

    fn nvme_write_enqueue(qid: u16, idx: u16, cid: u16, off: u64, sz: u64) {}
    fn nvme_write_complete(qid: u16, cid: u16, res: u8) {}

    fn nvme_flush_enqueue(qid: u16, idx: u16, cid: u16) {}
    fn nvme_flush_complete(qid: u16, cid: u16, res: u8) {}

    fn nvme_raw_cmd(
        qid: u16,
        cdw0nsid: u64,
        prp1: u64,
        prp2: u64,
        cdw10cdw11: u64,
    ) {
    }
}

impl block::Device<NvmeBlockQueue> for PciNvme {
    fn attachment(&self) -> &block::DeviceAttachment<NvmeBlockQueue> {
        &self.block_attach
    }
}

pub struct NvmeBlockQueueNotificationState {
    reqs: usize,
    last: Instant,
    cqs_to_notify: HashMap<u16, Arc<CompQueue>>,
}

pub struct NvmeBlockQueue {
    sq: Arc<SubQueue>,
    acc_mem: MemAccessor,
    state: Mutex<NvmeBlockQueueNotificationState>,
}

impl NvmeBlockQueue {
    pub(super) fn new(sq: Arc<SubQueue>, acc_mem: MemAccessor) -> Self {
        Self {
            sq,
            acc_mem,
            state: Mutex::new(
                NvmeBlockQueueNotificationState {
                    reqs: 0,
                    last: Instant::now(),
                    cqs_to_notify: Default::default(),
                }
            ),
        }
    }
}

impl block::DeviceQueue for NvmeBlockQueue {
    type Token = Permit;

    /// Pop an available I/O request off of the Submission Queue for hand-off to
    /// the underlying block backend
    fn next_reqs(&self) -> DeviceQueueNextReqs<Self::Token> {
        let sq = &self.sq;
        let Some(mem) = self.acc_mem.access() else {
            return DeviceQueueNextReqs::NoMem;
        };
        let params = self.sq.params();

        let mut reqs = Vec::with_capacity(65536); // XXX
        let mut skipped = 0;

        let (sq_reqs, stopped_could_not_reserve, queue_empty) = match sq.pop() {
            SubQueuePop::NoMem => {
                return DeviceQueueNextReqs::NoMem;
            }

            SubQueuePop::Reqs { sq_reqs, stopped_could_not_reserve, queue_empty } => {
                (sq_reqs, stopped_could_not_reserve, queue_empty)
            }
        };

        let mut cqs_to_notify: HashMap<u16, Arc<CompQueue>> = HashMap::new();

        for (sub, permit, idx) in sq_reqs {
            let qid = sq.id();
            probes::nvme_raw_cmd!(|| {
                (
                    qid,
                    u64::from(sub.cdw0) | (u64::from(sub.nsid) << 32),
                    sub.prp1,
                    sub.prp2,
                    (u64::from(sub.cdw10) | (u64::from(sub.cdw11) << 32)),
                )
            });
            let cid = sub.cid();
            let cmd = NvmCmd::parse(sub);

            match cmd {
                Ok(NvmCmd::Write(cmd)) => {
                    let off = params.lba_data_size * cmd.slba;
                    let size = params.lba_data_size * (cmd.nlb as u64);

                    if size > params.max_data_tranfser_size {
                        skipped += 1;
                        let comp = 
                            Completion::generic_err(bits::STS_INVAL_FIELD)
                                .dnr();
                        if let Some(cq) = permit.complete(comp) {
                            cqs_to_notify.insert(cq.id(), cq);
                        }
                        continue;
                    }

                    probes::nvme_write_enqueue!(|| (qid, idx, cid, off, size));

                    let bufs = cmd.data(size, &mem).collect();
                    let req =
                        Request::new_write(off as usize, size as usize, bufs);
                    reqs.push((req, permit, None));
                }
                Ok(NvmCmd::Read(cmd)) => {
                    let off = params.lba_data_size * cmd.slba;
                    let size = params.lba_data_size * (cmd.nlb as u64);

                    if size > params.max_data_tranfser_size {
                        skipped += 1;
                        let comp =
                            Completion::generic_err(bits::STS_INVAL_FIELD)
                                .dnr();
                        if let Some(cq) = permit.complete(comp) {
                            cqs_to_notify.insert(cq.id(), cq);
                        }
                        continue;
                    }

                    probes::nvme_read_enqueue!(|| (qid, idx, cid, off, size));

                    let bufs = cmd.data(size, &mem).collect();
                    let req =
                        Request::new_read(off as usize, size as usize, bufs);
                    reqs.push((req, permit, None));
                }
                Ok(NvmCmd::Flush) => {
                    probes::nvme_flush_enqueue!(|| (qid, idx, cid));
                    let req = Request::new_flush();
                    reqs.push((req, permit, None));
                }
                Ok(NvmCmd::Unknown(_)) | Err(_) => {
                    skipped += 1;
                    // For any other unrecognized or malformed command,
                    // just immediately complete it with an error
                    let comp = Completion::generic_err(bits::STS_INTERNAL_ERR);
                    if let Some(cq) = permit.complete(comp) {
                        cqs_to_notify.insert(cq.id(), cq);
                    }
                }
            }
        }

        for (_, cq) in cqs_to_notify {
            cq.fire_interrupt();
        }

        DeviceQueueNextReqs::Reqs {
            reqs,
            skipped,
            stopped_could_not_reserve,
            queue_empty,
        }
    }

    /// Place the operation result (success or failure) onto the corresponding
    /// Completion Queue.
    fn complete(
        &self,
        op: block::Operation,
        result: block::Result,
        permit: Self::Token,
    ) {
        let qid = permit.sqid();
        let cid = permit.cid();
        let resnum = result as u8;
        match op {
            Operation::Read(..) => {
                probes::nvme_read_complete!(|| (qid, cid, resnum));
            }
            Operation::Write(..) => {
                probes::nvme_write_complete!(|| (qid, cid, resnum));
            }
            Operation::Flush => {
                probes::nvme_flush_complete!(|| (qid, cid, resnum));
            }
            Operation::Discard(..) => {
                unreachable!("discard not supported in NVMe for now");
            }
        }


        /*
        // XXX uncomment to bypass this coalescing
        if let Some(cq) = permit.complete(Completion::from(result)) {
            cq.fire_interrupt();
        }
        */

        if let Some(cq) = permit.complete(Completion::from(result)) {
            let mut state = self.state.lock().unwrap();

            if !state.cqs_to_notify.contains_key(&cq.id()) {
                let old = state.cqs_to_notify.insert(cq.id(), cq);
                assert!(old.is_none());
            }

            state.reqs += 1;

            // XXX tune
            let now = Instant::now();
            if state.reqs > 32 || (now - state.last).as_micros() >= 100 {
                let to_notify = std::mem::take(&mut state.cqs_to_notify);
                state.reqs = 0;
                state.last = now;
                drop(state);

                // XXX don't hold state lock when doing this!
                for (_, cq) in to_notify {
                    cq.fire_interrupt();
                }
            }
        }
    }

    fn flush_notifications(&self) {
        let mut state = self.state.lock().unwrap();
        let to_notify = std::mem::take(&mut state.cqs_to_notify);
        state.reqs = 0;
        state.last = Instant::now();
        drop(state);

        // XXX don't hold state lock when doing this!
        for (_, cq) in to_notify {
            cq.fire_interrupt();
        }
    }
}
