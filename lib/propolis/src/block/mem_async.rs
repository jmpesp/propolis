// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::{Error, ErrorKind, Result};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::block;
use crate::block::attachment::WorkerCollection;
use crate::tasks::TaskGroup;
use crate::vmm::MemCtx;

/// Block device backend which uses anonymous memory as its storage.
///
/// While not useful for actually storage data beyond the life of an instance,
/// this backend can be used for measuring how other parts of the emulation
/// stack perform.
pub struct MemAsyncBackend {
    shared_state: Arc<SharedState>,
    workers: TaskGroup,
    worker_count: NonZeroUsize,
}
struct SharedState {
    seg: MmapSeg,
    info: block::DeviceInfo,
}
impl SharedState {
    async fn processing_loop(&self, wctx: block::AsyncWorkerCtx) {
        while let Some(dreq) = wctx.next_req().await {
            let req = dreq.req();
            if self.info.read_only && req.op.is_write() {
                dreq.complete(block::Result::ReadOnly);
                continue;
            }
            if req.op.is_discard() {
                dreq.complete(block::Result::Unsupported);
                continue;
            }

            let res = match wctx
                .acc_mem()
                .access()
                .and_then(|mem| self.process_request(&req, &mem).ok())
            {
                Some(_) => block::Result::Success,
                None => block::Result::Failure,
            };
            dreq.complete(res);
        }
    }

    fn process_request(
        &self,
        req: &block::Request,
        mem: &MemCtx,
    ) -> std::result::Result<(), &'static str> {
        let seg = &self.seg;
        match req.op {
            block::Operation::Read(off, _len) => {
                req.regions
                    .iter()
                    .try_fold(0usize, |nread, region| {
                        let map = mem.writable_region(region)?;
                        unsafe {
                            let read_ptr = map.raw_writable()?;
                            let len = map.len();
                            seg.read(off + nread, read_ptr, len)
                                .then_some(nread + len)
                        }
                    })
                    .ok_or("read failure")?;
            }
            block::Operation::Write(off, _len) => {
                req.regions
                    .iter()
                    .try_fold(0usize, |nwritten, region| {
                        let map = mem.readable_region(region)?;
                        unsafe {
                            let write_ptr = map.raw_readable()?;
                            let len = map.len();
                            seg.write(off + nwritten, write_ptr, len)
                                .then_some(nwritten + len)
                        }
                    })
                    .ok_or("write failure")?;
            }
            block::Operation::Flush => {
                // nothing to do
            }
            block::Operation::Discard(..) => {
                unreachable!("handled in processing_loop()")
            }
        }

        Ok(())
    }
}

impl MemAsyncBackend {
    pub fn create(
        size: u64,
        opts: block::BackendOpts,
        worker_count: NonZeroUsize,
    ) -> Result<Arc<Self>> {
        let block_size = opts.block_size.unwrap_or(block::DEFAULT_BLOCK_SIZE);

        if size == 0 {
            return Err(Error::new(ErrorKind::Other, "size cannot be 0"));
        } else if (size % u64::from(block_size)) != 0 {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "size {} not multiple of block size {}!",
                    size, block_size,
                ),
            ));
        }

        let info = block::DeviceInfo {
            block_size,
            total_size: size / u64::from(block_size),
            read_only: opts.read_only.unwrap_or(false),
            supports_discard: false,
        };
        let seg = MmapSeg::new(size as usize)?;

        Ok(Arc::new(Self {
            shared_state: Arc::new(SharedState { info, seg }),
            workers: TaskGroup::new(),
            worker_count,
        }))
    }

    fn spawn_workers(&self, workers: &Arc<WorkerCollection>) {
        let count = self.worker_count.get();
        self.workers.extend((0..count).map(|n| {
            let shared_state = self.shared_state.clone();
            let wctx = workers.inactive_worker(n);
            tokio::spawn(async move {
                let wctx =
                    wctx.activate_async().expect("worker slot is uncontended");
                shared_state.processing_loop(wctx).await
            })
        }))
    }
}

#[async_trait::async_trait]
impl block::Backend for MemAsyncBackend {
    fn info(&self) -> block::DeviceInfo {
        self.shared_state.info
    }

    fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    async fn start(&self, workers: &Arc<WorkerCollection>) -> anyhow::Result<()> {
        self.spawn_workers(workers);
        Ok(())
    }

    async fn stop(&self) -> () {
        self.workers.join_all().await;
    }
}

struct MmapSeg(NonNull<u8>, usize);
impl MmapSeg {
    fn new(size: usize) -> Result<Self> {
        let ptr = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }
        Ok(Self(NonNull::new(ptr as *mut u8).unwrap(), size))
    }
    unsafe fn write(&self, off: usize, data: *const u8, sz: usize) -> bool {
        if (off + sz) > self.1 {
            return false;
        }

        self.0.as_ptr().add(off).copy_from_nonoverlapping(data, sz);
        true
    }
    unsafe fn read(&self, off: usize, data: *mut u8, sz: usize) -> bool {
        if (off + sz) > self.1 {
            return false;
        }

        self.0.as_ptr().add(off).copy_to_nonoverlapping(data, sz);
        true
    }
}
impl Drop for MmapSeg {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.0.as_ptr() as *mut libc::c_void, self.1);
        }
    }
}
// Safety: The consumer is allowed to make their own pointer mistakes
unsafe impl Send for MmapSeg {}
unsafe impl Sync for MmapSeg {}
