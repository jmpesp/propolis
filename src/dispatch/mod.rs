use std::io::Result;
use std::sync::{Mutex, Arc};
use std::thread::{Builder, JoinHandle, Thread};
use std::fmt::format;

use crate::vcpu::VcpuHdl;
use crate::machine::MachineCtx;
use crate::pio::PioBus;

pub struct Dispatcher {
    mctx: MachineCtx,
    tasks: Mutex<Vec<(String, JoinHandle<()>)>>,
}

impl Dispatcher {
    pub fn new(mctx: MachineCtx) -> Self {
        Self {
            mctx,
            tasks: Mutex::new(Vec::new()),
        }
    }

    pub fn spawn<D>(&self, name: String, data: D, func: fn(DispCtx, D)) -> Result<()>
    where
        D: Send + 'static,
    {
        let ctx = DispCtx::new(self.mctx.clone());
        let hdl = Builder::new().name(name.clone()).spawn(move || {
            func(ctx, data);
        })?;
        self.tasks.lock().unwrap().push((name, hdl));
        Ok(())
    }
}

pub struct DispCtx {
    pub mctx: MachineCtx,
    pub vcpu: Option<VcpuHdl>,
}

impl DispCtx {
    fn new(mctx: MachineCtx) -> DispCtx {
        DispCtx { mctx, vcpu: None }
    }

    fn for_vcpu(mctx: MachineCtx, cpu: VcpuHdl) -> DispCtx {
        Self { mctx, vcpu: Some(cpu) }
    }
}
