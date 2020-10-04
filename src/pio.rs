use crate::util::aspace::ASpace;
use std::sync::{Arc, Mutex, Weak};

pub trait PioDev: Send + Sync {
    fn pio_out(&self, port: u16, off: u16, data: &[u8]);
    fn pio_in(&self, port: u16, off: u16, data: &mut [u8]);
}

type PioDevHdl = Arc<dyn PioDev>;

pub struct PioBus {
    map: Mutex<ASpace<Weak<dyn PioDev>>>,
}

impl PioBus {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(ASpace::new(0, u16::MAX as usize)),
        }
    }

    pub fn register(&self, start: u16, len: u16, dev: &PioDevHdl) {
        let weak = Arc::downgrade(dev);
        self.map
            .lock()
            .unwrap()
            .register(start as usize, len as usize, weak)
            .unwrap();
    }

    pub fn handle_out(&self, port: u16, bytes: u8, val: u32) {
        let buf = val.to_le_bytes();
        let data = match bytes {
            1 => &buf[0..1],
            2 => &buf[0..2],
            4 => &buf[0..],
            _ => panic!(),
        };
        if !self.do_pio(port, |p, o, dev| dev.pio_out(p, o, data)) {
            println!("unhandled IO out - port:{:x} len:{}", port, bytes);
        }
    }

    pub fn handle_in(&self, port: u16, bytes: u8) -> u32 {
        let mut buf = [0xffu8; 4];
        let data = match bytes {
            1 => &mut buf[0..1],
            2 => &mut buf[0..2],
            4 => &mut buf[0..],
            _ => panic!(),
        };
        if !self.do_pio(port, |p, o, dev| dev.pio_in(p, o, data)) {
            println!("unhandled IO in - port:{:x} len:{}", port, bytes);
        }

        u32::from_le_bytes(buf)
    }

    fn do_pio<F>(&self, port: u16, f: F) -> bool
    where
        F: FnOnce(u16, u16, &PioDevHdl),
    {
        let map = self.map.lock().unwrap();
        if let Ok((start, _len, weak)) = map.region_at(port as usize) {
            let dev = Weak::upgrade(weak).unwrap();
            // unlock map before entering handler
            drop(map);
            f(start as u16, port - start as u16, &dev);
            true
        } else {
            false
        }
    }
}

// Make the casting easier for trait objects
macro_rules! pio_dyn {
    ($e: expr) => { &($e as Arc<dyn PioDev>) }
}
