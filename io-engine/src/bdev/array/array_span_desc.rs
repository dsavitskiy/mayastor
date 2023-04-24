use spdk_rs::Cores;
use std::fmt::{Display, Formatter};

use super::{ArrayDesc, ArrayDeviceDesc, ArrayIo};

/// TODO
pub(super) struct ArraySpanDesc {
    core: u32,
    name: String,
    devices: Vec<ArrayDeviceDesc>,
}

impl Display for ArraySpanDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Span desc [{core}/{cur}] '{name}': {cnt} devices",
            core = self.core,
            cur = Cores::current(),
            name = self.name,
            cnt = self.devices.len()
        )
    }
}

impl Drop for ArraySpanDesc {
    fn drop(&mut self) {
        info!("%%%% {self} :: DROP");
        assert!(
            self.devices.is_empty(),
            "Array span channel must be destroyed before dropping"
        );
    }
}

impl ArraySpanDesc {
    pub(super) fn new(name: &str, devices: Vec<ArrayDeviceDesc>) -> Self {
        let res = Self {
            core: Cores::current(),
            name: name.to_owned(),
            devices,
        };
        info!("%%%% {res} :: NEW");
        res
    }
}

impl ArrayDesc for ArraySpanDesc {
    fn destroy(&mut self) {
        info!("%%%% {self} :: DESTROYING ...");
        self.devices.drain(..).for_each(|mut d| d.destroy());
        info!("%%%% {self} :: DESTROYED");
    }

    fn read(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");

        // let n = ArrayIo::alloc();
        // self.devices[0].read(&n);
        // io.ok();
        // // let ctx = alloc_bdev_io_ctx(
        // //     IoType::Read,
        // //     IoCtx {
        // //         device: self.device,
        // //         cb,
        // //         cb_arg,
        // //     },
        // //     offset_blocks,
        // //     num_blocks,
        // // )?;
        //
        // // let (desc, chan) = self.handle.io_tuple();
        // let rc = unsafe {
        //     let desc = self.devices[0].bdev_desc_ptr();
        //     let chan = io.io_channel_ptr();
        //
        //     spdk_bdev_readv_blocks(
        //         desc,
        //         chan,
        //         io.iovs(),
        //         io.iov_count(),
        //         io.offset(),
        //         io.num_blocks(),
        //         None,
        //         0 as *mut c_void,
        //     )
        // };
        //
        // if rc < 0 {
        //     io.fail();
        //     // Err(CoreError::ReadDispatch {
        //     //     source: Errno::from_i32(-rc),
        //     //     offset: offset_blocks,
        //     //     len: num_blocks,
        //     // })
        // }
    }

    fn write(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");
        io.ok();
    }

    fn write_zeros(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");
        io.ok();
    }

    fn unmap(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");
        io.ok();
    }

    fn reset(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");
        io.ok();
    }

    fn flush(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");
        io.ok();
    }

    fn nvme_admin(&self, io: &ArrayIo) {
        info!("%%%% {self} :: {io}");
        io.ok();
    }
}
