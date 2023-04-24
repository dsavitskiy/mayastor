use std::{
    fmt::{Display, Formatter},
    os::raw::c_void,
};

use spdk_rs::{
    libspdk::{spdk_bdev_desc, spdk_bdev_io, spdk_bdev_readv_blocks},
    BdevDesc,
    BdevEvent,
    Cores,
    NvmeStatus,
    UntypedBdev,
};

use crate::core::IoCompletionStatus;

use super::{ArrayDesc, ArrayDevice, ArrayError, ArrayIo};

/// TODO
pub struct ArrayDeviceDesc {
    core: u32,
    desc: BdevDesc<()>,
}

impl Display for ArrayDeviceDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Arry device desc [{core}/{cur}] '{name}'",
            core = self.core,
            cur = Cores::current(),
            name = self.name(),
        )
    }
}

impl Drop for ArrayDeviceDesc {
    fn drop(&mut self) {
        assert!(
            self.desc.legacy_as_ptr().is_null(),
            "Array device descriptor must be closed before dropping"
        );
    }
}

impl ArrayDeviceDesc {
    pub fn open(dev: &ArrayDevice) -> Result<Self, ArrayError> {
        let desc = BdevDesc::open(dev.name(), true, array_bdev_event_callback)
            .map_err(|errno| ArrayError::BdevDescOpenFailed {
                source: errno,
                name: dev.name().to_owned(),
            })?;

        Ok(Self {
            core: Cores::current(),
            desc,
        })
    }

    pub unsafe fn bdev_desc_ptr(&self) -> *mut spdk_bdev_desc {
        self.desc.legacy_as_ptr()
    }

    pub fn name(&self) -> String {
        self.desc.bdev().name().to_owned()
    }
}

/// TODO
fn array_bdev_event_callback(event: BdevEvent, bdev: UntypedBdev) {
    match event {
        BdevEvent::Remove => {
            info!("%%%% Received SPDK remove event for bdev '{}'", bdev.name());
        }
        BdevEvent::Resize => {
            warn!("%%%% Received SPDK resize event for bdev '{}'", bdev.name());
        }
        BdevEvent::MediaManagement => {
            warn!(
                "%%%% Received SPDK media management event for Bdev '{}'",
                bdev.name()
            );
        }
    };
}

impl ArrayDesc for ArrayDeviceDesc {
    fn destroy(&mut self) {
        self.desc.close();
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
        //     let desc = self.bdev_desc_ptr();
        //     let chan = io.io_channel_ptr();
        //
        //     spdk_bdev_readv_blocks(
        //         desc,
        //         chan,
        //         io.iovs(),
        //         io.iov_count(),
        //         io.offset(),
        //         io.num_blocks(),
        //         Some(io_completion),
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

extern "C" fn io_completion(
    child_bio: *mut spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    todo!()
    // let bio = unsafe { &mut *(ctx as *mut IoCtx) };
    //
    // // Get extended NVMe error status from original bio in case of error.
    // let status = if success {
    //     IoCompletionStatus::Success
    // } else {
    //     IoCompletionStatus::from(NvmeStatus::from(child_bio))
    // };
    //
    // (bio.cb)(&bio.device, status, bio.cb_arg);
    //
    // free_bdev_io_ctx(&mut *bio);
    //
    // unsafe {
    //     spdk_bdev_free_io(child_bio);
    // }
}
