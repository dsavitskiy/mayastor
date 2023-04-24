use std::{
    fmt::{Display, Formatter},
    ops::{Deref, DerefMut},
};

use spdk_rs::{
    libspdk::{spdk_bdev_io, spdk_io_channel},
    BdevIo,
    IoChannel,
};

use crate::core::IoStatus;

use super::{ArrayBdev, ArrayDesc, ArrayChannelData};

/// TODO
pub(super) struct ArrayBdevIoCtx {
    channel: IoChannel<ArrayChannelData>,
    status: IoStatus,
}

impl Display for ArrayBdevIoCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{status:?}", status = self.status)
    }
}

impl ArrayBdevIoCtx {
    pub(super) fn init_new_request(
        &mut self,
        channel: IoChannel<ArrayChannelData>,
    ) {
        self.channel = channel;
        self.status = IoStatus::Pending;
    }
}

/// I/O instance for an array Bdev.
pub struct ArrayIo(BdevIo<ArrayBdev>);

impl Display for ArrayIo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Array I/O {type:?} at {off}/{num}: {ctx}",
            type = self.io_type(),
            off = self.offset(),
            num = self.num_blocks(),
            ctx = self.ctx(),
        )
    }
}

impl Deref for ArrayIo {
    type Target = BdevIo<ArrayBdev>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ArrayIo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<*mut spdk_bdev_io> for ArrayIo {
    fn from(ptr: *mut spdk_bdev_io) -> Self {
        Self(BdevIo::<ArrayBdev>::legacy_from_ptr(ptr))
    }
}

impl ArrayIo {
    /// Makes a new instance of `ArrayIo`.
    #[inline(always)]
    pub(super) fn new(
        io: BdevIo<ArrayBdev>,
        chan: IoChannel<ArrayChannelData>,
    ) -> Self {
        let mut io = Self(io);
        io.ctx_mut().init_new_request(chan);
        io
    }

    /// Returns an immutable reference to the I/O context.
    #[inline(always)]
    pub(super) fn ctx(&self) -> &ArrayBdevIoCtx {
        self.driver_ctx::<ArrayBdevIoCtx>()
    }

    /// Returns a mutable reference to the I/O context.
    #[inline(always)]
    pub(super) fn ctx_mut(&mut self) -> &mut ArrayBdevIoCtx {
        self.driver_ctx_mut::<ArrayBdevIoCtx>()
    }

    /// TODO
    #[inline(always)]
    pub(super) fn io_channel(&self) -> &IoChannel<ArrayChannelData> {
        &self.ctx().channel
    }

    /// TODO
    #[inline(always)]
    pub(super) unsafe fn io_channel_ptr(&self) -> *mut spdk_io_channel {
        self.ctx().channel.legacy_as_ptr()
    }

    // /// TODO
    // #[inline(always)]
    // fn handle(&self) -> &dyn ArrayStore {
    //     self.ctx().channel.channel_data().handle()
    // }
    //
    // /// TODO
    // #[inline(always)]
    // fn as_cb_arg(&self) -> IoCompletionCallbackArg {
    //     self.0.legacy_as_ptr().cast()
    // }
    //
    // /// TODO
    // pub(super) fn submit_request(mut self) {
    //     info!("SUBM :: {self:?}");
    //
    //     if let Err(err) = match self.io_type() {
    //         IoType::Read => self.handle().readv(&self),
    //         // IoType::Write => self.handle().writev_blocks(
    //         //     self.iovs(),
    //         //     self.iov_count(),
    //         //     self.effective_offset(),
    //         //     self.num_blocks(),
    //         //     Self::completion_cb,
    //         //     self.as_cb_arg(),
    //         // ),
    //         // IoType::WriteZeros => self.handle().write_zeroes(
    //         //     self.effective_offset(),
    //         //     self.num_blocks(),
    //         //     Self::completion_cb,
    //         //     self.as_cb_arg(),
    //         // ),
    //         // IoType::Reset => {
    //         //     self.handle().reset(Self::completion_cb, self.as_cb_arg())
    //         // }
    //         // IoType::Unmap => self.handle().unmap_blocks(
    //         //     self.effective_offset(),
    //         //     self.num_blocks(),
    //         //     Self::completion_cb,
    //         //     self.as_cb_arg(),
    //         // ),
    //         // IoType::Flush => Ok(()),
    //         _ => {
    //             error!("{self:?}: I/O type not supported");
    //             self.fail();
    //             Err(CoreError::NotSupported {
    //                 source: Errno::EOPNOTSUPP,
    //             })
    //         }
    //     } {
    //         error!("Submission error: {self:?}: {err}");
    //         // let device_name = self.device().get_device().device_name();
    //     }
    // }

    // /// TODO
    // fn submit_read(&self) {}
    //
    // /// TODO
    // fn completion_cb(
    //     dev: &dyn BlockDevice,
    //     status: IoCompletionStatus,
    //     ctx: *mut c_void,
    // ) {
    //     let mut io = ArrayIo::from(ctx as *mut spdk_bdev_io);
    //     io.complete(dev, status);
    // }
    //
    // /// TODO
    // fn complete(&mut self, dev: &dyn BlockDevice, status: IoCompletionStatus)
    // {     info!("COMPL :: {self:?}");
    //
    //     if status == IoCompletionStatus::Success {
    //         self.ok();
    //     } else {
    //         self.fail();
    //     }
    // }
}
