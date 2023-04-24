use std::{
    ffi::CString,
    fmt::{Display, Formatter},
    ops::Deref,
};

use futures::channel::oneshot;
use nix::errno::Errno;

use crate::core::partition::{calc_data_partition, Partitions};

use spdk_rs::{
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
    libspdk::{bdev_aio_delete, create_aio_bdev},
    UntypedBdev,
};

use super::{ArrayError, ArrayState};

/// TODO
pub struct ArrayDevice {
    name: String,
    bdev: UntypedBdev,
    state: ArrayState,
    part: Partitions,
}

impl Display for ArrayDevice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Array Device '{name}' ({state:?}): {part}",
            name = self.name,
            state = self.state,
            part = self.part,
        )
    }
}

impl Drop for ArrayDevice {
    fn drop(&mut self) {
        assert_eq!(
            self.state,
            ArrayState::Closed,
            "Array device must be closed before dropping"
        );
    }
}

impl ArrayDevice {
    /// TODO
    pub async fn open(name: &str) -> Result<Self, ArrayError> {
        info!("%%%% AIO Device '{name}' :: OPENING ...");

        let cname = CString::new(name).unwrap();

        let errno = unsafe {
            create_aio_bdev(cname.as_ptr(), cname.as_ptr(), 0, false)
        };

        if errno != 0 {
            return Err(ArrayError::DeviceOpenFailed {
                source: Errno::from_i32(errno.abs()),
                name: name.to_owned(),
            });
        }

        let Some(bdev) = UntypedBdev::lookup_by_name(name) else {
            return Err(ArrayError::BdevNotFound {
                name: name.to_owned(),
            });
        };

        let Some(part) = calc_data_partition(
            0,
            bdev.num_blocks(),
            bdev.block_len() as u64,
        ) else {
            error!("%%%% AIO Device '{name}' :: bad size");

            Self::close_bdev(name, bdev).await?;

            return Err(ArrayError::DeviceOpenFailed {
                source: Errno::EINVAL,
                name: name.to_owned(),
            });
        };

        let res = Self {
            name: name.to_owned(),
            bdev,
            state: ArrayState::Online,
            part,
        };

        info!("%%%% {res} :: OPENED");

        Ok(res)
    }

    /// TODO
    pub async fn close(mut self) -> Result<(), ArrayError> {
        info!("%%%% {self} :: CLOSING ...");

        Self::close_bdev(&self.name, self.bdev).await?;
        self.state = ArrayState::Closed;

        info!("%%%% AIO Device '{name}' :: CLOSED", name = self.name);
        Ok(())
    }

    /// TODO
    pub async fn close_bdev(
        name: &str,
        bdev: UntypedBdev,
    ) -> Result<(), ArrayError> {
        let (s, r) = oneshot::channel::<ErrnoResult<()>>();

        unsafe {
            bdev_aio_delete(
                (*bdev.unsafe_inner_ptr()).name,
                Some(done_errno_cb),
                cb_arg(s),
            );
        }

        r.await
            .map_err(|recv_err| ArrayError::Canceled {
                source: recv_err,
                name: name.to_owned(),
            })?
            .map_err(|aio_err| ArrayError::DeviceCloseFailed {
                source: aio_err,
                name: name.to_owned(),
            })
    }

    /// TODO
    pub fn name(&self) -> &str {
        &self.name
    }

    /// TODO
    pub fn block_len(&self) -> u32 {
        self.part.block_len() as u32
    }

    /// TODO
    pub fn num_blocks(&self) -> u64 {
        self.part.data_blocks()
    }
}
