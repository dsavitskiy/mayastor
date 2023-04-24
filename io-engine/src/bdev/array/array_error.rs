use crate::bdev_api::BdevError;
use nix::errno::Errno;
use snafu::Snafu;

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum ArrayError {
    #[snafu(display("Async operation canceled on: '{name}'"))]
    Canceled {
        source: futures::channel::oneshot::Canceled,
        name: String,
    },
    #[snafu(display("Block device '{name}' not found"))]
    BdevNotFound { name: String },
    #[snafu(display("Failed to open descriptor for block device '{name}'"))]
    BdevDescOpenFailed { source: Errno, name: String },
    #[snafu(display("Failed to open device '{name}'"))]
    DeviceOpenFailed { source: Errno, name: String },
    #[snafu(display("Failed to close device '{name}'"))]
    DeviceCloseFailed { source: Errno, name: String },
    #[snafu(display("Failed to create block device '{name}'"))]
    BdevCreateFailed { source: Errno, name: String },
    #[snafu(display("Failed to destroy block device '{name}'"))]
    BdevDestroyFailed { source: Errno, name: String },
    #[snafu(display("Not enough devices for this array: '{array_name}'"))]
    NotEnoughDevices { array_name: String },
    #[snafu(display("Device block length mismatch: '{name}'"))]
    DeviceBlockLengthMismatch { name: String },
    #[snafu(display("Device size mismatch: '{name}'"))]
    DeviceSizeMismatch { name: String },
}

impl From<ArrayError> for BdevError {
    fn from(value: ArrayError) -> Self {
        match value {
            ArrayError::Canceled {
                source,
                name,
            } => BdevError::BdevCommandCanceled {
                source,
                name,
            },

            ArrayError::BdevNotFound {
                name,
            } => BdevError::BdevNotFound {
                name,
            },

            ArrayError::BdevDescOpenFailed {
                source,
                name,
            } => BdevError::CreateBdevFailed {
                source,
                name,
            },

            ArrayError::DeviceOpenFailed {
                source,
                name,
            } => BdevError::CreateBdevFailed {
                source,
                name,
            },

            ArrayError::DeviceCloseFailed {
                source,
                name,
            } => BdevError::CreateBdevFailed {
                source,
                name,
            },

            ArrayError::BdevCreateFailed {
                source,
                name,
            } => BdevError::CreateBdevFailed {
                source,
                name,
            },

            ArrayError::BdevDestroyFailed {
                source,
                name,
            } => BdevError::DestroyBdevFailed {
                source,
                name,
            },

            ArrayError::NotEnoughDevices {
                array_name,
            } => BdevError::CreateBdevInvalidParams {
                source: Errno::EINVAL,
                name: array_name,
            },

            ArrayError::DeviceBlockLengthMismatch {
                name,
            } => BdevError::CreateBdevInvalidParams {
                source: Errno::EINVAL,
                name,
            },

            ArrayError::DeviceSizeMismatch {
                name,
            } => BdevError::CreateBdevInvalidParams {
                source: Errno::EINVAL,
                name,
            },
        }
    }
}
