use std::{
    fmt::{Display, Formatter},
    os::raw::c_void,
};

use async_trait::async_trait;

use super::{
    Array,
    ArrayDesc,
    ArrayDeviceDesc,
    ArrayError,
    ArrayIo,
    ArrayParams,
    ArraySpanDesc,
};

/// TODO
pub struct ArraySpan {
    params: ArrayParams,
    block_len: u32,
    num_blocks: u64,
    required_alignment: u8,
}

impl Display for ArraySpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Span '{name}': {sz} x {blen} <{devs}>",
            name = self.params.name,
            sz = self.num_blocks,
            blen = self.block_len,
            devs = self.params.devices.device_names().join("; ")
        )
    }
}

impl Drop for ArraySpan {
    fn drop(&mut self) {
        assert_eq!(
            self.block_len, 0,
            "Array span must be destroyed before dropping"
        );
    }
}

impl ArraySpan {
    /// TODO
    pub async fn create_array(
        params: ArrayParams,
    ) -> Result<Box<dyn Array>, ArrayError> {
        info!(
            "%%%% Span '{name}' :: CREATING :: <{devices}>",
            name = params.name,
            devices = params.devices.device_names().join("; ")
        );

        let (block_len, num_blocks) = Self::validate_devices(&params).await?;

        let s = Self {
            params,
            block_len,
            num_blocks,
            required_alignment: 9,
        };

        info!("%%%% {s} :: CREATED");

        Ok(Box::new(s))
    }

    /// TODO
    async fn validate_devices(
        params: &ArrayParams,
    ) -> Result<(u32, u64), ArrayError> {
        let mut devices = params.devices.iter();

        let Some(first_dev) = devices.next() else {
            return Err(ArrayError::NotEnoughDevices {
                array_name: params.name.clone(),
            });
        };

        let block_len = first_dev.block_len();
        let num_blocks = first_dev.num_blocks();

        for dev in devices {
            if block_len != dev.block_len() {
                return Err(ArrayError::DeviceBlockLengthMismatch {
                    name: dev.name().to_owned(),
                });
            }

            if num_blocks != dev.num_blocks() {
                return Err(ArrayError::DeviceSizeMismatch {
                    name: dev.name().to_owned(),
                });
            }
        }

        Ok((block_len, num_blocks))
    }
}

#[async_trait]
impl Array for ArraySpan {
    async fn destroy(&mut self) {
        let s = self.to_string();
        info!("%%%% {s} :: DESTROYING ...");

        self.params.devices.close().await;
        self.block_len = 0;
        self.num_blocks = 0;
        self.required_alignment = 0;

        info!("%%%% {s} :: DESTROYED");
    }

    fn name(&self) -> &str {
        &self.params.name
    }

    fn uuid(&self) -> uuid::Uuid {
        self.params.uuid
    }

    fn block_len(&self) -> u32 {
        self.block_len
    }

    fn num_blocks(&self) -> u64 {
        self.num_blocks
    }

    fn required_alignment(&self) -> u8 {
        self.required_alignment
    }

    fn open_desc(&self) -> Box<dyn ArrayDesc> {
        let mut v = Vec::new();

        for dev in self.params.devices.iter() {
            let desc = ArrayDeviceDesc::open(&dev).unwrap();
            v.push(desc);
        }

        Box::new(ArraySpanDesc::new(&self.params.name, v))
    }
}
