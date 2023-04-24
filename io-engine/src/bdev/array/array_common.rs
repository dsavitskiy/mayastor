use async_trait::async_trait;
use spdk_rs::IoType;

use super::{ArrayDeviceGroup, ArrayIo};

/// TODO
pub struct ArrayParams {
    pub name: String,
    pub uuid: uuid::Uuid,
    pub devices: ArrayDeviceGroup,
}

/// TODO
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum ArrayState {
    Online,
    Closed,
}

/// TODO
#[async_trait]
pub trait Array {
    async fn destroy(&mut self);

    fn name(&self) -> &str;

    fn uuid(&self) -> uuid::Uuid;

    fn block_len(&self) -> u32;

    fn num_blocks(&self) -> u64;

    fn required_alignment(&self) -> u8;

    fn open_desc(&self) -> Box<dyn ArrayDesc>;

    fn io_type_supported(&self, io_type: IoType) -> bool {
        match io_type {
            IoType::Read
            | IoType::Write
            | IoType::WriteZeros
            | IoType::Unmap
            | IoType::Reset
            | IoType::Flush
            | IoType::NvmeAdmin => true,
            _ => {
                warn!("Array store: I/O type '{io_type:?}' not supported",);
                false
            }
        }
    }
}

/// TODO
pub trait ArrayDesc {
    fn destroy(&mut self);

    fn read(&self, io: &ArrayIo);

    fn write(&self, io: &ArrayIo);

    fn write_zeros(&self, io: &ArrayIo);

    fn unmap(&self, io: &ArrayIo);

    fn reset(&self, io: &ArrayIo);

    fn flush(&self, io: &ArrayIo);

    fn nvme_admin(&self, io: &ArrayIo);

    fn submit_request(&self, io: &ArrayIo) {
        match io.io_type() {
            IoType::Read => self.read(&io),
            IoType::Write => self.write(&io),
            IoType::WriteZeros => self.write_zeros(&io),
            IoType::Unmap => self.unmap(&io),
            IoType::Reset => self.reset(&io),
            IoType::Flush => self.flush(&io),
            IoType::NvmeAdmin => self.nvme_admin(&io),
            _ => {
                io.fail();
                todo!()
            }
        }
    }
}
