use nix::errno::Errno;
use std::{
    fmt::{Display, Formatter},
    ops::{Deref, DerefMut},
    pin::Pin,
};

use spdk_rs::{Bdev, BdevIo, BdevModule, BdevOps, IoChannel, IoDevice, IoType};

use crate::core::Reactor;

use super::{
    array_bdev_module::ArrayBdevModule,
    array_error::ArrayError,
    Array,
    ArrayDesc,
    ArrayIo,
    ARRAY_BDEV_MODULE_NAME,
    ARRAY_BDEV_PRODUCT_ID,
};

/// TODO
pub struct ArrayChannelData {
    desc: Box<dyn ArrayDesc>,
}

impl ArrayChannelData {
    fn new(desc: Box<dyn ArrayDesc>) -> Self {
        Self {
            desc,
        }
    }

    fn destroy(&mut self) {
        self.desc.destroy();
    }

    fn array_desc(&self) -> &dyn ArrayDesc {
        self.desc.as_ref()
    }
}

/// TODO
pub struct ArrayBdev {
    name: String,
    bdev: Option<Bdev<ArrayBdev>>,
    array: Option<Box<dyn Array>>,
}

impl Display for ArrayBdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Array Bdev '{name}'", name = self.name)
    }
}

impl Deref for ArrayBdev {
    type Target = Bdev<ArrayBdev>;

    fn deref(&self) -> &Self::Target {
        self.bdev.as_ref().expect("Array Bdev must be registered")
    }
}

impl DerefMut for ArrayBdev {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bdev.as_mut().expect("Array Bdev must be registered")
    }
}

impl BdevOps for ArrayBdev {
    type ChannelData = ArrayChannelData;
    type BdevData = ArrayBdev;
    type IoDev = ArrayBdev;

    fn destruct(mut self: Pin<&mut Self>) {
        let mut array = self.array.take().expect("Array must be initialized");

        Reactor::block_on(async move {
            array.destroy().await;
        });

        self.unregister_io_device();
    }

    fn submit_request(
        &self,
        chan: IoChannel<Self::ChannelData>,
        io: BdevIo<Self>,
    ) {
        let io = ArrayIo::new(io, chan);
        io.io_channel().channel_data().array_desc().submit_request(&io);
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        self.array().io_type_supported(io_type)
    }

    fn get_io_device(&self) -> &Self::IoDev {
        self
    }
}

impl IoDevice for ArrayBdev {
    type ChannelData = ArrayChannelData;

    fn io_channel_create(self: Pin<&mut Self>) -> Self::ChannelData {
        ArrayChannelData::new(self.array().open_desc())
    }

    fn io_channel_destroy(
        self: Pin<&mut Self>,
        mut io_chan: Self::ChannelData,
    ) {
        io_chan.destroy();
    }
}

impl ArrayBdev {
    /// TODO
    pub async fn create(array: Box<dyn Array>) -> Result<(), ArrayError> {
        let name = array.name().to_owned();

        let bm = BdevModule::find_by_name(ARRAY_BDEV_MODULE_NAME).unwrap();

        let mut bdev = bm
            .bdev_builder()
            .with_name(&array.name())
            .with_product_name(ARRAY_BDEV_PRODUCT_ID)
            .with_uuid(array.uuid().into())
            .with_block_length(array.block_len())
            .with_block_count(array.num_blocks())
            .with_required_alignment(array.required_alignment())
            .with_data(Self {
                name: array.name().to_owned(),
                bdev: None,
                array: Some(array),
            })
            .build();

        // TODO: claim disk bdevs?

        bdev.data_mut().bdev = Some(bdev.clone());

        bdev.data().register_io_device(Some(&bdev.data().name));

        bdev.register_bdev()
            .map_err(|errno| ArrayError::BdevCreateFailed {
                source: errno,
                name,
            })
    }

    /// TODO
    pub async fn destroy(&mut self) -> Result<(), ArrayError> {
        info!("%%%% {self} :: DESTROYING...");

        let name = self.name.clone();

        self.unregister_bdev_async().await.map_err(|spdk_err| {
            error!("%%%% {self} :: FAILED TO UNREG: {spdk_err}");
            ArrayError::BdevDestroyFailed {
                source: Errno::EINVAL,
                name: name.clone(),
            }
        })
    }

    /// TODO
    pub fn lookup_by_name(name: &str) -> Option<&ArrayBdev> {
        ArrayBdevModule::current()
            .iter_bdevs()
            .find(|bdev| bdev.name() == name)
            .map(|bdev| bdev.data())
    }

    /// TODO
    pub fn lookup_by_name_mut(name: &str) -> Option<Pin<&mut ArrayBdev>> {
        ArrayBdevModule::current()
            .iter_bdevs()
            .find(|bdev| bdev.name() == name)
            .map(|mut bdev| bdev.data_mut())
    }

    /// TODO
    #[inline(always)]
    fn array(&self) -> &dyn Array {
        self.array
            .as_ref()
            .expect("Array must be initialized")
            .as_ref()
    }
}
