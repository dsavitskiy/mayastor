use crate::bdev::nexus::NexusChild;
use chrono::Utc;

use crate::bdev::NexusState;

use super::{ChildState, Error, Nexus};

pub mod metadata {
    use chrono::{DateTime, Utc};
    use rmp_serde;
    use snafu::Snafu;
    use spdk_rs::DmaError;

    use crate::core::{partition::Partitions, BlockDeviceHandle, CoreError};

    /// TODO
    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(crate)), context(suffix(false)))]
    pub enum Error {
        #[snafu(display("DMA buffer allocation failed"))]
        DmaMalloc { source: DmaError },
        #[snafu(display("No space on metadata partition"))]
        NoSpace {},
        #[snafu(display("Device has bad or unsupported partitions"))]
        BadPartitions {},
        #[snafu(display("Encode failed"))]
        EncodeFailed { source: rmp_serde::encode::Error },
        #[snafu(display("Save failed"))]
        SaveFailed { source: CoreError },
        #[snafu(display("Invalid header"))]
        InvalidHeader {},
        #[snafu(display("Checksum mistach"))]
        ChecksumMistach {},
        #[snafu(display("Load failed"))]
        LoadFailed { source: CoreError },
        #[snafu(display("Decode failed"))]
        DecodeFailed { source: rmp_serde::decode::Error },
        #[snafu(display("Check failed"))]
        CheckFailed { source: CoreError },
    }

    const HEADER_MAGIC: [u8; 8] = *b"NEXUSMD0";
    const HEADER_VERSION: u32 = 1000;
    const HEADER_SIZE: usize = core::mem::size_of::<Header>() as usize;

    /// TODO
    #[derive(Debug, Clone, Copy, PartialEq, Serialize)]
    #[repr(C, packed(4))]
    struct Header {
        /// Header magic number.
        pub magic: [u8; 8],
        /// Header version.
        pub version: u32,
        /// Length of the entire data, including the headers.
        pub length: u32,
        /// Start of the metadata buffer.
        pub md_offset: u32,
        /// Length of the metadata buffer.
        pub md_length: u32,
        /// Checksum of the metadata buffer.
        pub md_checksum: u64,
    }

    impl Header {
        fn to_vec(&self) -> Vec<u8> {
            unsafe {
                core::slice::from_raw_parts(
                    self as *const Self as *const u8,
                    core::mem::size_of::<Self>(),
                )
            }
            .to_vec()
        }

        fn from_slice(buf: &[u8]) -> Result<Self, Error> {
            let h = unsafe {
                *std::mem::transmute::<*const u8, *const Header>(buf.as_ptr())
            };

            if buf.len() != HEADER_SIZE
                || h.magic != HEADER_MAGIC
                || h.version != HEADER_VERSION
            {
                return Err(Error::InvalidHeader {});
            };

            Ok(h)
        }
    }

    /// TODO
    pub(crate) struct Buffer {
        buf: Vec<u8>,
    }

    impl Buffer {
        /// TODO
        pub(crate) fn from_meta(md: Metadata) -> Result<Self, Error> {
            let md_buf = md.encode()?;

            let hdr = Header {
                magic: HEADER_MAGIC,
                version: HEADER_VERSION,
                length: (HEADER_SIZE * 2 + md_buf.len()) as u32,
                md_offset: HEADER_SIZE as u32,
                md_length: md_buf.len() as u32,
                md_checksum: Self::checksum(&md_buf),
            };

            let mut buf = hdr.to_vec();
            buf.extend_from_slice(&md_buf);
            buf.extend_from_slice(&hdr.to_vec());

            Ok(Self {
                buf,
            })
        }

        /// Reads metadata from the device's metadata partition.
        pub(crate) async fn read(
            hdl: &dyn BlockDeviceHandle,
        ) -> Result<Metadata, Error> {
            // Calculate partition sizes.
            let parts = Partitions::calculate_for_device(hdl.get_device())
                .ok_or_else(|| Error::BadPartitions {})?;

            // Read the first header only.
            let mut dma =
                hdl.dma_malloc_adjusted(HEADER_SIZE as u64).map_err(|e| {
                    Error::DmaMalloc {
                        source: e,
                    }
                })?;

            hdl.read_at(parts.meta_start_offset(), &mut dma)
                .await
                .map_err(|e| Error::LoadFailed {
                    source: e,
                })?;

            // Decode the header.
            let hdr = Header::from_slice(&dma.as_slice()[0 .. HEADER_SIZE])?;

            // Read the entire metadata chunk.
            let mut dma =
                hdl.dma_malloc_adjusted(hdr.length as u64).map_err(|e| {
                    Error::DmaMalloc {
                        source: e,
                    }
                })?;

            hdl.read_at(parts.meta_start_offset(), &mut dma)
                .await
                .map_err(|e| Error::LoadFailed {
                    source: e,
                })
                .map(|_| ())?;

            // Check the second header.
            let start = hdr.length as usize - HEADER_SIZE;
            let end = hdr.length as usize;
            let end_hdr = Header::from_slice(&dma.as_slice()[start .. end])?;

            if hdr != end_hdr {
                return Err(Error::InvalidHeader {});
            }

            // Decode metadata.
            let start = hdr.md_offset as usize;
            let end = start + hdr.md_length as usize;
            let md_buf = &dma.as_slice()[start .. end];

            if Self::checksum(&md_buf) != hdr.md_checksum {
                return Err(Error::ChecksumMistach {});
            }

            Metadata::decode(&md_buf)
        }

        /// TODO
        pub(crate) async fn write(
            &self,
            hdl: &dyn BlockDeviceHandle,
        ) -> Result<(), Error> {
            let parts = Partitions::calculate_for_device(hdl.get_device())
                .ok_or_else(|| Error::BadPartitions {})?;

            if self.buf.len() as u64 > parts.meta_size() {
                return Err(Error::NoSpace {});
            }

            // Allocate a DMA buffer for the metadata.
            let dma =
                hdl.dma_buf_from(&self.buf).map_err(|e| Error::DmaMalloc {
                    source: e,
                })?;

            hdl.write_at(parts.meta_start_offset(), &dma)
                .await
                .map_err(|e| Error::SaveFailed {
                    source: e,
                })
                .map(|_| ())
        }

        /// TODO
        fn checksum(_buf: &[u8]) -> u64 {
            0
        }
    }

    /// TODO
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum Metadata {
        None,
        V1(Nexus),
    }

    impl Metadata {
        pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
            rmp_serde::to_vec(self).map_err(|e| Error::EncodeFailed {
                source: e,
            })
        }

        pub(crate) fn decode(src: &[u8]) -> Result<Self, Error> {
            rmp_serde::from_slice(src).map_err(|e| Error::DecodeFailed {
                source: e,
            })
        }
    }

    /// TODO
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct Nexus {
        pub timestamp: DateTime<Utc>,
        pub state: State,
        pub name: String,
        pub nexus_uuid: String,
        pub bdev_uuid: String,
        pub requested_size: u64,
        pub children: Vec<Child>,
    }

    /// TODO
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Child {
        pub timestamp: DateTime<Utc>,
        pub state: State,
        pub name: String,
        pub device_name: Option<String>,
        pub device_uuid: Option<String>,
    }

    /// TODO
    #[derive(Debug, Serialize, Deserialize)]
    #[repr(u8)]
    pub enum State {
        Open,
        Closed,
        Faulted,
        Dirty,
    }
}

impl From<NexusState> for metadata::State {
    fn from(src: NexusState) -> Self {
        match src {
            NexusState::Init => Self::Dirty,
            NexusState::Closed => Self::Closed,
            NexusState::Open => Self::Open,
            NexusState::Reconfiguring => Self::Dirty,
            NexusState::ShuttingDown => Self::Dirty,
            NexusState::Shutdown => Self::Closed,
        }
    }
}

impl From<ChildState> for metadata::State {
    fn from(src: ChildState) -> Self {
        match src {
            ChildState::Init => Self::Dirty,
            ChildState::ConfigInvalid => Self::Dirty,
            ChildState::Open => Self::Open,
            ChildState::Destroying => Self::Dirty,
            ChildState::Closed => Self::Closed,
            ChildState::Faulted(_) => Self::Faulted,
        }
    }
}

/// TODO
#[derive(Debug, Serialize)]
struct MetadataCopy {
    child: String,
    meta: metadata::Metadata,
}

impl<'n> Nexus<'n> {
    /// TODO
    /// [1] Read metadata copies from each nexus child device.
    /// [2] Filter out metadata copies with different nexus UUIDs.
    /// [3] Select the copy with the latest timestamp.
    /// [4] Use child state to validate if a child can be used.
    ///     No meta for child -> ignore
    ///     Open -> keep
    ///     !Open -> Close and remove
    pub(crate) async fn validate_children_metadata(&self) {
        assert_eq!(self.state(), NexusState::Init);

        let md_cur = self.current_metadata();

        // println!("---- [C] ---------------------");
        // println!("{m}", m = serde_json::to_string_pretty(&md_cur).unwrap());
        // println!("------------------------------");

        let md_copies = self.read_metadata().await;

        // println!("---- [1] ---------------------");
        // println!("{m}", m =
        // serde_json::to_string_pretty(&md_copies).unwrap());
        // println!("------------------------------");

        let md_latest = md_copies
            .into_iter()
            .filter_map(
                |MetadataCopy {
                     child,
                     meta,
                 }| match meta {
                    metadata::Metadata::V1(md_nex) => {
                        if md_nex.nexus_uuid != md_cur.nexus_uuid
                            || md_nex.bdev_uuid != md_cur.bdev_uuid
                        {
                            warn!(
                                "{self:?}: child '{child}' has metadata for \
                            a different nexus: '{u}'",
                                u = md_nex.nexus_uuid
                            );
                            None
                        } else {
                            Some(md_nex)
                        }
                    }
                    metadata::Metadata::None => {
                        debug!(
                            "{self:?}: child '{child}' has no metadata copy",
                        );
                        None
                    }
                },
            )
            .max_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let Some(md_nex) = md_latest else {
            debug!("{self:?}: no metadata found on child devices");
            return;
        };

        for md_child in md_nex.children {
            if matches!(md_child.state, metadata::State::Open) {
                continue;
            }

            let Some(child) = self.lookup_child(&md_child.name) else {
                debug!("{self:?}: child '{name}' from metadata not found", name = md_child.name);
                continue;
            };

            warn!("{self:?}: nexus metadata indicates that child '{name}' is faulted; closing and removing it", name = md_child.name);

            if let Err(e) = child.close().await {
                error!("{child:?}: child failed to close: {e}");
            }
        }
    }

    /// TODO
    async fn read_metadata(&self) -> Vec<MetadataCopy> {
        debug!(
            "{self:?}: reading nexus metadata copies from all child devices"
        );

        let mut md_copies = Vec::new();

        for child in self.children_iter() {
            md_copies.push(MetadataCopy {
                child: child.name.clone(),
                meta: child.read_metadata().await,
            });
        }

        md_copies
    }

    /// TODO
    pub(crate) async fn update_metadata(&self) {
        debug!("{self:?}: updating nexus metadata on all child devices");
        if let Err(e) = self.write_metadata().await {
            error!("{self:?}: updating nexus metadata failed: {e}");
        }
    }

    /// Captures and saves current nexus metadata to all child devices.
    async fn write_metadata(&self) -> Result<(), Error> {
        let md = metadata::Metadata::V1(self.current_metadata());

        let md_buf = metadata::Buffer::from_meta(md).map_err(|e| {
            Error::MetadataError {
                source: e,
                name: self.name.clone(),
            }
        })?;

        for child in self.children_iter() {
            child.write_metadata(&md_buf).await;
        }

        Ok(())
    }

    /// TODO
    fn current_metadata(&self) -> metadata::Nexus {
        let timestamp = Utc::now();

        metadata::Nexus {
            timestamp,
            state: self.state().into(),
            name: self.nexus_name().to_string(),
            nexus_uuid: self.uuid().to_string(),
            bdev_uuid: unsafe { self.bdev().uuid().to_string() },
            requested_size: self.req_size(),
            children: self
                .children_iter()
                .map(|child| metadata::Child {
                    timestamp,
                    state: child.state().into(),
                    name: child.name.clone(),
                    device_name: child.device.as_ref().map(|d| d.device_name()),
                    device_uuid: child
                        .device
                        .as_ref()
                        .map(|d| d.uuid().to_string()),
                })
                .collect(),
        }
    }
}

impl<'c> NexusChild<'c> {
    /// Reads a copy of nexus metadata residing on this child's device.
    /// Returns `Metadata::None` in the case metadata is not present, or cannot
    /// be read or decoded.
    async fn read_metadata(&self) -> metadata::Metadata {
        if self.device.is_none() {
            warn!(
                "{self:?}: reading a copy of nexus metadata: no device present"
            );
            return metadata::Metadata::None;
        }

        match self.get_io_handle_nonblock().await {
            Ok(hdl) => match metadata::Buffer::read(hdl.as_ref()).await {
                Ok(md) => md,
                Err(e) => {
                    match e {
                        metadata::Error::InvalidHeader {} => {
                            debug!("{self:?}: reading a copy of nexus metadata failed: {e}");
                        }
                        _ => {
                            warn!("{self:?}: reading a copy of nexus metadata failed: {e}");
                        }
                    };
                    metadata::Metadata::None
                }
            },
            Err(e) => {
                // TODO: not a "hard" error for a failed child
                warn!("{self:?}: reading a copy of nexus metadata failed: {e}");
                metadata::Metadata::None
            }
        }
    }

    /// TODO
    async fn write_metadata(&self, md_buf: &metadata::Buffer) {
        if self.device.is_none() {
            error!(
                "{self:?}: writing a copy of nexus metadata: no device present"
            );
            return;
        }

        if !self.is_healthy() {
            error!("{self:?}: writing a copy of nexus metadata: not healthy");
            return;
        }

        match self.get_io_handle_nonblock().await {
            Ok(hdl) => {
                if let Err(e) = md_buf.write(hdl.as_ref()).await {
                    error!("{self:?}: writing a copy of nexus metadata: {e}");
                } else {
                    debug!(
                        "{self:?}: successfully wrote a copy of nexus metadata"
                    );
                }
            }
            Err(e) => {
                error!("{self:?}: writing a copy of nexus metadata: {e}");
            }
        }
    }
}
