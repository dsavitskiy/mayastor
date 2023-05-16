use std::{
    cmp::min,
    fmt::{Display, Formatter},
};

use super::BlockDevice;

/// GPT partition table size in bytes.
pub const GPT_TABLE_SIZE: u64 = 128 * 128;

/// Offset for reserved metadata partition, in bytes.
pub const METADATA_RESERVATION_OFFSET: u64 = 1024 * 1024;

/// Reserved size for metadata partition, in bytes.
pub const METADATA_RESERVATION_SIZE: u64 = 4 * 1024 * 1024;

/// Start of data partition, in bytes.
pub const DATA_PARTITION_OFFSET: u64 =
    METADATA_RESERVATION_OFFSET + METADATA_RESERVATION_SIZE;

/// Converts an offset in bytes into offset in number of aligned blocks for
/// the given block size.
pub fn bytes_to_alinged_blocks(size: u64, block_len: u64) -> u64 {
    let blocks = size / block_len;
    match size % block_len {
        0 => blocks,
        _ => blocks + 1,
    }
}

/// I/O engine partitions.
///
/// Device layout:
///
/// 0     ───── reserved for protective MBR
/// 1     ───── reserved for primary GPT header
/// 2     ──┐
///         ├── reserved for GPT entries
/// 33    ──┘
/// 34    ──┐
///         ├── unused
/// 2047  ──┘
/// 2048  ──┐
///         ├── 4M reserved for metadata
/// 10239 ──┘
/// 10240 ──┐
///         ├── available for user data
/// N-34  ──┘
/// N-33  ──┐
///         ├── reserved for the copy of GPT entries
/// N-2   ──┘
/// N-1   ───── last device block, reserved for secondary GPT header
pub struct Partitions {
    /// Total number of blocks.
    num_blocks: u64,
    /// Block size in bytes.
    block_len: u64,
    /// First and last blocks of the metadata partition.
    meta: (u64, u64),
    /// First and last blocks of the user-data partition.
    data: (u64, u64),
    /// First and last blocks of the usable LBA range.
    lba: (u64, u64),
}

impl Display for Partitions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{data} x {blen} (meta={meta}, full={full})",
            meta = self.meta_blocks(),
            data = self.data_blocks(),
            full = self.num_blocks(),
            blen = self.block_len(),
        )
    }
}

impl Partitions {
    /// Calculates offsets of the first and last blocks of the device
    /// partitions for the given device size and block size.
    ///
    /// # Arguments
    /// * `req_size`: Requested data partition size in bytes. If zero, use the
    ///   entire block range.
    /// * `num_blocks`: Size of the device in blocks.
    /// * `block_len`: Block size in bytes.
    ///
    /// # Return
    /// `None` if the device is too small to accommodate the required data
    /// layout.
    pub fn calculate(
        req_size: u64,
        num_blocks: u64,
        block_len: u64,
    ) -> Option<Self> {
        let req_size = if req_size == 0 {
            num_blocks * block_len
        } else {
            req_size
        };

        // Number of blocks occupied by GPT tables.
        let gpt_blocks = bytes_to_alinged_blocks(GPT_TABLE_SIZE, block_len);

        // First block of metadata reservation.
        let lba_start =
            bytes_to_alinged_blocks(METADATA_RESERVATION_OFFSET, block_len);

        // Last usable device block.
        let lba_end = num_blocks - gpt_blocks - 2;

        // Blocks used by metadata reservation.
        let meta_blocks =
            bytes_to_alinged_blocks(METADATA_RESERVATION_SIZE, block_len);

        // First block of data.
        let data_start = lba_start + meta_blocks;
        if data_start > lba_end {
            // Device is too small to accommodate Metadata reservation.
            return None;
        }

        // Number of requested data blocks.
        let req_blocks = bytes_to_alinged_blocks(req_size, block_len);

        // Last data block.
        let data_end = min(data_start + req_blocks - 1, lba_end);

        Some(Self {
            num_blocks,
            block_len,
            meta: (lba_start, data_start - 1),
            data: (data_start, data_end),
            lba: (lba_start, lba_end),
        })
    }

    /// Calculates offsets of the first and last blocks of the device
    /// partitions for the given device.
    pub fn calculate_for_device(dev: &dyn BlockDevice) -> Option<Self> {
        Self::calculate(0, dev.num_blocks(), dev.block_len())
    }

    /// Returns the total number of blocks.
    pub fn num_blocks(&self) -> u64 {
        self.num_blocks
    }

    /// Returns the block size in bytes.
    pub fn block_len(&self) -> u64 {
        self.block_len
    }

    /// Returns the first block of the metadata partition.
    pub fn meta_start_blk(&self) -> u64 {
        self.meta.0
    }

    /// Returns the offset of the metadata partition start in bytes.
    pub fn meta_start_offset(&self) -> u64 {
        self.meta.0 * self.block_len
    }

    /// Returns the last block of the metadata partition.
    pub fn meta_end_blk(&self) -> u64 {
        self.meta.1
    }

    /// Returns the total number of blocks of the metadata partition.
    pub fn meta_blocks(&self) -> u64 {
        self.meta_end_blk() - self.meta_start_blk() + 1
    }

    /// Returns the size of the metadata partition in bytes.
    pub fn meta_size(&self) -> u64 {
        self.meta_blocks() * self.block_len
    }

    /// Returns the first block of the user-data partition.
    pub fn data_start_blk(&self) -> u64 {
        self.data.0
    }

    /// Returns the offset of the user-data partition start in bytes.
    pub fn data_start_offset(&self) -> u64 {
        self.data.0 * self.block_len
    }

    /// Returns the last block of the user-data partition.
    pub fn data_end_blk(&self) -> u64 {
        self.data.1
    }

    /// Returns the total number of blocks of the user-data partition.
    pub fn data_blocks(&self) -> u64 {
        self.data_end_blk() - self.data_start_blk() + 1
    }

    /// Returns the size of the user-data partition in bytes.
    pub fn data_size(&self) -> u64 {
        self.data_blocks() * self.block_len
    }

    /// Returns the first block of the usable LBA range.
    pub fn lba_start_blk(&self) -> u64 {
        self.lba.0
    }

    /// Returns the last block of the usable LBA range.
    pub fn lba_end_blk(&self) -> u64 {
        self.lba.1
    }

    /// Returns the total number of blocks of the usable LBA range.
    pub fn lba_blocks(&self) -> u64 {
        self.lba_end_blk() - self.lba_start_blk() + 1
    }
}
