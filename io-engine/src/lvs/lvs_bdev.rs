use std::{ffi::CStr, os::raw::c_char};

use spdk_rs::libspdk::{
    lvol_store_bdev,
    spdk_bs_free_cluster_count,
    spdk_bs_get_cluster_size,
    spdk_bs_total_data_cluster_count,
    spdk_lvol_store,
    vbdev_lvol_store_first,
    vbdev_lvol_store_next,
};

use crate::core::{Bdev, UntypedBdev};

/// Structure representing a pool which comprises lvol store and
/// underlying bdev.
///
/// Note about safety: The structure wraps raw C pointers from SPDK.
/// It is safe to use only in synchronous context. If you keep Pool for
/// longer than that then something else can run on reactor_0 in between,
/// which may destroy the pool and invalidate the pointers!
pub struct LvsBdev {
    inner: *mut lvol_store_bdev,
}

impl LvsBdev {
    /// An easy converter from a raw pointer to Pool object
    unsafe fn from_ptr(ptr: *mut lvol_store_bdev) -> LvsBdev {
        LvsBdev {
            inner: ptr,
        }
    }

    /// TODO
    #[inline(always)]
    fn lvol_store(&self) -> &spdk_lvol_store {
        unsafe { &*((*self.inner).lvs) }
    }

    /// Get name of the pool.
    pub fn get_name(&self) -> &str {
        unsafe {
            let lvs = self.lvol_store();
            CStr::from_ptr(&lvs.name as *const c_char).to_str().unwrap()
        }
    }

    /// Get base bdev for the pool (in our case AIO or uring bdev).
    pub fn get_base_bdev(&self) -> UntypedBdev {
        unsafe { Bdev::checked_from_ptr((*self.inner).bdev).unwrap() }
    }

    /// Get capacity of the pool in bytes.
    pub fn get_capacity(&self) -> u64 {
        unsafe {
            let lvs = self.lvol_store();
            let cluster_size = spdk_bs_get_cluster_size(lvs.blobstore);
            let total_clusters =
                spdk_bs_total_data_cluster_count(lvs.blobstore);
            total_clusters * cluster_size
        }
    }

    /// Get free space in the pool in bytes.
    pub fn get_free(&self) -> u64 {
        unsafe {
            let lvs = self.lvol_store();
            let cluster_size = spdk_bs_get_cluster_size(lvs.blobstore);
            spdk_bs_free_cluster_count(lvs.blobstore) * cluster_size
        }
    }
}

/// Iterator over available storage pools.
#[derive(Default)]
pub struct LvsBdevIter {
    lvs_bdev_ptr: Option<*mut lvol_store_bdev>,
}

impl LvsBdevIter {
    pub fn new() -> Self {
        Self {
            lvs_bdev_ptr: None,
        }
    }
}

impl Iterator for LvsBdevIter {
    type Item = LvsBdev;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lvs_bdev_ptr {
            Some(current) => {
                if current.is_null() {
                    return None;
                }
                self.lvs_bdev_ptr =
                    Some(unsafe { vbdev_lvol_store_next(current) });
                Some(unsafe { LvsBdev::from_ptr(current) })
            }
            None => {
                let current = unsafe { vbdev_lvol_store_first() };
                if current.is_null() {
                    self.lvs_bdev_ptr = Some(current);
                    return None;
                }
                self.lvs_bdev_ptr =
                    Some(unsafe { vbdev_lvol_store_next(current) });
                Some(unsafe { LvsBdev::from_ptr(current) })
            }
        }
    }
}
