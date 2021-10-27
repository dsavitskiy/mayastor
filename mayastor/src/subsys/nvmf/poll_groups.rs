use std::ptr::NonNull;

use futures::channel::oneshot;
use libc::c_void;
use spdk_sys::{
    spdk_nvmf_poll_group, spdk_nvmf_poll_group_create,
    spdk_nvmf_poll_group_destroy,
};

use crate::{
    core::{Cores, Mthread, Reactors},
    ffihelper::cb_arg,
    subsys::NvmfTarget,
};
#[derive(Clone, Debug)]
struct Pg(NonNull<spdk_nvmf_poll_group>);
/// # Safety
///
/// We never alias the Pg
unsafe impl Send for Pg {}

#[repr(C)]
#[derive(Clone, Debug)]
pub(crate) struct PollGroup {
    thread: Mthread,
    group: Pg,
    core: u32,
}

impl PollGroup {
    pub fn new(mt: Mthread) -> Self {
        Self {
            thread: mt,
            group: Pg(NonNull::new(unsafe {
                spdk_nvmf_poll_group_create(NvmfTarget::get().as_ptr())
            })
            .expect("failed to allocate PG")),
            core: Cores::current(),
        }
    }
    /// Poll groups are destroyed only during shutdown. Consumes Self whereafter
    /// the Pg is no longer usable.
    pub async fn destroy(self) {
        extern "C" fn pg_destroy_done(arg: *mut c_void, arg1: i32) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
            s.send(arg1).expect("receiver dropped");
        }

        let (s, r) = oneshot::channel::<i32>();

        self.thread.msg(|| {
            info!(core=?Cores::current(), pg = ?self, "destroying");
            unsafe {
                spdk_nvmf_poll_group_destroy(
                    self.group.0.as_ptr(),
                    Some(pg_destroy_done),
                    cb_arg(s),
                )
            };
        });

        r.await.unwrap();
    }
}
