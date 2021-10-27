use std::{
    cell::RefCell,
    ffi::{c_void, CString},
    pin::Pin,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

use futures::{
    channel::oneshot::{self, Receiver},
    Future,
};
use lazy_static::__Deref;
use nix::errno::Errno;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use spdk_sys::{
    nvmf_tgt_accept,
    spdk_env_get_core_count,
    spdk_nvmf_listen_opts,
    spdk_nvmf_listen_opts_init,
    spdk_nvmf_poll_group,
    spdk_nvmf_poll_group_destroy,
    spdk_nvmf_subsystem_create,
    spdk_nvmf_subsystem_set_mn,
    spdk_nvmf_target_opts,
    spdk_nvmf_tgt,
    spdk_nvmf_tgt_add_transport,
    spdk_nvmf_tgt_create,
    spdk_nvmf_tgt_destroy,
    spdk_nvmf_tgt_listen_ext,
    spdk_nvmf_tgt_stop_listen,
    spdk_nvmf_transport_create,
    spdk_poller,
    spdk_poller_register_named,
    spdk_poller_unregister,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
    SPDK_NVMF_DISCOVERY_NQN,
    SPDK_NVMF_SUBTYPE_DISCOVERY,
};
use tokio::runtime;

use crate::{
    core::{Cores, Mthread, Reactor, Reactors},
    ffihelper::{
        cb_arg,
        done_errno_cb,
        AsStr,
        ErrnoResult,
        FfiResult,
        IntoCString,
    },
    subsys::{
        nvmf::{
            poll_groups::PollGroup,
            subsystem::NvmfSubsystem,
            transport,
            transport::{get_ipv4_address, TransportId},
            Error,
            NVMF_PGS,
        },
        Config,
    },
};

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) static NVMF_TGT: OnceCell<Target> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct Target {
    inner: Inner,
}

///
/// # Safety
///
/// The pointer is allocated by us and never freed during the lifetime of the
/// binary. The target itself is protected internally by a mutex.
#[derive(Debug, Clone)]
struct Inner(NonNull<spdk_nvmf_tgt>);

impl std::ops::Deref for Inner {
    type Target = spdk_nvmf_tgt;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl std::ops::DerefMut for Inner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

impl Default for Target {
    fn default() -> Self {
        Target::new()
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        if !self.0 {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl Target {
    fn new() -> Self {
        let cfg = Config::get();
        let opts: Box<spdk_nvmf_target_opts> =
            cfg.nvmf_tcp_tgt_conf.clone().into();

        NVMF_PGS.get_or_init(|| Mutex::new(Vec::new()));

        dbg!(Mthread::get_init());
        let tgt = unsafe { spdk_nvmf_tgt_create(&*opts as *const _ as *mut _) };
        Self {
            inner: Inner(NonNull::new(tgt).unwrap()),
        }
    }

    pub fn get() -> &'static Target {
        NVMF_TGT.get_or_init(|| Self::default())
    }

    pub async fn start(&self) {
        self.create_poll_groups().await;
        self.enable_tcp().await;
        self.listen();

        info!(ptr= ?self.as_ptr(), "target at address with {:?} PGs", NVMF_PGS.get().map(|p| p.lock().len()));
    }

    async fn create_poll_groups(&self) {
        let groups = Cores::count().into_iter().map(|c| {
                let (s,r) = oneshot::channel::<i32>();
                Mthread::new(format!("mayastor_nvmf_tcp_pg_core_{}", c), c)
                    .expect("failed to allocate thread")
                    .msg(|| {
                            let thread = Mthread::current().unwrap();
                            let pg = PollGroup::new(thread);
                            NVMF_PGS.get().map(|p| p.lock().push(pg));
                            debug!(core = ?Cores::current(), thread = ?thread, "PG created");
                            s.send(0).expect("failure during poll group creation");
            });
                r
        }).collect::<Vec<_>>();

        futures::future::join_all(groups).await;
    }

    pub fn as_ptr(&self) -> *mut spdk_nvmf_tgt {
        self.inner.0.as_ptr()
    }

    pub async fn destroy_async() {
        let mut t = NVMF_PGS.get().unwrap().lock();
        while let Some(pg) = t.pop() {
            pg.destroy().await;
        }

        NVMF_TGT.get().map(|t| t.destroy());
    }

    async fn enable_tcp(&self) {
        let cfg = Config::get();
        let mut opts = cfg.nvmf_tcp_tgt_conf.opts.into();
        let transport = unsafe {
            spdk_nvmf_transport_create(
                "TCP".to_string().into_cstring().into_raw(),
                &mut opts,
            )
        };

        if transport.is_null() {
            panic!()
        }

        let (s, r) = futures::channel::oneshot::channel::<ErrnoResult<()>>();
        unsafe {
            spdk_nvmf_tgt_add_transport(
                self.as_ptr(),
                transport,
                Some(done_errno_cb),
                cb_arg(s),
            )
        };

        let _result = r.await.unwrap();
        debug!("Added TCP nvmf transport");
    }

    fn listen(&self) {
        let cfg = Config::get();
        let trid_nexus = TransportId::new(cfg.nexus_opts.nvmf_nexus_port);
        let mut opts = spdk_nvmf_listen_opts::default();

        unsafe {
            spdk_nvmf_listen_opts_init(
                &mut opts,
                std::mem::size_of::<spdk_nvmf_listen_opts>() as u64,
            );
        }

        let rc = unsafe {
            spdk_nvmf_tgt_listen_ext(
                self.as_ptr(),
                trid_nexus.as_ptr(),
                &mut opts,
            )
        };

        if rc != 0 {
            panic!("failed to create target");
        }

        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);
        let rc = unsafe {
            spdk_nvmf_tgt_listen_ext(
                self.as_ptr(),
                trid_replica.as_ptr(),
                &mut opts,
            )
        };

        if rc != 0 {
            panic!("failed to create target");
        }
        info!(
            "nvmf target listening on {}:({},{})",
            get_ipv4_address().unwrap(),
            trid_nexus.trsvcid.as_str(),
            trid_replica.trsvcid.as_str(),
        );
    }

    fn stop_listen() {
        let tgt = NVMF_TGT.get().unwrap().as_ptr();
        let cfg = Config::get();
        let trid_nexus = TransportId::new(cfg.nexus_opts.nvmf_nexus_port);
        let rc = unsafe { spdk_nvmf_tgt_stop_listen(tgt, trid_nexus.as_ptr()) };

        if rc != 0 {
            error!("failed to listen for target");
        }

        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);
        let rc =
            unsafe { spdk_nvmf_tgt_stop_listen(tgt, trid_replica.as_ptr()) };

        if rc != 0 {
            panic!("failed to stop listen for target");
        }

        info!("NVMe-oF target stopped listening");
    }

    fn destroy(&self) {
        extern "C" fn destroy_cb(_arg: *mut c_void, _status: i32) {
            info!("NVMe-oF target shutdown completed");
            unsafe {
                spdk_subsystem_fini_next();
            }
        }

        unsafe {
            spdk_nvmf_tgt_destroy(
                NVMF_TGT.get().unwrap().as_ptr(),
                Some(destroy_cb),
                std::ptr::null_mut(),
            )
        };
    }

    /// stop all subsystems on this target we are borrowed here
    pub fn stop_subsystems(&self, mut cb: impl FnMut() + 'static) {
        crate::core::runtime::spawn_local(async move  {
        dbg!(NvmfSubsystem::stop_all().await);
        dbg!(Self::stop_listen());
        dbg!(Self::destroy_async().await);
        cb();
        debug!("all subsystems stopped!");
        });
    }
}
