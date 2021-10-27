//!
//! This allows us to send futures from within mayastor to the tokio
//! runtime to do whatever it needs to do. The tokio threads are
//! unaffinitized such that they do not run on any of our reactors.

use futures::Future;

/// Spawn a future on the tokio runtime.
pub fn spawn(f: impl Future<Output = ()> + Send + 'static) {
    tk::RUNTIME.spawn(f);
}

/// Spawn a future on the local runtime.
pub fn spawn_local(f: impl Future<Output = ()> + 'static) {
    ms::Runtime::spawn_local(f)
}

/// Block on the given future until it completes
pub fn block_on(f: impl Future<Output = ()> + Send + 'static) {
    tk::RUNTIME.block_on(f);
}

/// Spawn a future that might block on a separate worker thread the
/// number of threads available is determined by max_blocking_threads
pub fn spawn_blocking<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tk::RUNTIME.spawn_blocking(f)
}

pub fn mayastor_start<T, F>(name: &str, args: Vec<T>, start_cb: F) -> i32
where
    T: Into<Vec<u8>>,
    F: FnOnce(),
{
    app::mayastor_start(name, args, start_cb)
}

/// Intialize the mayastor
pub fn mayastor_init() {
    ms::Runtime::init();
}
/// Intialize the mayastor
pub fn mayastor_fini() {
    ms::Runtime::fini();
}

mod tk {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::Future;
    use once_cell::sync::Lazy;
    use tokio::task::JoinHandle;

    pub struct Runtime {
        rt: tokio::runtime::Runtime,
    }

    pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("tokio-mgt-thread-{}", id)
            })
            .on_thread_start(|| {
                let handle = std::thread::current();
                crate::core::Mthread::unaffinitize();
                println!("{:?} unaffinitized", handle.name())
            })
            .on_thread_stop(|| {
                let handle = std::thread::current();
                crate::core::Mthread::unaffinitize();
                println!("{:?} shutdown", handle.name());
            })
            .enable_all()
            .worker_threads(4)
            .max_blocking_threads(2)
            .build()
            .unwrap();

        Runtime {
            rt,
        }
    });

    impl Runtime {
        pub fn block_on(&self, f: impl Future<Output = ()> + Send + 'static) {
            self.rt.block_on(f);
        }

        pub fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) {
            let handle = self.rt.handle().clone();
            handle.spawn(f);
        }

        pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
        where
            F: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            let handle = self.rt.handle().clone();
            handle.spawn_blocking(|| {
                crate::core::Mthread::unaffinitize();
                f()
            })
        }
    }
}

mod ms {
    use futures::{task::LocalSpawnExt, Future};
    use spdk_sys::{spdk_reactor, spdk_reactor_get};
    use std::{cell::RefCell, ptr::NonNull};

    use crate::{
        core::{poller::Poller, Cores, Mthread},
        subsys::Config,
    };

    // Initialization is dynamically performed on the first call to within a
    // thread.
    // Any runtime assertion (i.e does it run on the right core)
    // should be done prior to dispatching the thread.

    thread_local!(pub (self) static FIRST_CORE_RUNTIME: Runtime = Runtime::new());
    // The per thread local poller polls the future queue.
    thread_local!(pub (self) static FUTURE_POLLER: RefCell<Option<Poller<'static>>> = RefCell::new(None));

    #[derive(Debug)]
    /// This wraps over the native reactor of SPDK
    struct Reactor(NonNull<spdk_reactor>);

    #[derive(Debug)]
    pub struct Runtime {
        /// The core this runtime runs one, we only want to have one of this
        /// per application instance
        core: u32,
        /// A pool of futures that are either runnable or pending
        local: RefCell<futures::executor::LocalPool>,
        /// The reactor on this core
        reactor: Reactor,
    }

    impl Runtime {
        /// Construct a new runtime, can only run one the first core specified.
        fn new() -> Self {
            assert_eq!(Cores::current(), Cores::first());
            Self {
                core: Cores::current(),
                local: RefCell::new(futures::executor::LocalPool::new()),
                reactor: Reactor(
                    NonNull::new(unsafe { spdk_reactor_get(Cores::current()) })
                        .expect("core must be allocated"),
                ),
            }
        }

        /// Function called by the poller `[FUTURE_POLLER]` and runs all futures
        /// that can run until completion. If no progress can be made,
        /// (i.e futures are Pending) then we yield.
        fn tick(&self) -> i32 {
            self.local.borrow_mut().run_until_stalled();
            0
        }

        /// Helper function that gets the thread which is considered the init
        /// thread.
        fn thread() -> Mthread {
            Mthread::get_init()
        }

        /// Spawn a future on the first core {what, where}-ever the core maybe.
        /// Internally, this is achieved by putting a fn pointer with
        /// the rte ring buffers. Which in turn insert the future into
        /// the executor.
        pub fn spawn_local<F>(f: F)
        where
            F: Future<Output = ()> + 'static,
        {
            Self::thread().msg(|| {
                FIRST_CORE_RUNTIME.with(|runtime| {
                    assert_eq!(Mthread::current().is_some(), true);
                    assert_eq!(Cores::current(), Cores::first());

                    runtime
                        .local
                        .borrow_mut()
                        .spawner()
                        .spawn_local(f)
                        .unwrap();
                });
            });
        }

        /// Futures are not run until the executor is poked. The SPDK poller
        /// interface will poke the futures.
        pub fn init() {
            Config::get_or_init(Config::default);
            FUTURE_POLLER.with(|poller| {
                *poller.borrow_mut() = Some(
                    crate::core::poller::Builder::new()
                        .with_interval(0)
                        .with_name("futures_ticker")
                        .with_poll_fn(|| {
                            FIRST_CORE_RUNTIME.with(|runtime| runtime.tick());
                            0
                        })
                        .build(),
                );
            });
        }

        /// During shutdown we need to stop SPDK from polling.
        pub fn fini() {
            crate::subsys::NvmfTarget::get().stop_subsystems(Self::stop_poller);
        }

        fn stop_poller() {
            FUTURE_POLLER.with(|f| {
                let poller = f.borrow_mut().take().unwrap();
                dbg!(poller.stop());
            });
        }
    }
}

mod app {
    use std::{
        ffi::CString,
        os::raw::{c_char, c_int},
        ptr::null_mut,
    };

    use libc::c_void;
    use spdk_sys::{
        spdk_app_fini,
        spdk_app_opts,
        spdk_app_opts_init,
        spdk_app_parse_args,
        spdk_app_start,
        spdk_app_stop,
    };

    extern "C" fn usage() {
        // i.e. println!(" -f <path>                 save pid to this file");
    }

    pub fn mayastor_start<T, F>(
        name: &str,
        mut args: Vec<T>,
        start_cb: F,
    ) -> i32
    where
        T: Into<Vec<u8>>,
        F: FnOnce(),
    {
        // hand over command line args to spdk arg parser
        let args = args
            .drain(..)
            .map(|arg| CString::new(arg).unwrap())
            .collect::<Vec<CString>>();
        let mut c_args = args
            .iter()
            .map(|arg| arg.as_ptr())
            .collect::<Vec<*const c_char>>();
        c_args.push(std::ptr::null());

        let mut opts: spdk_app_opts = Default::default();

        unsafe {
            spdk_app_opts_init(
                &mut opts as *mut spdk_app_opts,
                std::mem::size_of::<spdk_app_opts>() as u64,
            );
            opts.rpc_addr =
                CString::new("/var/tmp/mayastor.sock").unwrap().into_raw();
            opts.print_level = spdk_sys::SPDK_LOG_INFO;
            if spdk_app_parse_args(
                (c_args.len() as c_int) - 1,
                c_args.as_ptr() as *mut *mut i8,
                &mut opts,
                null_mut(), // extra short options i.e. "f:S:"
                null_mut(), // extra long options
                None,       // extra options parse callback
                Some(usage),
            ) != spdk_sys::SPDK_APP_PARSE_ARGS_SUCCESS
            {
                return -1;
            }
        }

        opts.name = CString::new(name).unwrap().into_raw();
        opts.shutdown_cb = Some(mayastor_shutdown_cb);

        unsafe {
            let rc = spdk_app_start(
                &mut opts,
                Some(app_start_cb::<F>),
                // Double box to convert from fat to thin pointer
                Box::into_raw(Box::new(Box::new(start_cb))) as *mut c_void,
            );

            // this will remove shm file in /dev/shm and do other cleanups
            spdk_app_fini();

            rc
        }
    }
    fn start_tokio_runtime() {
        let rpc_address = "192.168.1.4";
        let node_name = "mayastor";
        let persistent_store_endpoint = "";

        crate::core::Mthread::spawn_unaffinitized(move || {
            crate::core::runtime::block_on(async move {
                let mut futures = Vec::new();

                //PersistentStore::init(persistent_store_endpoint).await;
                //crate::core::runtime::spawn(device_monitor());

                futures.push(futures::FutureExt::boxed(
                    crate::grpc::MayastorGrpcServer::run(
                        "192.168.1.4:10124".parse().unwrap(),
                        rpc_address.into(),
                    ),
                ));

                futures::future::try_join_all(futures)
                    .await
                    .expect_err("runtime exited in the abnormal state");
            });
        });
    }

    // spdk_all_start callback which starts the future executor and finally
    // calls
    /// user provided start callback.
    extern "C" fn app_start_cb<F>(arg1: *mut c_void)
    where
        F: FnOnce(),
    {
        crate::core::runtime::mayastor_init();
        crate::core::runtime::spawn_local(async {
            crate::subsys::NvmfTarget::get().start().await;
        });
        start_tokio_runtime();
    let cb: Box<Box<F>> = unsafe { Box::from_raw(arg1 as *mut Box<F>) };
    cb();
    }

    /// Cleanly exit from program.
    pub fn spdk_stop(rc: i32) {
        crate::core::runtime::mayastor_fini();
        unsafe { spdk_app_stop(rc) };
    }

    /// A callback called by spdk when it is shutting down.
    extern "C" fn mayastor_shutdown_cb() {
        spdk_stop(0);
    }
}
