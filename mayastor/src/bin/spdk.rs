extern crate libc;

use std::{
    env,
    ffi::{c_void, CString},
    io::{Error, ErrorKind},
    iter::Iterator,
    os::raw::{c_char, c_int},
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
    vec::Vec,
};

use futures::FutureExt;
use log::debug;
use mayastor::{
    core::{device_monitor, runtime, Mthread},
    delay,
    grpc,
    logger,
    persistent_store::PersistentStore,
    subsys::{self, Config, NvmfSubsystem, NvmfTarget, Registration},
};
use spdk_sys::{
    spdk_app_fini,
    spdk_app_opts,
    spdk_app_opts_init,
    spdk_app_parse_args,
    spdk_app_start,
    spdk_app_stop,
};
use tracing::info;
mayastor::CPS_INIT!();
fn main() -> Result<(), std::io::Error> {
    logger::init("TRACE");

    runtime::mayastor_start("MayaStor", std::env::args().collect(), || {


        info!("MayaStor started ");
        println!("wassup?");
    });

    Ok(())
}
//    let args = env::args()
//        .map(|arg| CString::new(arg).unwrap())
//        .collect::<Vec<CString>>();
//    let mut c_args = args
//        .iter()
//        .map(|arg| arg.as_ptr())
//        .collect::<Vec<*const c_char>>();
//    c_args.push(std::ptr::null());
//
//    let mut opts: spdk_app_opts = Default::default();
//
//    logger::init("TRACE");
//    unsafe {
//        spdk_app_opts_init(
//            &mut opts as *mut spdk_app_opts,
//            std::mem::size_of::<spdk_app_opts>() as u64,
//        );
//
//        if spdk_app_parse_args(
//            (c_args.len() as c_int) - 1,
//            c_args.as_ptr() as *mut *mut c_char,
//            &mut opts,
//            null_mut(), // extra short options i.e. "f:S:"
//            null_mut(), // extra long options
//            None,       // extra options parse callback
//            None,       // usage
//        ) != spdk_sys::SPDK_APP_PARSE_ARGS_SUCCESS
//        {
//            return Err(Error::new(
//                ErrorKind::Other,
//                "Parsing arguments failed",
//            ));
//        }
//    }
//
//    opts.name = CString::new("spdk".to_owned()).unwrap().into_raw();
//    opts.shutdown_cb = Some(spdk_shutdown_cb);
//
//    let rc = unsafe {
//        let rc = spdk_app_start(&mut opts, Some(app_start_cb), null_mut());
//        // this will remove shm file in /dev/shm and do other cleanups
//        spdk_app_fini();
//        rc
//    };
//
//    if rc != 0 {
//        Err(Error::new(
//            ErrorKind::Other,
//            format!("spdk failed with error {}", rc),
//        ))
//    } else {
//        Ok(())
//    }
//}

extern "C" fn spdk_shutdown_cb() {
    runtime::mayastor_fini();
    unsafe { spdk_app_stop(0) };
}
fn start_tokio_runtime() {
    let rpc_address = "192.168.1.4";
    let node_name = "mayastor";
    let persistent_store_endpoint = "";

    Mthread::spawn_unaffinitized(move || {
        runtime::block_on(async move {
            let mut futures = Vec::new();

            //PersistentStore::init(persistent_store_endpoint).await;
            runtime::spawn(device_monitor());

            futures.push(
                grpc::MayastorGrpcServer::run(
                    "192.168.1.4:10124".parse().unwrap(),
                    rpc_address.into(),
                )
                .boxed(),
            );

            futures::future::try_join_all(futures)
                .await
                .expect_err("runtime exited in the abnormal state");
        });
    });
}

extern "C" fn app_start_cb(_arg: *mut c_void) {
    // use in cases when you want to burn less cpu and speed does not matter
    if let Some(_key) = env::var_os("MAYASTOR_DELAY") {
        delay::register();
    }

    runtime::mayastor_init();
    runtime::spawn_local(async {
        mayastor::subsys::NvmfTarget::get().start().await;
    });
    start_tokio_runtime();
}
