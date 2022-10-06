pub mod common;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use futures::future::try_join_all;
use io_engine_tests::{
    compose::{rpc::v1::SharedRpcHandle, ContainerSpec},
    file_io::wipe_device,
    nexus::test_write_to_nexus,
    rand::random_sleep,
};
use std::time::Duration;

const MAX_NEXUSES: u64 = 10;
const MAX_BACK_TASKS: u64 = 20;
// const POOL_SIZE: u64 = 100 * (MAX_NEXUSES + MAX_BACK_TASKS);
const REPL_SIZE: u64 = 100;

fn tids() -> String {
    format!("{:?}", std::thread::current().id())
}

async fn repl_cd(
    mut repl: ReplicaBuilder,
    i: u64,
    k: u64,
    ms_nex: SharedRpcHandle,
) {
    if let Err(e) = repl.create().await {
        println!(
            "[{}] repl {} destroy err: {}",
            tids(),
            repl.name(),
            e.to_string()
        );
        return;
    }

    if let Err(e) = repl.share().await {
        println!(
            "[{}] repl {} share err: {}",
            tids(),
            repl.name(),
            e.to_string()
        );
        if let Err(e) = repl.destroy().await {
            println!(
                "[{}] repl {} destroy #2 err: {}",
                tids(),
                repl.name(),
                e.to_string()
            );
        }
        return;
    }

    println!("[{}] repl {} created", tids(), repl.name());

    // tokio::time::sleep(Duration::from_millis(5)).await;
    random_sleep(1, 5).await;

    let mut nex = NexusBuilder::new(ms_nex.clone())
        .with_name(&format!("tmp_nexus_{}_{}", i, k))
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl);

    nex.create().await.unwrap();
    nex.publish().await.unwrap();
    println!("[{}] repl {} create/publish ok", tids(), nex.name());

    random_sleep(1, 5).await;

    nex.destroy().await.unwrap();

    random_sleep(1, 5).await;

    if let Err(e) = repl.unshare().await {
        println!(
            "[{}] repl {} unshare err: {}",
            tids(),
            repl.name(),
            e.to_string()
        );
    }

    if let Err(e) = repl.destroy().await {
        println!(
            "[{}] repl {} destroy err: {}",
            tids(),
            repl.name(),
            e.to_string()
        );
    }
    println!("[{}] repl {} destroy ok", tids(), repl.name());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// 1. Create a pool
// 2. Create N volumes
// 3. Create N nexues
async fn concurrent_io() {
    common::composer_init();

    const NVME_0_PATH: &str = "/dev/nvme0n1";

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2,3,4,5,6",
                "-Fcompact,nodate,host,color",
            ]),
        )
        .add_container_spec(
            ContainerSpec::from_binary(
                "ms_repl_0",
                Binary::from_dbg("io-engine")
                    .with_args(vec![
                        "-l",
                        "7,8,9,10,12,12",
                        "-Fcompact,nodate,host,color",
                    ])
                    .with_privileged(Some(true)),
            )
            .with_direct_bind(NVME_0_PATH),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_repl_0 = conn.grpc_handle_shared("ms_repl_0").await.unwrap();

    //
    let mut pool_0 = PoolBuilder::new(ms_repl_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_device(NVME_0_PATH);
    // .with_malloc("mem0", POOL_SIZE);
    wipe_device(NVME_0_PATH).await.unwrap();
    pool_0.create().await.unwrap();

    //
    let mut replicas = Vec::new();
    let mut io_tasks = Vec::new();

    // Create N replicas.
    for i in 0 .. MAX_NEXUSES {
        let mut repl = ReplicaBuilder::new(ms_repl_0.clone())
            .with_pool(&pool_0)
            .with_name(&format!("repl_{}", i))
            .with_new_uuid()
            .with_size_mb(REPL_SIZE)
            .with_thin(false);

        repl.create().await.unwrap();
        repl.share().await.unwrap();

        let mut nex = NexusBuilder::new(ms_nex.clone())
            .with_name(&format!("nexus_{}", i))
            .with_new_uuid()
            .with_size_mb(REPL_SIZE)
            .with_replica(&repl);

        nex.create().await.unwrap();
        nex.publish().await.unwrap();

        replicas.push(repl);
        // nexuses.push(nex);

        println!("%%%% Nexus {} created", i);
        // let t = tokio::spawn(async move {
        //     for k in 0 .. 10 {
        //         println!("%%%% Nexus {} / Loop {}", i, k);
        //         tokio::time::sleep(Duration::from_millis(1)).await;
        //         test_write_to_nexus(&nex, 30, 1).await.unwrap();
        //     }
        // });
        // io_tasks.push(t);

        let h = tokio::spawn(async move {
            for j in 0 .. 1 {
                println!(
                    "[{}] ++++ {} test write @ nexus {} ...",
                    tids(),
                    j,
                    i
                );
                if let Err(e) = test_write_to_nexus(&nex, 20, 2).await {
                    println!(
                        "[{}] ++++ {} test write @ nexus {} error: {}",
                        tids(),
                        j,
                        i,
                        e.to_string()
                    );
                } else {
                    println!(
                        "[{}] ++++ {} test write @ nexus {} ",
                        tids(),
                        j,
                        i
                    );
                }
            }
        });
        io_tasks.push(h);
    }

    // Start M tasks creating and destroying additional replicas and nexuses in
    // parallel.
    for i in 0 .. MAX_BACK_TASKS {
        println!("new back task #{}...", i);

        let ms_repl_0 = ms_repl_0.clone();
        let pool_uuid = pool_0.uuid();
        let ms_nex_0 = ms_nex.clone();

        let h = tokio::spawn(async move {
            for k in 0 .. 10 {
                let repl = ReplicaBuilder::new(ms_repl_0.clone())
                    .with_pool_uuid(&pool_uuid)
                    .with_name(&format!("tmp_repl_{}_{}", i, k))
                    .with_new_uuid()
                    .with_size_mb(REPL_SIZE)
                    .with_thin(false);

                repl_cd(repl, i, k, ms_nex_0.clone()).await;
                // tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        io_tasks.push(h);
    }

    println!("--> waiting all tasks...");
    try_join_all(io_tasks).await.unwrap();
    println!("--> waiting all tasks done");

    panic!("qqq");
}
