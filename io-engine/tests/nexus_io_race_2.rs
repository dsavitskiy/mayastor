// #![allow(unused_variables)]
#![allow(unused_mut)]

pub mod common;
extern crate libnvme_rs;

use crossbeam::channel::unbounded;
use once_cell::sync::OnceCell;

use common::{
    bdev_io,
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason},
            GrpcConnect,
        },
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::DataSize,
    nexus::{test_write_to_nexus, NexusBuilder},
    nvme::{list_mayastor_nvme_devices, nvme_connect},
    pool::PoolBuilder,
    replica::{validate_replicas, ReplicaBuilder},
    MayastorTest,
};

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup, nexus_lookup_mut},
    constants::NVME_NQN_PREFIX,
    core::{MayastorCliArgs, Mthread, Protocol},
    lvs::Lvs,
    persistent_store::PersistentStore,
    pool_backend::PoolArgs,
};

use io_engine_tests::{
    fio::{Fio, FioJob},
    nexus::test_fio_to_nexus,
    reactor_poll,
};

static POOL_NAME: &str = "tpool";
static NEXUS_UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";
static REPL_UUID: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a4";
static HOSTNQN: &str = NVME_NQN_PREFIX;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";
static DISKNAME2: &str = "/tmp/disk2.img";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn get_mayastor_nvme_device() -> String {
    let nvme_ms = list_mayastor_nvme_devices();
    assert_eq!(nvme_ms.len(), 1);
    format!("/dev/{}", nvme_ms[0].device)
}

#[tokio::test]
/// Create a nexus with a local and a remote replica.
/// Verify that write-zeroes does actually write zeroes.
async fn nexus_io_race_orig() {
    common::composer_init();
    const ETCD_ENDPOINT: &str = "http://localhost:2379";

    let test = Builder::new()
        .name("io-race")
        .add_container_spec(
            common::compose::ContainerSpec::from_binary(
                "etcd",
                Binary::from_path(env!("ETCD_BIN")).with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .with_logs(false)
        .build()
        .await
        .unwrap();

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 44 * 1024);
    common::truncate_file(DISKNAME2, 44 * 1024);

    let ms = get_ms();
    let nexus_name = format!("nexus-{NEXUS_UUID}");
    let name = nexus_name.clone();
    let r1 = REPL_UUID;
    let r2 = "1c7152fd-d2a6-4ee7-8729-2822906d44a4";
    let r3 = "229bc77c-7422-4a76-940a-e8f2e93d88bf";

    PersistentStore::init(Some(ETCD_ENDPOINT.to_string())).await;

    ms.spawn(async move {
        // Create local pool and replica
        let pool1 = Lvs::create_or_import(PoolArgs {
            name: POOL_NAME.to_string(),
            disks: vec![BDEVNAME1.to_string()],
            uuid: None,
        })
        .await
        .unwrap();
        let pool2 = Lvs::create_or_import(PoolArgs {
            name: "p2".to_string(),
            disks: vec![DISKNAME2.to_string()],
            uuid: None,
        })
        .await
        .unwrap();

        const POOL_SZ_BYTES: u64 = 32 * 1024 * 1024;

        pool1
            .create_lvol(r1, POOL_SZ_BYTES, Some(r1), true)
            .await
            .unwrap();

        pool2
            .create_lvol(r2, POOL_SZ_BYTES, Some(r2), true)
            .await
            .unwrap();

        pool1
            .create_lvol(r3, POOL_SZ_BYTES, None, false)
            .await
            .unwrap();

        // create nexus on local node with 2 children, local and remote
        nexus_create(
            &name,
            POOL_SZ_BYTES,
            Some(NEXUS_UUID),
            &[
                format!("loopback:///{r1}?uuid={r1}"),
                format!("loopback:///{r2}?uuid={r2}"),
            ],
        )
        .await
        .unwrap();

        nexus_lookup_mut(&name)
            .unwrap()
            .share(Protocol::Nvmf, None)
            .await
            .unwrap();

        reactor_poll!(600);
    })
    .await;

    let nqn = format!("{HOSTNQN}:nexus-{NEXUS_UUID}");
    nvme_connect("127.0.0.1", &nqn, true);

    let ns = get_mayastor_nvme_device();
    println!(">>>> ns: {ns}");

    // pause etcd..
    test.pause("etcd").await.unwrap();

    ms.spawn({
        let name = nexus_name.clone();
        async move {
            let size_bytes =
                nexus_lookup(&name).unwrap().size_in_bytes() - 5242880;
            let size_blks = size_bytes / 512;

            for blk in 0 .. 6143 {
                let noffset = blk * 512;
                let offset_repl = noffset + 5242880;

                println!(">>>> Writing Nexus @ {noffset}B {blk} blks");
                bdev_io::write_some(&name, noffset, 0xae).await.unwrap();
            }
        }
    })
    .await;

    let fail_io = ms.spawn({
        let name = nexus_name.clone();
        async move {
            println!(">>>> Now the bad IO!");

            let (s, r) = unbounded();
            Mthread::spawn_unaffinitized(move || {
                s.send(common::dd_urandom_blkdev_test(&ns))
            });

            //- let dd_result: i32;

            async fn dummy() {}

            let r = loop {
                dummy().await;
                io_engine::core::Reactors::current().poll_once();
                if let Ok(r) = r.try_recv() {
                    break r;
                }
            };

            println!(">>>> Now the bad IO! DONE");
            r
        }
    });
    tokio::pin!(fail_io);

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(3));
    tokio::select! {
        _ = timeout => {
            println!(">>>> Timeout!");
        }
        mut dd_result = &mut fail_io => {
            assert_ne!(dd_result, 0, "IOs should be stuck!");
        }
    }

    // unpause etcd..
    test.thaw("etcd").await.unwrap();

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(3));
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
            panic!("Should have completed!");
        }
        mut n_wr = &mut fail_io => {
            println!(">>>> Success!");
        }
    }

    let name = nexus_name.clone();
    ms.spawn(async move {
        for blk in 6144 .. 6145 {
            let noffset = blk * 512;
            let offset_repl = noffset + 5242880;
            let r_blks = offset_repl / 512;

            println!(">>>> Reading rr {r2} @ {offset_repl}B {r_blks} blks");
            bdev_io::read_some(r2, offset_repl, 0xae).await.unwrap();

            println!(">>>> Reading r1 {r1} @ {offset_repl}B {r_blks} blks");
            let result_1 = bdev_io::read_some_safe(r1, offset_repl, 0xae)
                .await
                .unwrap();
            assert_eq!(
                result_1, false,
                "Should fail because it's failed due to enospc"
            );
        }
    })
    .await;

    let mut etcd = etcd_client::Client::connect([ETCD_ENDPOINT], None)
        .await
        .unwrap();
    let response = etcd.get(NEXUS_UUID, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let nexus_info: io_engine::bdev::NexusInfo =
        serde_json::from_slice(value).unwrap();

    // Check the persisted nexus info is correct.
    assert!(!nexus_info.clean_shutdown);
    let r1 = nexus_info.children.iter().find(|c| c.uuid == r1).unwrap();
    assert!(!r1.healthy);

    let r2 = nexus_info.children.iter().find(|c| c.uuid == r2).unwrap();
    assert!(r2.healthy);
}

#[tokio::test]
#[ignore]
// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_io_race_ng() {
    const POOL_SIZE: u64 = 100;
    const REPL_SIZE: u64 = 80;
    const NEXUS_SIZE: u64 = REPL_SIZE;
    const DATA_SIZE_OK: u64 = POOL_SIZE - REPL_SIZE - 10;

    common::composer_init();

    let etcd_endpoint = format!("http://10.1.0.2:2379");

    let test = Builder::new()
        .name("io-race")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_spec(
            common::compose::ContainerSpec::from_binary(
                "etcd",
                Binary::from_path(env!("ETCD_BIN")).with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    &etcd_endpoint,
                    "--listen-client-urls",
                    &etcd_endpoint,
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec![
                "-Fcolor,nodate",
                "-l",
                "3,4",
                "-p",
                &etcd_endpoint,
            ]),
        )
        .with_clean(true)
        .with_logs(true)
        .build()
        .await
        .unwrap();

    println!(">>>> {test:#?}");

    let conn = GrpcConnect::new(&test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    // Node #0
    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    // Node #1
    let mut pool_1 = PoolBuilder::new(ms_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    // Nexus
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // Fault a child.
    nex_0
        .inject_fault_at_replica(
            &repl_0,
            &format!("op=write&offset={offset}", offset = 10 * 128),
        )
        .await
        .unwrap();

    nex_0
        .inject_fault_at_replica(&repl_0, &format!("op=retire_persist"))
        .await
        .unwrap();

    //
    // println!(">>>> PAUSE ETCD ...");
    // test.pause("etcd").await.unwrap();
    // println!(">>>> PAUSE ETCD DONE");

    // Run FIO in order to trigger I/O fault.
    test_fio_to_nexus(
        &nex_0,
        Fio::new().with_job(
            FioJob::new()
                .with_bs(4096)
                .with_iodepth(8)
                .with_size(DataSize::from_mb(DATA_SIZE_OK)),
        ),
    )
    .await
    .unwrap();

    // // Check child state.
    // let child = nex_0.get_nexus_replica_child(&repl_0).await.unwrap();
    // assert_eq!(child.state(), ChildState::Faulted);
    // assert_eq!(child.state_reason(), ChildStateReason::IoFailure);

    // println!(">>>> THAW ETCD ...");
    // test.thaw("etcd").await.unwrap();
    // println!(">>>> THAW ETCD DONE");

    ////////////////////////////////////////////////////////////////////////////

    println!("--------------------------------------------------------");
    test.print_log("etcd");

    println!("--------------------------------------------------------");
    test.print_log("ms_nex");

    ////////////////////////////////////////////////////////////////////////////

    let mut etcd = etcd_client::Client::connect([etcd_endpoint], None)
        .await
        .unwrap();

    let response = etcd.get(nex_0.name(), None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();

    let nexus_info: io_engine::bdev::NexusInfo =
        serde_json::from_slice(value).unwrap();

    println!("{nexus_info:#?}");

    ////////////////////////////////////////////////////////////////////////////

    validate_replicas(&vec![repl_0, repl_1]).await;
}