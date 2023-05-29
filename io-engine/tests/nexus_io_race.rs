use std::{str::FromStr, time::Duration};

pub mod common;

use common::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::DataSize,
    fio::{Fio, FioJob},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

//-----------------------------

use once_cell::sync::OnceCell;

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut, Injection, InjectionOp},
    core::{MayastorCliArgs, Protocol},
    logger::LogFormat,
    lvs::Lvs,
    persistent_store::PersistentStore,
    pool_backend::PoolArgs,
};

use io_engine_tests::{bdev_io, reactor_poll, MayastorTest};

/// TODO
#[allow(dead_code)]
struct TestCluster {
    test: Box<ComposeTest>,
    etcd_endpoint: String,
    etcd: etcd_client::Client,
    ms_0: SharedRpcHandle,
    ms_1: SharedRpcHandle,
    ms_nex: SharedRpcHandle,
}

impl TestCluster {
    async fn create() -> Self {
        let etcd_endpoint = format!("http://10.1.0.2:2379");

        let test = Box::new(
            Builder::new()
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
                .unwrap(),
        );

        let conn = GrpcConnect::new(&test);

        let etcd = etcd_client::Client::connect([&etcd_endpoint], None)
            .await
            .unwrap();

        let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
        let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
        let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

        Self {
            test,
            etcd_endpoint,
            etcd,
            ms_0,
            ms_1,
            ms_nex,
        }
    }
}

const POOL_SIZE: u64 = 100;
const REPL_SIZE: u64 = 80;
const NEXUS_SIZE: u64 = REPL_SIZE;

/// TODO
#[allow(dead_code)]
struct TestStorage {
    pool_0: PoolBuilder,
    repl_0: ReplicaBuilder,
    pool_1: PoolBuilder,
    repl_1: ReplicaBuilder,
    nex_0: NexusBuilder,
}

impl TestStorage {
    async fn create(cluster: &TestCluster) -> Self {
        let ms_0 = cluster.ms_0.clone();
        let ms_1 = cluster.ms_1.clone();
        let ms_nex = cluster.ms_nex.clone();

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

        Self {
            pool_0,
            repl_0,
            pool_1,
            repl_1,
            nex_0,
        }
    }
}

/*
create
   etcd,
   r0, r1
   nex [r0, r1]

1)
pause ETCD
inject error to r0
write to nex
-> r0 fails
-> I/O on nexus must stuck

2)
thaw ETCD
-> I/O on nexus must complete

3)
???


4)
-> ETCD report r0, r1 correctly

*/

/* Tokio select:
use tokio::sync::oneshot;

let (s1, r1) = oneshot::channel();
let (s2, r2) = oneshot::channel();

tokio::spawn(async {
    tokio::time::sleep(Duration::from_millis(100)).await;
    s1.send("(1)").ok();
});

tokio::spawn(async {
    tokio::time::sleep(Duration::from_millis(200)).await;
    s2.send("(2)").ok();
});

tokio::select! {
    val = r1 => {
        println!("rx1 completed first with {:?}", val);
    }
    val = r2 => {
        println!("rx2 completed first with {:?}", val);
    }
}
*/

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn nexus_io_race_ng() {
    const BLOCK_SIZE: u64 = 512;

    common::composer_init();

    let cluster = TestCluster::create().await;

    let TestStorage {
        pool_0: _,
        repl_0,
        pool_1: _,
        repl_1: _,
        nex_0,
    } = TestStorage::create(&cluster).await;

    println!("[1] Storage created");

    // Fault replica #0 at block 10.
    nex_0
        .inject_fault_at_replica(
            &repl_0,
            &format!("op=write&offset={offset}", offset = 10),
        )
        .await
        .unwrap();

    println!("[2] Injected");

    // Pause ETCD.
    cluster.test.pause("etcd").await.unwrap();
    println!("[3] ETCD Paused");

    let r1 = tokio::spawn({
        let nex_0 = nex_0.clone();
        async move {
            // println!("A: [1] Writing...");
            // let r = test_write_to_nexus(
            //     &nex_0,
            //     DataSize::from_blocks(0, BLOCK_SIZE),
            //     10,
            //     DataSize::from_kb(16),
            // )
            // .await;
            // println!("A: [2] Writing done: {r:?}");

            println!("A: [1] FIO Writing...");
            test_fio_to_nexus(
                &nex_0,
                Fio::new()
                    .with_job(
                        FioJob::new()
                            // .with_runtime(1)
                            .with_bs(4096)
                            .with_iodepth(8)
                            .with_size(DataSize::from_mb(NEXUS_SIZE - 10)),
                    )
                    .with_verbose_err(true)
                    .with_verbose(true),
            )
            .await
            .unwrap();
            println!("A: [2] FIO Writing done");
        }
    });
    tokio::pin!(r1);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut r1)
            .await
            .is_err(),
        "I/O to nexus must freeze when ETCD is paused"
    );

    // Thaw ETCD.
    cluster.test.thaw("etcd").await.unwrap();
    println!("[5] ETCD Thawed");

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut r1)
            .await
            .is_ok(),
        "I/O to nexus must proceed when ETCD is thawed"
    );

    println!("-------------------------------------------");

    let n = nex_0.get_nexus().await.unwrap();
    println!("[-]: Nexus:\n{n:#?}");

    cluster.test.print_log("ms_nex");
}

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            // reactor_mask: "0xf".to_string(),
            log_format: Some(LogFormat::from_str("compact,nodate").unwrap()),
            ..Default::default()
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_io_race_bdev_ng() {
    const ETCD_ENDPOINT: &str = "http://localhost:2379";

    const POOL_SIZE: u64 = 32 * 1024 * 1024;

    const DISK_NAME_0: &str = "/tmp/disk1.img";
    const BDEV_NAME_0: &str = "aio:///tmp/disk1.img?blk_size=512";
    const POOL_NAME_0: &str = "pool_0";
    const REPL_NAME_0: &str = "repl_0";
    const REPL_UUID_0: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a4";

    const DISK_NAME_1: &str = "/tmp/disk2.img";
    const POOL_NAME_1: &str = "pool_1";
    const REPL_NAME_1: &str = "repl_1";
    const REPL_UUID_1: &str = "1c7152fd-d2a6-4ee7-8729-2822906d44a4";

    const NEXUS_ADDR: &str = "127.0.0.1:8420";
    const NEXUS_NAME: &str = "nexus_0";
    const NEXUS_UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";

    //
    common::composer_init();
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

    PersistentStore::init(Some(ETCD_ENDPOINT.to_string())).await;

    //
    let ms = get_ms();

    //
    common::delete_file(&[DISK_NAME_0.into(), DISK_NAME_1.into()]);
    common::truncate_file(DISK_NAME_0, 44 * 1024);
    common::truncate_file(DISK_NAME_1, 44 * 1024);

    ms.spawn(async move {
        // Pool #0 and replica #0.
        let pool_0 = Lvs::create_or_import(PoolArgs {
            name: POOL_NAME_0.to_string(),
            disks: vec![BDEV_NAME_0.to_string()],
            uuid: None,
        })
        .await
        .unwrap();

        // Pool #1 and replica #1.
        pool_0
            .create_lvol(REPL_NAME_0, POOL_SIZE, Some(REPL_UUID_0), false)
            .await
            .unwrap();

        let pool_1 = Lvs::create_or_import(PoolArgs {
            name: POOL_NAME_1.to_string(),
            disks: vec![DISK_NAME_1.to_string()],
            uuid: None,
        })
        .await
        .unwrap();

        pool_1
            .create_lvol(REPL_NAME_1, POOL_SIZE, Some(REPL_UUID_1), false)
            .await
            .unwrap();

        // Create a nexus with 2 children.
        nexus_create(
            NEXUS_NAME,
            POOL_SIZE,
            Some(NEXUS_UUID),
            &[
                format!("loopback:///{REPL_NAME_0}?uuid={REPL_UUID_0}"),
                format!("loopback:///{REPL_NAME_1}?uuid={REPL_UUID_1}"),
            ],
        )
        .await
        .unwrap();

        nexus_lookup_mut(NEXUS_NAME)
            .unwrap()
            .share(Protocol::Nvmf, None)
            .await
            .unwrap();

        reactor_poll!(600);
    })
    .await;

    // Inject a fault on the nexus.
    let nex = nexus_lookup_mut(NEXUS_NAME).unwrap();

    let inj_device = nex
        .children_iter()
        .nth(0)
        .unwrap()
        .get_device_name()
        .unwrap();

    nex.inject_add(Injection::new(
        &inj_device,
        InjectionOp::Write,
        Duration::ZERO,
        Duration::MAX,
        0 .. 1,
    ));

    // Pause etcd.
    test.pause("etcd").await.unwrap();
    println!("\nTest: ETCD paused\n");

    let io = ms.spawn(async {
        println!("Test: Writing to nexus bdev ...");
        bdev_io::write_blocks(NEXUS_NAME, 0, 1, 0xaa).await.unwrap();
        println!("\nTest: Writing to nexus bdev finished\n");
    });
    tokio::pin!(io);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut io)
            .await
            .is_err(),
        "I/O to nexus must freeze when ETCD is paused"
    );

    // Thaw etcd.
    test.thaw("etcd").await.unwrap();
    println!("\nTest: ETCD thawed\n");

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut io)
            .await
            .is_ok(),
        "I/O to nexus must proceed when ETCD is thawed"
    );
}
