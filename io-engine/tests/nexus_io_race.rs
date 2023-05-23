use std::time::Duration;

pub mod common;

use common::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::DataSize,
    nexus::{test_fio_to_nexus, test_write_to_nexus, NexusBuilder},
    nvme::{list_mayastor_nvme_devices, nvme_connect},
    pool::PoolBuilder,
    replica::{validate_replicas, ReplicaBuilder},
};

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
        const POOL_SIZE: u64 = 100;
        const REPL_SIZE: u64 = 80;
        const NEXUS_SIZE: u64 = REPL_SIZE;

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

#[tokio::test]
// #[ignore]
// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    // Fault replica #0 at block 10.
    nex_0
        .inject_fault_at_replica(
            &repl_0,
            &format!("op=write&offset={offset}", offset = 10),
        )
        .await
        .unwrap();

    // let t = sleep_with_progress(2500).await;

    // Pause ETCD.
    cluster.test.pause("etcd").await.unwrap();

    println!(">>>> Go #0 ...");
    let io_0 = test_write_to_nexus(
        &nex_0,
        DataSize::from_blocks(0, BLOCK_SIZE),
        1,
        DataSize::from_kb(16),
    );
    tokio::pin!(io_0);

    println!(">> 1");
    let timeout_0 = tokio::time::sleep(Duration::from_secs(3));
    println!(">> 2");
    tokio::select! {
        _ = timeout_0 => {
            println!(">>>> T.O. #0");
        }
        mut res = &mut io_0 => {
            panic!("IOs must stuck");
        }
    }

    println!(">>>> Go #0 done");

    // cluster.test.print_log("ms_nex");
    // Run FIO in order to trigger I/O fault.
    // test_fio_to_nexus(
    //     &nex_0,
    //     &Fio::new().with_job(
    //         FioJob::new()
    //             .with_bs(4096)
    //             .with_iodepth(8)
    //             .with_size(DataSize::from_mb(DATA_SIZE_OK)),
    //     ),
    // )
    // .await
    // .unwrap();
}

// async fn sleep_with_progress(ms: u64) {
//     use std::io::{stdout, Write};
//
//     print!("\n(sleeping ");
//     stdout().flush().unwrap();
//
//     let d = 100;
//     let mut t = 0;
//     loop {
//         let s = if t % 1000 == 0 {
//             (t / 1000).to_string()
//         } else {
//             ".".to_string()
//         };
//         print!("{s}");
//         stdout().flush().unwrap();
//
//         tokio::time::sleep(Duration::from_millis(d)).await;
//
//         t += d;
//         if t >= ms {
//             break;
//         }
//     }
//     println!(")");
// }
