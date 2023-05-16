pub mod common;

use common::{
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason},
            GrpcConnect,
        },
        Binary,
        Builder,
    },
    file_io::DataSize,
    fio::{Fio, FioJob},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

const POOL_SIZE: u64 = 80;
const REPL_SIZE: u64 = 60;
const NEXUS_SIZE: u64 = REPL_SIZE;
const DATA_SIZE_OK: u64 = POOL_SIZE - REPL_SIZE - 10;

#[tokio::test]
#[cfg(feature = "nexus-fault-injection")]
async fn nexus_metadata_check() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
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
                "-l",
                "3,4",
                "-F",
                "compact,color",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

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

    // Destroy the nexus and create it again on the same replicas.
    nex_0.destroy().await.unwrap();
    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // Check child states.
    for child in nex_0.get_nexus().await.unwrap().children {
        assert_eq!(child.state(), ChildState::Online);
        assert_eq!(child.state_reason(), ChildStateReason::None);
    }

    // Fault a child.
    nex_0
        .inject_fault_at_replica(
            &repl_0,
            &format!("op=write&offset={offset}", offset = 10 * 128),
        )
        .await
        .unwrap();

    // Run FIO in order to trigger I/O fault.
    test_fio_to_nexus(
        &nex_0,
        &Fio::new().with_job(
            FioJob::new()
                .with_bs(4096)
                .with_iodepth(8)
                .with_size(DataSize::from_mb(DATA_SIZE_OK)),
        ),
    )
    .await
    .unwrap();

    // Check child state.
    let child = nex_0.get_nexus_replica_child(&repl_0).await.unwrap();
    assert_eq!(child.state(), ChildState::Faulted);
    assert_eq!(child.state_reason(), ChildStateReason::IoFailure);

    // Destroy the nexus and create it again on the same replicas.
    nex_0.destroy().await.unwrap();
    nex_0.create().await.unwrap();

    // Now as one of the replicas were faulted, the nexus won't open it.
    let n = nex_0.get_nexus().await.unwrap();
    println!("---- 2 ----");
    println!("{n:#?}");

    let child = nex_0.get_nexus_replica_child(&repl_0).await.unwrap();
    assert_eq!(child.state(), ChildState::Degraded);
    assert_eq!(child.state_reason(), ChildStateReason::Closed);

    // test.print_log("ms_nex");
}
