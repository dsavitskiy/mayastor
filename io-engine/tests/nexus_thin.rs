pub mod common;

use common::{
    bdev::list_bdevs,
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    nexus::{list_nexuses, NexusBuilder},
    nice_json,
    pool::{list_pools, PoolBuilder},
    replica::{list_replicas, ReplicaBuilder},
};
use crate::common::nvme::nvme_discover;

#[tokio::test]
async fn nexus_thin_local_nospc() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let mut ms1 = conn.grpc_handle("ms1").await.unwrap();

    let pool_0 = PoolBuilder::new()
        .with_name("pool0")
        .with_uuid("6e3c062c-293b-46e6-8ab3-ff13c1643437")
        .with_bdev("malloc:///mem0?size_mb=80");

    let pool_1 = PoolBuilder::new()
        .with_name("pool1")
        .with_uuid("6b177ff6-0100-4456-af52-8875b1641079")
        .with_bdev("malloc:///mem1?size_mb=80");

    let repl_0 = ReplicaBuilder::new()
        .with_pool(&pool_0)
        .with_name("r0")
        .with_uuid("f099e2ea-61a9-40ce-a1c3-2cb13956355a")
        .with_size_mb(60)
        .with_thin(true);

    let fill_0 = ReplicaBuilder::new()
        .with_pool(&pool_0)
        .with_name("f0")
        .with_uuid("96d196a6-5f70-4894-8b2e-6da4b74a3c37")
        .with_size_mb(60)
        .with_thin(false);

    let repl_1 = ReplicaBuilder::new()
        .with_pool(&pool_1)
        .with_name("r1")
        .with_uuid("6466b8d5-97be-4b21-8d44-5d8cbbd6d6a0")
        .with_size_mb(60)
        .with_thin(true);

    pool_0.create(&mut ms1).await.unwrap();
    pool_1.create(&mut ms1).await.unwrap();

    repl_0.create(&mut ms1).await.unwrap();
    fill_0.create(&mut ms1).await.unwrap();
    repl_1.create(&mut ms1).await.unwrap();

    let nex = NexusBuilder::new()
        .with_name("nexus0")
        .with_uuid("55b66a8f-6b4e-4a02-98c5-fb7d01f1abe5")
        .with_size_mb(60)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex.create(&mut ms1).await.unwrap();
    nex.publish(&mut ms1).await.unwrap();

    nvme_discover(&ms1.endpoint.ip().to_string());

    // ////
    // let rr = list_pools(&mut ms1).await.unwrap();
    // println!("{}", nice_json(&rr));
    //
    // ////
    // let rr = list_replicas(&mut ms1).await.unwrap();
    // println!("{}", nice_json(&rr));
    //
    // ////
    // let rr = list_nexuses(&mut ms1).await.unwrap();
    // println!("{}", nice_json(&rr));
    //
    // ////
    // let rr = list_bdevs(&mut ms1).await.unwrap();
    // println!("{}", nice_json(&rr));

    // + create two pools
    // + create r0,fill0,r1
    // + create nexus
    // + publish nexus
    // connect {
    //      copy data
    // }
    // check nexus children
}
