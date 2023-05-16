#![allow(unused_variables)]
#![allow(unused_mut)]

//! Nexus IO tests for multipath NVMf, reservation, and write-zeroes
use common::bdev_io;

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    constants::NVME_NQN_PREFIX,
    core::{MayastorCliArgs, Protocol},
    lvs::Lvs,
    pool_backend::PoolArgs,
};

use crossbeam::channel::unbounded;
use once_cell::sync::OnceCell;

pub mod common;

use common::{
    compose::{Binary, Builder},
    nvme::{list_mayastor_nvme_devices, nvme_connect},
    MayastorTest,
};
use io_engine::{
    bdev::nexus::nexus_lookup,
    core::Mthread,
    persistent_store::PersistentStore,
};
use io_engine_tests::reactor_poll;

extern crate libnvme_rs;

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
async fn io_race() {
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

    let mayastor = get_ms();
    let nexus_name = format!("nexus-{NEXUS_UUID}");
    let name = nexus_name.clone();
    let r1 = REPL_UUID;
    let r2 = "1c7152fd-d2a6-4ee7-8729-2822906d44a4";
    let r3 = "229bc77c-7422-4a76-940a-e8f2e93d88bf";

    PersistentStore::init(Some(ETCD_ENDPOINT.to_string())).await;

    mayastor
        .spawn(async move {
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

            pool1
                .create_lvol(r1, 32 * 1024 * 1024, Some(r1), true)
                .await
                .unwrap();

            pool2
                .create_lvol(r2, 32 * 1024 * 1024, Some(r2), true)
                .await
                .unwrap();

            pool1
                .create_lvol(r3, 32 * 1024 * 1024, None, false)
                .await
                .unwrap();

            // create nexus on local node with 2 children, local and remote
            nexus_create(
                &name,
                32 * 1024 * 1024,
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
    tracing::debug!("ns: {ns}");

    // pause etcd..
    test.pause("etcd").await.unwrap();

    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            let size_bytes =
                nexus_lookup(&name).unwrap().size_in_bytes() - 5242880;
            let size_blks = size_bytes / 512;

            for blk in 0 .. 6143 {
                let noffset = blk * 512;
                let offset_repl = noffset + 5242880;

                let start = std::time::Instant::now();
                println!("Writing Nexus @ {noffset}B {blk} blks");
                bdev_io::write_some(&name, noffset, 0xae).await.unwrap();
            }
        })
        .await;

    let name = nexus_name.clone();
    let fail_io = mayastor.spawn(async move {
        println!("Now the bad IO!");

        let (s, r) = unbounded();
        Mthread::spawn_unaffinitized(move || {
            s.send(common::dd_urandom_blkdev_test(&ns))
        });

        let dd_result: i32;

        async fn dummy() {}

        loop {
            dummy().await;
            io_engine::core::Reactors::current().poll_once();
            if let Ok(r) = r.try_recv() {
                break r;
            }
        }
    });
    tokio::pin!(fail_io);

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(3));
    tokio::select! {
        _ = timeout => {
            println!("Timeout!");
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
            println!("Success!");
        }
    }

    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            for blk in 6144 .. 6145 {
                let noffset = blk * 512;
                let offset_repl = noffset + 5242880;
                let r_blks = offset_repl / 512;

                println!("Reading rr {r2} @ {offset_repl}B {r_blks} blks");
                bdev_io::read_some(r2, offset_repl, 0xae).await.unwrap();

                println!("Reading r1 {r1} @ {offset_repl}B {r_blks} blks");
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
