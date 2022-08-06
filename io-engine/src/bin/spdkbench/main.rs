#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_must_use)]
#![allow(unused_imports)]

mod context;
mod fio;
mod nvme;
mod tools;

use crate::{
    context::{Context, Replica},
    fio::FioJobMode,
    nvme::Nvme,
    tools::run_command_args,
};
use colored_json::prelude::*;
use fio::{Fio, FioJob};
use rpc::mayastor as rpc;
use tonic::transport::Endpoint;

// /// TODO
// async fn prepare_pools(ctx: &mut Context) -> Result<(), String> {
//     println!("Preparing pools...");
//
//     ctx.create_pool("pool0", "/dev/nvme0n1").await?;
//     ctx.create_pool("pool1", "/dev/nvme1n1").await?;
//
//     ctx.destroy_replica("r0").await?;
//     ctx.destroy_replica("r1").await?;
//
//     println!("Preparing pools OK");
//     Ok(())
// }
//
// /// TODO
// async fn prepare_replicas(ctx: &mut Context) -> Result<(), String> {
//     println!("Preparing replicas...");
//
//     ctx.create_replica("pool0", "r0", 32 * 1024, false).await?;
//     ctx.create_replica("pool1", "r1", 32 * 1024, true).await?;
//
//     println!("Preparing replicas OK");
//     Ok(())
// }
//

//
// /// TODO
// async fn connect_replicas(
//     ctx: &mut Context,
//     nvme: &Nvme,
// ) -> Result<Vec<Replica>, String> {
//     println!("Connecting replicas...");
//
//     let repls = ctx.list_replicas().await?;
//     let relps: Vec<Replica> = repls
//         .into_iter()
//         .filter_map(|r| connect_replica(r, nvme))
//         .collect();
//
//     println!("Connecting replicas OK");
//     Ok(relps)
// }

/// TODO
async fn bench_replica(r: &Replica<'_>) {
    println!();
    println!("Running FIO for replica {} ...", r);

    let mut job = FioJob::new(&r.name);
    job.filename = r.device.as_ref().unwrap().clone();
    job.mode = FioJobMode::RandRw;
    job.size = 20 * 1024;
    job.runtime = 30;
    job.iodepth = 16;
    job.numjobs = 1;

    let fio = Fio::new(
        &format!(
            "bench_{}{}",
            r.name,
            match r.thin {
                true => "_thin",
                false => "",
            }
        ),
        vec![job],
    );
    fio.run().await;

    println!();
    let pool = r.ctx.get_pool(&r.pool).await.unwrap();
    println!("Pool stat: {}", pool);
    println!();
}

/// TODO
async fn test_pool(
    ctx: &Context<'_>,
    pool: &str,
    rname: &str,
    size: u64,
    thin: bool,
) -> Result<(), String> {
    ctx.destroy_replica(rname).await?;
    ctx.create_replica(pool, rname, size, thin).await?;
    let mut r = ctx.get_replica(rname).await?;

    r.connect().await?;
    bench_replica(&r).await;
    r.disconnect().await?;

    println!();

    Ok(())
}

/// TODO
async fn test_alloc(
    ctx: &Context<'_>,
    pool: &str,
    rname: &str,
    size: u64,
    thin: bool,
    count: u64,
) -> Result<(), String> {
    println!();
    println!("Testing replica alloc");

    let mut repls = Vec::new();

    for i in 0 .. count {
        println!("Allocating replica #{}...", i);
        let rname2 = format!("{}_{}", rname, i);
        ctx.create_replica(pool, &rname2, size, thin).await?;
        let mut r = ctx.get_replica(&rname2).await?;
        r.connect().await?;

        println!();
        let pool = r.ctx.get_pool(&r.pool).await.unwrap();
        println!("Pool stat: {}", pool);
        println!();

        repls.push(r);
    }

    ctx.nvme.print_list();
    println!();

    println!("Filling replicas...");
    for r in &repls {
        println!();
        println!("Filling replica {}...", r);

        let mut job = FioJob::new(&r.name);
        job.filename = r.device.as_ref().unwrap().clone();
        job.mode = FioJobMode::SeqWrite;
        job.size = size;
        job.runtime = 600;

        let fio = Fio::new(
            &format!(
                "fill_{}{}",
                r.name,
                match r.thin {
                    true => "_thin",
                    false => "",
                }
            ),
            vec![job],
        );
        fio.run().await;

        println!();
        let pool = r.ctx.get_pool(&r.pool).await.unwrap();
        println!("Pool stat: {}", pool);
        println!();
    }

    Ok(())
}

/// TODO
async fn run_test(ctx: &Context<'_>) -> Result<(), String> {
    test_alloc(&ctx, "pool0", "r0", 200 * 1024, true, 10).await?;

    // let pools = vec![("pool0", "/dev/nvme0n1", "r0")];
    // let pools = vec![
    //     ("pool0", "/dev/nvme0n1", "r0"),
    //     ("pool0", "/dev/nvme1n1", "r1"),
    // ];

    // for (pool, device, repl) in pools {
    //     println!("Running benchmarks for device '{}'", device);
    //     ctx.destroy_pool(pool).await?;
    //     ctx.create_pool(pool, device).await?;
    //
    //     test_pool(&ctx, pool, repl, 40 * 1024, false).await?;
    //     test_pool(&ctx, pool, repl, 40 * 1024, true).await?;
    //
    //     println!();
    // }

    Ok(())
}

/// TODO
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), String> {
    env_logger::init();

    let ctx = Context::new("127.0.0.1", 10124, 8420).await;

    ctx.cleanup().await;
    println!();

    let res = run_test(&ctx).await;

    println!();
    ctx.cleanup().await;
    res
}
