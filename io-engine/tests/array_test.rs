pub mod common;
use common::MayastorTest;

use io_engine::{
    core::{MayastorCliArgs, Protocol},
};

#[tokio::test]
async fn array_test_1() {
    let ms = MayastorTest::new(MayastorCliArgs::default());

    // Create a nexus with a single child
    ms.spawn(async {
        // nexus_create(NEXUS_NAME, 512 * 131_072, None, &children)
        //     .await
        //     .expect("Failed to create nexus");
    })
    .await;
}
