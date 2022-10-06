use once_cell::sync::OnceCell;
use rand::{distributions::Uniform, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::Duration;
use tokio::sync::Mutex;

type TestRng = rand_chacha::ChaCha8Rng;

static RNG_SEED: OnceCell<Mutex<TestRng>> = OnceCell::new();

fn new_test_rng() -> Mutex<TestRng> {
    Mutex::new(ChaCha8Rng::from_entropy())
}

pub fn set_test_rng_seed(seed: u64) {
    RNG_SEED
        .set(Mutex::new(ChaCha8Rng::seed_from_u64(seed)))
        .unwrap();
}

pub async fn create_random_test_buf(size_mb: usize) -> Vec<u8> {
    let rng = RNG_SEED.get_or_init(new_test_rng).lock().await.clone();
    let range = Uniform::new_inclusive(0, u8::MAX);

    rng.sample_iter(&range)
        .take(size_mb * 1024 * 1024)
        .collect()
}

pub async fn random_sleep(min_ms: u64, max_ms: u64) {
    let mut rng = RNG_SEED.get_or_init(new_test_rng).lock().await;
    let range = Uniform::new_inclusive(min_ms, max_ms);
    let t = rng.sample(&range);
    println!(
        "#### tokio sleeping for {} ms (in {} .. {})",
        t, min_ms, max_ms
    );
    tokio::time::sleep(Duration::from_millis(t)).await;
}
