use super::rand::create_random_test_buf;
use std::{io::SeekFrom, path::Path};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub async fn test_write_to_file(
    path: impl AsRef<Path>,
    count: usize,
    buf_size_mb: usize,
) -> std::io::Result<()> {
    let src_buf = create_random_test_buf(buf_size_mb).await;

    let mut dst_buf: Vec<u8> = vec![];
    dst_buf.resize(src_buf.len(), 0);

    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path)
        .await?;

    for _i in 0 .. count {
        let pos = f.stream_position().await?;

        // Write buffer.
        f.write_all(&src_buf).await?;

        // Validate written data.
        f.seek(SeekFrom::Start(pos)).await?;
        f.read_exact(&mut dst_buf).await?;

        for k in 0 .. src_buf.len() {
            if src_buf[k] != dst_buf[k] {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Data validation failed at {}: written {:?}, read {:?}",
                        pos + k as u64,
                        src_buf[k],
                        dst_buf[k]
                    ),
                ));
            }
        }
    }

    Ok(())
}

pub async fn compute_file_checksum(
    path: impl AsRef<Path>,
) -> std::io::Result<String> {
    let mut f = OpenOptions::new()
        .write(false)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path)
        .await?;

    let mut buf = [0; 16384];
    let mut hasher = md5::Context::new();

    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.consume(&buf[.. n]);
    }

    Ok(hex::encode(hasher.compute().0))
}

pub async fn wipe_device(device_path: &str) -> std::io::Result<()> {
    println!("@@@@ wipe {}", device_path);
    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&device_path)
        .await?;

    let mut buf: Vec<u8> = vec![];
    buf.resize(1024 * 1024, 0);

    f.write_all(&buf).await
}
