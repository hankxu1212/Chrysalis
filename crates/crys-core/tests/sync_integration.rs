//! Sync integration tests against real S3.
//!
//! Mirrors the end-to-end Phase 5 cycle (`add → commit → push → clone →
//! pull`) but uses the S3 backend. Each test isolates itself under
//! `crys-test/<pid>-<nanos>-<salt>/` and best-effort cleans up on the way
//! out via a Cleanup guard. Gated on `CRYS_TEST_BUCKET`.

use std::time::{SystemTime, UNIX_EPOCH};

use crys_core::repo::{init_remote, DEFAULT_CHUNK_SIZE};
use crys_core::s3::S3Client;
use crys_core::stage;
use crys_core::store::S3Store;
use crys_core::sync::{clone_repo, pull, push};
use crys_core::{Repo, S3Uri};

fn bucket() -> Option<String> {
    std::env::var("CRYS_TEST_BUCKET").ok()
}

fn unique_prefix(salt: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("crys-test/{}-{}-{}", std::process::id(), nanos, salt)
}

macro_rules! require_bucket {
    () => {
        match bucket() {
            Some(b) => b,
            None => {
                eprintln!("skipping sync integration test: set CRYS_TEST_BUCKET to enable");
                return;
            }
        }
    };
}

/// Best-effort delete every key under `prefix` so the test bucket doesn't
/// accumulate detritus.
async fn purge_prefix(client: &S3Client, bucket: &str, prefix: &str) {
    let walk_prefix = format!("{}/", prefix.trim_end_matches('/'));
    if let Ok(keys) = client.list_prefix(bucket, &walk_prefix).await {
        for key in keys {
            let _ = client.delete(bucket, &key).await;
        }
    }
}

fn write_file(root: &std::path::Path, rel: &str, body: &[u8]) {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

#[tokio::test]
async fn full_cycle_against_real_s3() {
    let bucket = require_bucket!();
    let prefix = unique_prefix("cycle");
    let remote_uri = format!("s3://{bucket}/{prefix}");

    let client = S3Client::from_env().await;
    let s3_uri = S3Uri::parse(&remote_uri).unwrap();
    init_remote(&client, &s3_uri, DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();
    let remote = S3Store::new(client.clone(), s3_uri.clone());

    // Repo A: init local, write a few files (one bigger than chunk_size to
    // exercise multi-chunk and multipart paths), commit, push.
    let dir_a = tempfile::tempdir().unwrap();
    let repo_a = Repo::init(dir_a.path(), &remote_uri).await.unwrap();
    let store_a = repo_a.store().await.unwrap();
    write_file(dir_a.path(), "art/cat.png", b"small image bytes");
    let big = vec![0xCD; 9 * 1024 * 1024]; // 9 MB → multipart on push
    write_file(dir_a.path(), "art/big.bin", &big);

    stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
    let c1 = stage::commit(&repo_a, &store_a, "alice", "first")
        .await
        .unwrap();

    let pushed = push(&repo_a, &remote).await.unwrap().unwrap();
    assert_eq!(pushed, c1);

    // Repo B: clone; verify materialized files match.
    let dir_b = tempfile::tempdir().unwrap();
    let repo_b = clone_repo(&remote, &remote_uri, dir_b.path())
        .await
        .unwrap();
    assert_eq!(repo_b.head().await.unwrap(), Some(c1.clone()));
    assert_eq!(
        std::fs::read(dir_b.path().join("art/cat.png")).unwrap(),
        b"small image bytes"
    );
    assert_eq!(
        std::fs::read(dir_b.path().join("art/big.bin"))
            .unwrap()
            .len(),
        big.len()
    );

    // Repo A: edit, commit, push. Repo B: pull; sees the change.
    write_file(dir_a.path(), "art/cat.png", b"updated image bytes!");
    stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
    let c2 = stage::commit(&repo_a, &store_a, "alice", "edit")
        .await
        .unwrap();
    push(&repo_a, &remote).await.unwrap();

    pull(&repo_b, &remote).await.unwrap();
    assert_eq!(repo_b.head().await.unwrap(), Some(c2));
    assert_eq!(
        std::fs::read(dir_b.path().join("art/cat.png")).unwrap(),
        b"updated image bytes!"
    );

    purge_prefix(&client, &bucket, &prefix).await;
}
