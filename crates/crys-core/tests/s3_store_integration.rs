//! S3 backend integration tests.
//!
//! Mirrors the `S3Client` integration suite in `tests/s3_integration.rs` but
//! exercises the `ObjectStore` trait through `S3Store`, plus the
//! `init_remote` bootstrap helper.
//!
//! Gated on `CRYS_TEST_BUCKET`; tests skip cleanly without it. Each test
//! isolates itself under `crys-test/<pid>-<nanos>-<salt>/` and best-effort
//! deletes its keys via a `Cleanup` guard.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crys_core::repo::{init_remote, DEFAULT_CHUNK_SIZE};
use crys_core::s3::S3Client;
use crys_core::store::{ObjectStore, S3Store};
use crys_core::{Error, Hash, S3Uri};

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

struct Cleanup<'a> {
    client: &'a S3Client,
    bucket: String,
    keys: Vec<String>,
}

impl<'a> Cleanup<'a> {
    fn new(client: &'a S3Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            keys: Vec::new(),
        }
    }

    fn track(&mut self, key: impl Into<String>) {
        self.keys.push(key.into());
    }

    async fn run(self) {
        for key in &self.keys {
            let _ = self.client.delete(&self.bucket, key).await;
        }
    }
}

macro_rules! require_bucket {
    () => {
        match bucket() {
            Some(b) => b,
            None => {
                eprintln!("skipping S3 store integration test: set CRYS_TEST_BUCKET to enable");
                return;
            }
        }
    };
}

async fn fresh_store(prefix: &str) -> (S3Client, S3Store, String) {
    let bucket = bucket().expect("CRYS_TEST_BUCKET");
    let client = S3Client::from_env().await;
    let uri = S3Uri::parse(&format!("s3://{bucket}/{prefix}")).unwrap();
    let store = S3Store::new(client.clone(), uri);
    (client, store, bucket)
}

#[tokio::test]
async fn store_round_trip() {
    let _ = require_bucket!();
    let prefix = unique_prefix("rt");
    let (client, store, bucket) = fresh_store(&prefix).await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let payload = Bytes::from_static(b"hello chrysalis");
    let hash = Hash::of(&payload);
    cleanup.track(format!("{prefix}/objects/{}", hash.storage_path()));

    assert!(!store.has(&hash).await.unwrap());
    store.put(&hash, payload.clone()).await.unwrap();
    assert!(store.has(&hash).await.unwrap());
    assert_eq!(store.get(&hash).await.unwrap(), payload);

    cleanup.run().await;
}

#[tokio::test]
async fn store_get_missing_returns_not_found() {
    let _ = require_bucket!();
    let prefix = unique_prefix("missing");
    let (_client, store, _bucket) = fresh_store(&prefix).await;

    let hash = Hash::of(b"never-written");
    match store.get(&hash).await {
        Err(Error::NotFound { .. }) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn store_list_returns_only_object_hashes() {
    let _ = require_bucket!();
    let prefix = unique_prefix("list");
    let (client, store, bucket) = fresh_store(&prefix).await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let mut written = Vec::new();
    for i in 0u8..3 {
        let payload = Bytes::from(vec![i; 1]);
        let hash = Hash::of(&payload);
        store.put(&hash, payload).await.unwrap();
        cleanup.track(format!("{prefix}/objects/{}", hash.storage_path()));
        written.push(hash);
    }

    let mut listed = store.list().await.unwrap();
    listed.sort();
    written.sort();
    assert_eq!(listed, written);

    cleanup.run().await;
}

#[tokio::test]
async fn store_head_round_trip() {
    let _ = require_bucket!();
    let prefix = unique_prefix("head");
    let (client, store, bucket) = fresh_store(&prefix).await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());
    cleanup.track(format!("{prefix}/HEAD"));

    assert!(store.get_head().await.unwrap().is_none());

    let h = Hash::of(b"commit-1");
    store.put_head(Some(&h)).await.unwrap();
    assert_eq!(store.get_head().await.unwrap(), Some(h.clone()));

    let h2 = Hash::of(b"commit-2");
    store.put_head(Some(&h2)).await.unwrap();
    assert_eq!(store.get_head().await.unwrap(), Some(h2));

    store.put_head(None).await.unwrap();
    assert!(store.get_head().await.unwrap().is_none());

    cleanup.run().await;
}

/// 9 MB payload exceeds the 8 MB multipart threshold and forces the
/// multipart code path.
#[tokio::test]
async fn put_multipart_round_trips_large_payload() {
    let _ = require_bucket!();
    let prefix = unique_prefix("mp");
    let (client, store, bucket) = fresh_store(&prefix).await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let mut payload = Vec::with_capacity(9 * 1024 * 1024);
    for i in 0..(9 * 1024 * 1024) {
        payload.push((i % 251) as u8);
    }
    let payload = Bytes::from(payload);
    let hash = Hash::of(&payload);
    cleanup.track(format!("{prefix}/objects/{}", hash.storage_path()));

    store.put(&hash, payload.clone()).await.unwrap();
    let got = store.get(&hash).await.unwrap();
    assert_eq!(got.len(), payload.len());
    assert_eq!(got, payload);

    cleanup.run().await;
}

#[tokio::test]
async fn init_remote_creates_config_and_head() {
    let _ = require_bucket!();
    let prefix = unique_prefix("init");
    let (client, _store, bucket) = fresh_store(&prefix).await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());
    cleanup.track(format!("{prefix}/config.json"));
    cleanup.track(format!("{prefix}/HEAD"));

    let uri = S3Uri::parse(&format!("s3://{bucket}/{prefix}")).unwrap();
    init_remote(&client, &uri, DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();

    assert!(client
        .head(&bucket, &format!("{prefix}/config.json"))
        .await
        .unwrap());
    assert!(client
        .head(&bucket, &format!("{prefix}/HEAD"))
        .await
        .unwrap());

    let head_bytes = client
        .get(&bucket, &format!("{prefix}/HEAD"))
        .await
        .unwrap();
    assert!(head_bytes.is_empty(), "fresh HEAD must be empty");

    cleanup.run().await;
}

#[tokio::test]
async fn init_remote_refuses_existing_repo() {
    let _ = require_bucket!();
    let prefix = unique_prefix("init-twice");
    let (client, _store, bucket) = fresh_store(&prefix).await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());
    cleanup.track(format!("{prefix}/config.json"));
    cleanup.track(format!("{prefix}/HEAD"));

    let uri = S3Uri::parse(&format!("s3://{bucket}/{prefix}")).unwrap();
    init_remote(&client, &uri, DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();

    let err = init_remote(&client, &uri, DEFAULT_CHUNK_SIZE)
        .await
        .expect_err("second init must fail");
    assert!(matches!(err, Error::RepoExists(_)), "got {err:?}");

    cleanup.run().await;
}
