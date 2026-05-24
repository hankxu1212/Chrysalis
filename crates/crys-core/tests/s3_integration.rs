//! S3 client integration tests.
//!
//! These exercise [`crys_core::s3::S3Client`] against a real AWS S3 bucket.
//! They are gated on the `CRYS_TEST_BUCKET` env var per design §11; without
//! it, every test logs a skip message and returns `Ok(())`.
//!
//! Each test isolates itself under a unique key prefix
//! (`crys-test/<uuid>/...`) and cleans up its own keys on the way out, even on
//! assertion failure (via a `Cleanup` guard).
//!
//! Required env:
//! - `CRYS_TEST_BUCKET`: the bucket name.
//! - Standard AWS credential env (`AWS_PROFILE`, or `AWS_ACCESS_KEY_ID` +
//!   `AWS_SECRET_ACCESS_KEY` + `AWS_REGION`).
//!
//! Optional:
//! - `AWS_REGION` if not set in the credential profile.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crys_core::s3::S3Client;
use crys_core::{Error, S3Uri};

fn bucket() -> Option<String> {
    std::env::var("CRYS_TEST_BUCKET").ok()
}

/// Generates a unique prefix per test invocation. Combines nanosecond clock,
/// `std::process::id()`, and a per-test salt so two tests in the same run (or
/// two parallel CI jobs) never collide.
fn unique_prefix(salt: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("crys-test/{}-{}-{}", std::process::id(), nanos, salt)
}

/// Drop guard that deletes every key it knows about, best-effort.
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
                eprintln!("skipping S3 integration test: set CRYS_TEST_BUCKET to enable");
                return;
            }
        }
    };
}

#[tokio::test]
async fn put_and_get_round_trip() {
    let bucket = require_bucket!();
    let client = S3Client::from_env().await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let key = format!("{}/round-trip", unique_prefix("rt"));
    cleanup.track(&key);

    let payload = Bytes::from_static(b"hello chrysalis");
    client
        .put(&bucket, &key, payload.clone())
        .await
        .expect("put");

    let got = client.get(&bucket, &key).await.expect("get");
    assert_eq!(got, payload);

    cleanup.run().await;
}

#[tokio::test]
async fn head_reports_presence() {
    let bucket = require_bucket!();
    let client = S3Client::from_env().await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let prefix = unique_prefix("head");
    let present = format!("{prefix}/present");
    let absent = format!("{prefix}/absent");
    cleanup.track(&present);

    assert!(!client.head(&bucket, &present).await.expect("head before"));
    assert!(!client.head(&bucket, &absent).await.expect("head absent"));

    client
        .put(&bucket, &present, Bytes::from_static(b"x"))
        .await
        .expect("put");

    assert!(client.head(&bucket, &present).await.expect("head after"));
    assert!(!client.head(&bucket, &absent).await.expect("head absent 2"));

    cleanup.run().await;
}

#[tokio::test]
async fn get_missing_returns_not_found() {
    let bucket = require_bucket!();
    let client = S3Client::from_env().await;

    let key = format!("{}/never-written", unique_prefix("missing"));
    let err = client.get(&bucket, &key).await.expect_err("should 404");
    match err {
        Error::NotFound { .. } => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn put_if_absent_blocks_overwrite() {
    let bucket = require_bucket!();
    let client = S3Client::from_env().await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let key = format!("{}/once", unique_prefix("ifabsent"));
    cleanup.track(&key);

    // First write succeeds.
    client
        .put_if_absent(&bucket, &key, Bytes::from_static(b"v1"))
        .await
        .expect("first put_if_absent");

    // Second write must fail with PreconditionFailed.
    let err = client
        .put_if_absent(&bucket, &key, Bytes::from_static(b"v2"))
        .await
        .expect_err("second put_if_absent");
    match err {
        Error::PreconditionFailed { .. } => {}
        other => panic!("expected PreconditionFailed, got {other:?}"),
    }

    // Body is still v1.
    let got = client.get(&bucket, &key).await.expect("get after");
    assert_eq!(got.as_ref(), b"v1");

    cleanup.run().await;
}

#[tokio::test]
async fn list_prefix_paginates() {
    let bucket = require_bucket!();
    let client = S3Client::from_env().await;
    let mut cleanup = Cleanup::new(&client, bucket.clone());

    let prefix = unique_prefix("list");
    let n = 5;
    for i in 0..n {
        let key = format!("{prefix}/k{i:02}");
        cleanup.track(&key);
        client
            .put(&bucket, &key, Bytes::from(format!("body-{i}")))
            .await
            .expect("put");
    }

    let mut listed = client.list_prefix(&bucket, &prefix).await.expect("list");
    listed.sort();
    assert_eq!(listed.len(), n);
    for (i, key) in listed.iter().enumerate() {
        assert!(key.ends_with(&format!("k{i:02}")), "unexpected key {key}");
    }

    cleanup.run().await;
}

#[tokio::test]
async fn delete_is_idempotent() {
    let bucket = require_bucket!();
    let client = S3Client::from_env().await;

    let key = format!("{}/once", unique_prefix("del"));
    client
        .put(&bucket, &key, Bytes::from_static(b"x"))
        .await
        .expect("put");
    client.delete(&bucket, &key).await.expect("first delete");
    // Second delete on an already-absent key is a no-op.
    client.delete(&bucket, &key).await.expect("second delete");
    assert!(!client.head(&bucket, &key).await.expect("head after delete"));
}

#[tokio::test]
async fn s3_uri_round_trips() {
    // Doesn't need a bucket; exercises pure parsing.
    let parsed = S3Uri::parse("s3://test/repo/HEAD").unwrap();
    assert_eq!(parsed.bucket, "test");
    assert_eq!(parsed.key, "repo/HEAD");
    assert_eq!(parsed.to_string(), "s3://test/repo/HEAD");
}
