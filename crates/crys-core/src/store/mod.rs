//! Content-addressed object stores.
//!
//! The [`ObjectStore`] trait is the seam that keeps S3 specifics out of the
//! rest of the code (design §9). Both [`LocalStore`] (filesystem) and
//! [`MemoryStore`] (HashMap, for tests) implement it; the S3-backed store
//! lands in Phase 4.
//!
//! All implementations follow design §4's storage rules:
//!
//! - The key under `objects/` is the hash's `<ab>/<cdef…>` storage path.
//! - Object bytes are passed through verbatim — `put` does not gzip-wrap and
//!   `get` does not gunzip. The caller already has `storage_bytes()` from
//!   [`crate::objects::CanonicalJson`].
//! - The `HEAD` pointer is mutable; everything under `objects/` is immutable.

mod local;
mod memory;

pub use local::LocalStore;
pub use memory::MemoryStore;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Hash, Result};

/// Async store of content-addressed objects plus one mutable `HEAD` pointer.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch an object's storage bytes (gzip-wrapped JSON for non-chunks; raw
    /// bytes for chunks). Returns [`crate::Error::NotFound`] if absent.
    async fn get(&self, hash: &Hash) -> Result<Bytes>;

    /// Idempotent insert. Writing the same hash twice is a no-op (content is
    /// identical by definition).
    async fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()>;

    /// Cheap presence check.
    async fn has(&self, hash: &Hash) -> Result<bool>;

    /// Enumerate every hash currently stored. Order is unspecified.
    async fn list(&self) -> Result<Vec<Hash>>;

    /// Read `HEAD`. Returns `Ok(None)` if `HEAD` is empty (no commits yet).
    /// A missing `HEAD` is also treated as `None` so a freshly-cloned repo
    /// looks identical to a freshly-initialized one.
    async fn get_head(&self) -> Result<Option<Hash>>;

    /// Overwrite `HEAD`. `None` writes an empty `HEAD` (the "no commits yet"
    /// sentinel).
    async fn put_head(&self, head: Option<&Hash>) -> Result<()>;
}

/// Conformance test suite — call this from the impl-specific test modules.
/// Lives behind `cfg(test)` so it doesn't bloat the public API surface.
#[cfg(test)]
pub(crate) mod conformance {
    use super::*;
    use crate::{Error, Hash};

    pub async fn round_trip<S: ObjectStore>(store: &S) {
        let payload = Bytes::from_static(b"hello chrysalis");
        let hash = Hash::of(&payload);

        assert!(!store.has(&hash).await.unwrap());
        store.put(&hash, payload.clone()).await.unwrap();
        assert!(store.has(&hash).await.unwrap());
        assert_eq!(store.get(&hash).await.unwrap(), payload);

        // Double-write is a no-op.
        store.put(&hash, payload.clone()).await.unwrap();
        assert_eq!(store.get(&hash).await.unwrap(), payload);
    }

    pub async fn missing_returns_not_found<S: ObjectStore>(store: &S) {
        let hash = Hash::of(b"never-written");
        match store.get(&hash).await {
            Err(Error::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    pub async fn list_returns_all_keys<S: ObjectStore>(store: &S) {
        let mut written = Vec::new();
        for i in 0u8..5 {
            let payload = Bytes::from(vec![i; 1]);
            let hash = Hash::of(&payload);
            store.put(&hash, payload).await.unwrap();
            written.push(hash);
        }
        let mut listed = store.list().await.unwrap();
        listed.sort();
        written.sort();
        assert_eq!(listed, written);
    }

    pub async fn head_round_trip<S: ObjectStore>(store: &S) {
        assert!(store.get_head().await.unwrap().is_none());

        let h = Hash::of(b"some-commit");
        store.put_head(Some(&h)).await.unwrap();
        assert_eq!(store.get_head().await.unwrap(), Some(h.clone()));

        // Overwrite with a different value.
        let h2 = Hash::of(b"another-commit");
        store.put_head(Some(&h2)).await.unwrap();
        assert_eq!(store.get_head().await.unwrap(), Some(h2));

        // Clear back to empty.
        store.put_head(None).await.unwrap();
        assert!(store.get_head().await.unwrap().is_none());
    }
}
