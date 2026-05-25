//! Filesystem-backed `ObjectStore`. Used for the local cache under
//! `.crys/objects/` and (in tests) as a stand-in for a real backend.
//!
//! Writes go through a tempfile + atomic rename so an interrupted process
//! never leaves a half-written object visible to a concurrent reader. The
//! design's "all object writes are idempotent (content-addressed)"
//! recovery guarantee (§10) depends on this.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{HeadToken, ObjectStore};
use crate::{Error, Hash, Result};

const HEAD_FILE: &str = "HEAD";
const OBJECTS_DIR: &str = "objects";

/// Filesystem object store rooted at `<root>/`. Owns `<root>/objects/<ab>/<cdef…>`
/// for content and `<root>/HEAD` for the mutable pointer.
#[derive(Debug, Clone)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    /// Open a store rooted at `root`, creating `objects/` and an empty `HEAD`
    /// if they don't exist yet.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join(OBJECTS_DIR)).await?;
        let head_path = root.join(HEAD_FILE);
        if !head_path.exists() {
            fs::write(&head_path, b"").await?;
        }
        Ok(Self { root })
    }

    fn object_path(&self, hash: &Hash) -> PathBuf {
        self.root.join(OBJECTS_DIR).join(hash.storage_path())
    }

    fn head_path(&self) -> PathBuf {
        self.root.join(HEAD_FILE)
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let path = self.object_path(hash);
        match fs::read(&path).await {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::NotFound {
                bucket: "local".into(),
                key: hash.as_hex().to_string(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        let path = self.object_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        atomic_write(&path, &bytes).await
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        Ok(fs::try_exists(self.object_path(hash)).await?)
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        let objects_root = self.root.join(OBJECTS_DIR);
        let mut hashes = Vec::new();
        let mut shards = match fs::read_dir(&objects_root).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(hashes),
            Err(e) => return Err(e.into()),
        };
        while let Some(shard) = shards.next_entry().await? {
            if !shard.file_type().await?.is_dir() {
                continue;
            }
            let shard_name = match shard.file_name().to_str() {
                Some(s) if s.len() == 2 => s.to_string(),
                _ => continue,
            };
            let mut entries = fs::read_dir(shard.path()).await?;
            while let Some(entry) = entries.next_entry().await? {
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if name.len() != 62 {
                    continue;
                }
                if let Ok(hash) = Hash::from_hex(format!("{shard_name}{name}")) {
                    hashes.push(hash);
                }
            }
        }
        Ok(hashes)
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        let path = self.object_path(hash);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_head(&self) -> Result<Option<Hash>> {
        let bytes = match fs::read(self.head_path()).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let trimmed = std::str::from_utf8(&bytes)
            .map_err(|_| Error::InvalidHash("HEAD is not utf-8".into()))?
            .trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        Ok(Some(Hash::from_hex(trimmed)?))
    }

    async fn put_head(&self, head: Option<&Hash>) -> Result<()> {
        let path = self.head_path();
        let bytes = head
            .map(|h| h.as_hex().as_bytes().to_vec())
            .unwrap_or_default();
        atomic_write(&path, &bytes).await
    }

    async fn get_head_with_token(&self) -> Result<(Option<Hash>, HeadToken)> {
        let head = self.get_head().await?;
        Ok((head.clone(), local_token(head.as_ref())))
    }

    async fn compare_and_set_head(
        &self,
        expected: &HeadToken,
        new: Option<&Hash>,
    ) -> Result<HeadToken> {
        // The local store is per-process; CAS is a courtesy for trait
        // conformance and tests. Compare under the same `get_head` snapshot we
        // immediately overwrite — racy across threads, but `LocalStore` is
        // never shared between concurrent writers in practice.
        let current = self.get_head().await?;
        if local_token(current.as_ref()) != *expected {
            return Err(Error::PreconditionFailed {
                bucket: "local".into(),
                key: "HEAD".into(),
            });
        }
        self.put_head(new).await?;
        Ok(local_token(new))
    }
}

fn local_token(head: Option<&Hash>) -> HeadToken {
    match head {
        Some(h) => HeadToken::new(h.as_hex().to_string()),
        None => HeadToken::absent(),
    }
}

/// Write `bytes` to `path` atomically via tempfile + rename. The tempfile
/// lives in the same directory as `path` so the rename is on the same
/// filesystem.
async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("object path has no parent"))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".crys-tmp-")
        .tempfile_in(parent)?;
    {
        let file = temp.as_file_mut();
        let mut tokio_file = tokio::fs::File::from_std(file.try_clone()?);
        tokio_file.write_all(bytes).await?;
        tokio_file.flush().await?;
    }
    temp.persist(path)
        .map_err(|e| std::io::Error::other(format!("persist tempfile: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fresh() -> (tempfile::TempDir, LocalStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).await.unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn round_trip() {
        let (_dir, store) = fresh().await;
        crate::store::conformance::round_trip(&store).await;
    }

    #[tokio::test]
    async fn missing_returns_not_found() {
        let (_dir, store) = fresh().await;
        crate::store::conformance::missing_returns_not_found(&store).await;
    }

    #[tokio::test]
    async fn list_returns_all_keys() {
        let (_dir, store) = fresh().await;
        crate::store::conformance::list_returns_all_keys(&store).await;
    }

    #[tokio::test]
    async fn head_round_trip() {
        let (_dir, store) = fresh().await;
        crate::store::conformance::head_round_trip(&store).await;
    }

    #[tokio::test]
    async fn cas_head_serializes_writers() {
        let (_dir, store) = fresh().await;
        crate::store::conformance::cas_head_serializes_writers(&store).await;
    }

    #[tokio::test]
    async fn open_creates_objects_dir_and_empty_head() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).await.unwrap();
        assert!(dir.path().join("objects").is_dir());
        assert!(dir.path().join("HEAD").is_file());
        assert!(store.get_head().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn objects_land_at_sharded_path() {
        let (dir, store) = fresh().await;
        let payload = Bytes::from_static(b"path test");
        let hash = Hash::of(&payload);
        store.put(&hash, payload).await.unwrap();
        let on_disk = dir.path().join("objects").join(hash.storage_path());
        assert!(on_disk.is_file(), "{on_disk:?} should exist");
    }
}
