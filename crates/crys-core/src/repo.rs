//! `.crys/` repository layout: discovery, config, HEAD, REMOTE_HEAD, index.
//!
//! See design §6 for the on-disk layout and §7 for the index format. This
//! module owns the *files* under `.crys/`; the `objects/` subtree is owned by
//! [`crate::store::LocalStore`].
//!
//! All writes go through the store's atomic-write path or `serde_json` plus
//! tempfile rename, so an interrupted process never leaves a half-written
//! config/HEAD/index.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::store::LocalStore;
use crate::{Error, Hash, Result};

/// Default chunk size in bytes (8 MB), matching design §4.
pub const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

const CRYS_DIR: &str = ".crys";
const CONFIG_FILE: &str = "config";
const HEAD_FILE: &str = "HEAD";
const REMOTE_HEAD_FILE: &str = "REMOTE_HEAD";
const INDEX_FILE: &str = "index";

/// `.crys/config` shape — the repo-local config (design §6).
///
/// Distinct from the on-S3 `config.json` (design §5) which carries
/// `format_version`, `chunk_size`, `created_at`. Both are written by
/// [`Repo::init`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub remote: String,
    pub region: Option<String>,
    pub chunk_size: u64,
}

/// One entry in `.crys/index` (design §7).
///
/// `mtime` is RFC 3339 UTC. Recorded so a future fast-path `add` can skip
/// re-hashing files that haven't changed; v1 doesn't use it but the field is
/// in the on-disk format from day one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub file_hash: Hash,
    pub mtime: String,
    pub size: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexFile {
    pub entries: BTreeMap<String, IndexEntry>,
}

/// On-S3 `config.json` (design §5). Distinct from [`Config`] which is the
/// per-clone local config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    pub format_version: u32,
    pub chunk_size: u64,
    pub created_at: DateTime<Utc>,
}

impl RemoteConfig {
    pub fn new(chunk_size: u64) -> Self {
        Self {
            format_version: 1,
            chunk_size,
            created_at: Utc::now(),
        }
    }
}

/// A handle to a Chrysalis repository. Construct via [`Repo::init`] for new
/// repos or [`Repo::open`] for existing ones.
#[derive(Debug, Clone)]
pub struct Repo {
    workdir: PathBuf,
    crys_dir: PathBuf,
    config: Config,
}

impl Repo {
    /// Initialize a new `.crys/` directory at `workdir`. Fails with
    /// [`Error::RepoExists`] if `.crys/` is already present.
    ///
    /// This sets up the local-only state. Phase 4's `crys init` call site
    /// also writes the remote `config.json` and `HEAD` to S3 — that's not
    /// done here so the function stays independently testable without AWS.
    pub async fn init(workdir: impl Into<PathBuf>, remote: impl Into<String>) -> Result<Self> {
        let workdir = workdir.into();
        let crys_dir = workdir.join(CRYS_DIR);
        if crys_dir.exists() {
            return Err(Error::RepoExists(crys_dir.display().to_string()));
        }
        fs::create_dir_all(&crys_dir).await?;

        let config = Config {
            remote: remote.into(),
            region: None,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };
        write_json(&crys_dir.join(CONFIG_FILE), &config).await?;

        // HEAD, REMOTE_HEAD, index — all empty initially.
        fs::write(crys_dir.join(HEAD_FILE), b"").await?;
        fs::write(crys_dir.join(REMOTE_HEAD_FILE), b"").await?;
        write_json(&crys_dir.join(INDEX_FILE), &IndexFile::default()).await?;

        // Pre-create the local object store layout so later phases don't have
        // to special-case the freshly-init'd state.
        LocalStore::open(&crys_dir).await?;

        Ok(Self {
            workdir,
            crys_dir,
            config,
        })
    }

    /// Find and open the `.crys/` repo containing `start_dir` or any of its
    /// ancestors.
    pub async fn open(start_dir: impl AsRef<Path>) -> Result<Self> {
        let start = start_dir.as_ref();
        let mut current: Option<&Path> = Some(start);
        while let Some(dir) = current {
            let candidate = dir.join(CRYS_DIR);
            if candidate.is_dir() {
                let config_bytes = fs::read(candidate.join(CONFIG_FILE)).await?;
                let config: Config = serde_json::from_slice(&config_bytes)?;
                return Ok(Self {
                    workdir: dir.to_path_buf(),
                    crys_dir: candidate,
                    config,
                });
            }
            current = dir.parent();
        }
        Err(Error::NotARepo(start.display().to_string()))
    }

    /// Working-tree root (the parent of `.crys/`).
    pub fn workdir(&self) -> &Path {
        &self.workdir
    }

    /// `.crys/` directory.
    pub fn crys_dir(&self) -> &Path {
        &self.crys_dir
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Open a [`LocalStore`] backed by `.crys/objects/` and `.crys/HEAD`.
    pub async fn store(&self) -> Result<LocalStore> {
        LocalStore::open(&self.crys_dir).await
    }

    /// Read `.crys/HEAD` (the local commit pointer). `None` means no commits
    /// yet.
    pub async fn head(&self) -> Result<Option<Hash>> {
        read_hash_file(&self.crys_dir.join(HEAD_FILE)).await
    }

    /// Read `.crys/REMOTE_HEAD` (the last observed remote tip).
    pub async fn remote_head(&self) -> Result<Option<Hash>> {
        read_hash_file(&self.crys_dir.join(REMOTE_HEAD_FILE)).await
    }

    /// Overwrite `.crys/HEAD`.
    pub async fn set_head(&self, head: Option<&Hash>) -> Result<()> {
        write_hash_file(&self.crys_dir.join(HEAD_FILE), head).await
    }

    /// Overwrite `.crys/REMOTE_HEAD`.
    pub async fn set_remote_head(&self, head: Option<&Hash>) -> Result<()> {
        write_hash_file(&self.crys_dir.join(REMOTE_HEAD_FILE), head).await
    }

    pub async fn read_index(&self) -> Result<IndexFile> {
        let bytes = fs::read(self.crys_dir.join(INDEX_FILE)).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn write_index(&self, index: &IndexFile) -> Result<()> {
        write_json(&self.crys_dir.join(INDEX_FILE), index).await
    }
}

async fn read_hash_file(path: &Path) -> Result<Option<Hash>> {
    let bytes = match fs::read(path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let trimmed = std::str::from_utf8(&bytes)
        .map_err(|_| Error::InvalidHash(format!("{}: not utf-8", path.display())))?
        .trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(Hash::from_hex(trimmed)?))
}

async fn write_hash_file(path: &Path, hash: Option<&Hash>) -> Result<()> {
    let bytes = hash
        .map(|h| h.as_hex().as_bytes().to_vec())
        .unwrap_or_default();
    atomic_write_bytes(path, &bytes).await
}

async fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    atomic_write_bytes(path, &bytes).await
}

async fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("path has no parent"))?;
    fs::create_dir_all(parent).await?;
    let temp = tempfile::Builder::new()
        .prefix(".crys-tmp-")
        .tempfile_in(parent)?;
    fs::write(temp.path(), bytes).await?;
    temp.persist(path)
        .map_err(|e| std::io::Error::other(format!("persist tempfile: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_then_open_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "s3://bucket/prefix").await.unwrap();
        assert_eq!(repo.config().remote, "s3://bucket/prefix");
        assert_eq!(repo.config().chunk_size, DEFAULT_CHUNK_SIZE);
        assert!(repo.head().await.unwrap().is_none());
        assert!(repo.remote_head().await.unwrap().is_none());
        let index = repo.read_index().await.unwrap();
        assert!(index.entries.is_empty());

        // Re-open from a nested directory and find the same repo.
        let nested = dir.path().join("subdir");
        fs::create_dir_all(&nested).await.unwrap();
        let opened = Repo::open(&nested).await.unwrap();
        assert_eq!(opened.workdir(), repo.workdir());
        assert_eq!(opened.config().remote, "s3://bucket/prefix");
    }

    #[tokio::test]
    async fn init_refuses_existing_repo() {
        let dir = tempfile::tempdir().unwrap();
        Repo::init(dir.path(), "s3://b/p").await.unwrap();
        let err = Repo::init(dir.path(), "s3://b/p").await.unwrap_err();
        assert!(matches!(err, Error::RepoExists(_)));
    }

    #[tokio::test]
    async fn open_outside_repo_returns_not_a_repo() {
        let dir = tempfile::tempdir().unwrap();
        let err = Repo::open(dir.path()).await.unwrap_err();
        assert!(matches!(err, Error::NotARepo(_)));
    }

    #[tokio::test]
    async fn head_and_remote_head_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "s3://b/p").await.unwrap();

        let h = Hash::of(b"commit-1");
        repo.set_head(Some(&h)).await.unwrap();
        assert_eq!(repo.head().await.unwrap(), Some(h.clone()));

        repo.set_remote_head(Some(&h)).await.unwrap();
        assert_eq!(repo.remote_head().await.unwrap(), Some(h));

        repo.set_head(None).await.unwrap();
        assert!(repo.head().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn index_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "s3://b/p").await.unwrap();

        let mut entries = BTreeMap::new();
        entries.insert(
            "src/main.rs".into(),
            IndexEntry {
                file_hash: Hash::of(b"main"),
                mtime: "2026-05-24T12:00:00Z".into(),
                size: 1234,
            },
        );
        let index = IndexFile { entries };
        repo.write_index(&index).await.unwrap();

        let read_back = repo.read_index().await.unwrap();
        assert_eq!(read_back.entries.len(), 1);
        assert_eq!(read_back.entries["src/main.rs"].size, 1234);
    }

    #[tokio::test]
    async fn store_round_trips_through_repo() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "s3://b/p").await.unwrap();
        let store = repo.store().await.unwrap();

        let payload = bytes::Bytes::from_static(b"some object");
        let hash = Hash::of(&payload);
        crate::store::ObjectStore::put(&store, &hash, payload.clone())
            .await
            .unwrap();
        let got = crate::store::ObjectStore::get(&store, &hash).await.unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn remote_config_has_format_version_1() {
        let cfg = RemoteConfig::new(DEFAULT_CHUNK_SIZE);
        assert_eq!(cfg.format_version, 1);
        assert_eq!(cfg.chunk_size, DEFAULT_CHUNK_SIZE);
    }
}
