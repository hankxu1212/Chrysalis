//! Garbage collection of unreachable objects in the local store.
//!
//! `crys reset` (and similar pointer-moving operations) leaves orphan
//! objects in `.crys/objects/` — chunks and manifests that no live
//! reference points at anymore. This module sweeps them.
//!
//! An object is "live" iff it's reachable from the union of:
//!
//! - the current `HEAD`,
//! - the last-observed `REMOTE_HEAD`,
//! - every file currently referenced by the index (covers staged-but-
//!   not-committed work, which would otherwise be considered garbage).
//!
//! Anything else under `.crys/objects/` is removed. We do **not** touch
//! the remote — `S3Store::delete` returns an error by design.
//!
//! This is a local operation; future Chrysalis revisions can grow a
//! separate "remote gc" path with its own affordances.

use std::collections::HashSet;

use crate::objects::{CanonicalJson, FileBody};
use crate::repo::Repo;
use crate::store::ObjectStore;
use crate::sync::{walk_reachable, WalkScope};
use crate::{Hash, Result};

/// Outcome of a GC sweep.
#[derive(Debug, Default, Clone)]
pub struct GcReport {
    /// Hashes that were (or would be, in dry-run mode) removed.
    pub removed: Vec<Hash>,
    /// Hashes considered live and kept.
    pub kept: usize,
}

/// Sweep unreachable objects from `store`. If `dry_run` is true, the report
/// lists what *would* be removed without actually deleting anything.
pub async fn gc<S: ObjectStore>(repo: &Repo, store: &S, dry_run: bool) -> Result<GcReport> {
    // 1. Build the live set: union of HEAD, REMOTE_HEAD, and every
    //    file_hash currently in the index.
    let mut live: HashSet<Hash> = HashSet::new();

    if let Some(head) = repo.head().await? {
        let reachable = walk_reachable(store, &head, WalkScope::Full, None).await?;
        extend_live(&mut live, reachable);
    }
    if let Some(remote_head) = repo.remote_head().await? {
        let reachable = walk_reachable(store, &remote_head, WalkScope::Full, None).await?;
        extend_live(&mut live, reachable);
    }

    let index = repo.read_index().await?;
    for entry in index.entries.values() {
        // The file body is a manifest pointing at chunks. We need both the
        // manifest itself and its chunks to be considered live.
        if live.insert(entry.file_hash.clone()) {
            // Newly added — also pull in its chunks. If the manifest has
            // already been counted, walking again would be redundant.
            if let Ok(bytes) = store.get(&entry.file_hash).await {
                if let Ok(body) = FileBody::from_storage_bytes(&bytes) {
                    for chunk in body.chunks {
                        live.insert(chunk);
                    }
                }
            }
        }
    }

    // 2. Diff against the on-disk listing.
    let on_disk = store.list().await?;
    let mut removed = Vec::new();
    for hash in on_disk {
        if live.contains(&hash) {
            continue;
        }
        if !dry_run {
            store.delete(&hash).await?;
        }
        removed.push(hash);
    }

    Ok(GcReport {
        kept: live.len(),
        removed,
    })
}

fn extend_live(live: &mut HashSet<Hash>, reachable: crate::sync::ReachableSet) {
    for h in reachable.commits {
        live.insert(h);
    }
    for h in reachable.trees {
        live.insert(h);
    }
    for h in reachable.files {
        live.insert(h);
    }
    for h in reachable.chunks {
        live.insert(h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::{add, commit, reset, ResetMode};
    use crate::store::MemoryStore;

    async fn fresh_repo() -> (tempfile::TempDir, Repo, MemoryStore) {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "s3://test/repo").await.unwrap();
        (dir, repo, MemoryStore::new())
    }

    fn write_file(root: &std::path::Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    #[tokio::test]
    async fn gc_keeps_everything_reachable_from_head() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"hello");
        add(&repo, &store, dir.path()).await.unwrap();
        commit(&repo, &store, "tester", "first").await.unwrap();

        let before = store.list().await.unwrap().len();
        let report = gc(&repo, &store, false).await.unwrap();
        let after = store.list().await.unwrap().len();
        assert!(
            report.removed.is_empty(),
            "nothing reachable should be GC'd"
        );
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn gc_removes_orphans_after_reset_mixed() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"v1");
        add(&repo, &store, dir.path()).await.unwrap();
        commit(&repo, &store, "tester", "first").await.unwrap();

        // Stage a *different* version — those new chunks/manifest are not
        // pointed at by HEAD, but the index still references them.
        write_file(dir.path(), "a.txt", b"v2-different");
        add(&repo, &store, dir.path()).await.unwrap();
        let before = store.list().await.unwrap().len();

        // Mixed reset: index reverts to HEAD, so the v2 chunks/manifest
        // become orphans.
        reset(&repo, &store, None, ResetMode::Mixed).await.unwrap();

        let report = gc(&repo, &store, false).await.unwrap();
        assert!(!report.removed.is_empty(), "should have removed v2 objects");
        let after = store.list().await.unwrap().len();
        assert_eq!(after, before - report.removed.len());
    }

    #[tokio::test]
    async fn gc_keeps_staged_uncommitted_objects() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"hello");
        add(&repo, &store, dir.path()).await.unwrap();
        // No commit yet — but the staged file's manifest+chunks must survive.

        let before = store.list().await.unwrap().len();
        let report = gc(&repo, &store, false).await.unwrap();
        assert!(
            report.removed.is_empty(),
            "staged-but-uncommitted objects must be considered live"
        );
        assert_eq!(before, store.list().await.unwrap().len());
    }

    #[tokio::test]
    async fn gc_dry_run_does_not_delete() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"v1");
        add(&repo, &store, dir.path()).await.unwrap();
        commit(&repo, &store, "tester", "first").await.unwrap();
        write_file(dir.path(), "a.txt", b"v2-different");
        add(&repo, &store, dir.path()).await.unwrap();
        reset(&repo, &store, None, ResetMode::Mixed).await.unwrap();

        let before = store.list().await.unwrap().len();
        let report = gc(&repo, &store, true).await.unwrap();
        assert!(!report.removed.is_empty());
        assert_eq!(
            before,
            store.list().await.unwrap().len(),
            "dry run must not delete"
        );
    }
}
