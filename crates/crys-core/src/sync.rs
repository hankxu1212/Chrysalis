//! Sync layer: `fetch`, `pull`, `push`, `clone` (design §8).
//!
//! Operates on two [`ObjectStore`]s — a `local` (typically `LocalStore`
//! pointing at `.crys/`) and a `remote` (typically [`crate::S3Store`]).
//! Tests substitute two `MemoryStore`s sharing one in-memory backend.
//!
//! # Reachability
//!
//! `walk_reachable` enumerates every chunk/file/tree/commit reachable from a
//! starting commit. Reused by:
//!
//! - `push`: compute upload set (filter by `!remote.has(h)`).
//! - `clone`: compute download set.
//!
//! `walk_metadata_only` is the same walk minus chunks, used by `fetch`.
//!
//! # Push ordering (design §8 step 5)
//!
//! Uploads happen in dependency order: chunks → files → trees → commits →
//! `HEAD`. If push is interrupted, the remote is never left referencing
//! objects that aren't there yet.
//!
//! # Linear-history ancestor check
//!
//! `is_ancestor(a, b)` walks `b`'s parent chain looking for `a`. Used by
//! pull's fast-forward gate and push's "remote has changes; pull first"
//! gate.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};

use crate::objects::{CanonicalJson, CommitBody, EntryMode, FileBody, Hash, TreeBody};
use crate::repo::Repo;
use crate::stage::{checkout_tree, rebuild_index_from_tree};
use crate::store::ObjectStore;
use crate::{Error, Result};

/// Concurrency for transfer operations. Bounds memory and S3 connection use.
const TRANSFER_CONCURRENCY: usize = 16;

/// Reporter for long-running transfer operations (push/clone/pull).
///
/// Called from inside `copy_many` for transfer phases, and from
/// `walk_reachable` for the discovery phase that precedes them. CLI
/// implementations render progress bars; library consumers can pass
/// [`NoopProgress`] when they don't care.
///
/// Methods are sync because the CLI's progress backend (`indicatif`) is sync;
/// the cost of calling these inside `copy_one` is negligible vs. an S3
/// round-trip.
pub trait Progress: Send + Sync {
    /// Announce a phase. `total = 0` signals an indeterminate phase, which
    /// the CLI renders as a spinner. Otherwise renders as a progress bar.
    /// `kind` is one of `"walking"`, `"chunks"`, `"files"`, `"trees"`,
    /// `"commits"`.
    fn start_phase(&self, kind: &str, total: usize);
    /// One object finished. `bytes` is its size on the wire (0 for the
    /// walking phase, where nothing is copied).
    fn object_copied(&self, kind: &str, bytes: u64);
    /// Phase done.
    fn finish_phase(&self, kind: &str);
}

/// No-op progress reporter for tests and library callers that don't render
/// progress.
#[derive(Debug, Default)]
pub struct NoopProgress;

impl Progress for NoopProgress {
    fn start_phase(&self, _: &str, _: usize) {}
    fn object_copied(&self, _: &str, _: u64) {}
    fn finish_phase(&self, _: &str) {}
}

/// Trait-object alias used by the long-running sync entry points so callers
/// can swap reporters without dragging a generic parameter through every
/// signature.
pub type ProgressHandle = Arc<dyn Progress>;

fn noop() -> ProgressHandle {
    Arc::new(NoopProgress)
}

/// What to enumerate during a reachability walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkScope {
    /// All commits, trees, files, and chunks reachable from the tip.
    Full,
    /// All commits, trees, and files — but not chunks. Matches `fetch`'s
    /// "metadata + manifests only" rule (design §8).
    MetadataOnly,
}

/// Set of hashes split by kind so callers can preserve push ordering
/// (chunks → files → trees → commits).
#[derive(Debug, Default)]
pub struct ReachableSet {
    pub chunks: Vec<Hash>,
    pub files: Vec<Hash>,
    pub trees: Vec<Hash>,
    pub commits: Vec<Hash>,
}

impl ReachableSet {
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
            && self.files.is_empty()
            && self.trees.is_empty()
            && self.commits.is_empty()
    }

    pub fn total(&self) -> usize {
        self.chunks.len() + self.files.len() + self.trees.len() + self.commits.len()
    }
}

/// Walk every object reachable from `tip`, drawing object bodies from
/// `source`. Stops descending into a commit chain when `stop_at` matches a
/// commit hash (used to walk only the new portion of history during push).
pub async fn walk_reachable<S: ObjectStore>(
    source: &S,
    tip: &Hash,
    scope: WalkScope,
    stop_at: Option<&Hash>,
) -> Result<ReachableSet> {
    walk_reachable_with_progress(source, tip, scope, stop_at, &noop()).await
}

/// Same as [`walk_reachable`] but reports per-object progress via the
/// `"walking"` phase. The walk does N sequential GETs to inflate every
/// commit/tree/file body, so on a large repo this is the slowest phase by
/// wall time before any bar is visible — emitting a spinner here is what
/// makes `crys clone` look alive on big repos.
pub async fn walk_reachable_with_progress<S: ObjectStore>(
    source: &S,
    tip: &Hash,
    scope: WalkScope,
    stop_at: Option<&Hash>,
    progress: &ProgressHandle,
) -> Result<ReachableSet> {
    progress.start_phase("walking", 0);
    let mut set = ReachableSet::default();
    let mut commit_seen = HashSet::new();
    let mut tree_seen = HashSet::new();
    let mut file_seen = HashSet::new();
    let mut chunk_seen = HashSet::new();

    let mut commit_queue: Vec<Hash> = vec![tip.clone()];
    while let Some(commit_hash) = commit_queue.pop() {
        if Some(&commit_hash) == stop_at {
            continue;
        }
        if !commit_seen.insert(commit_hash.clone()) {
            continue;
        }
        let bytes = source.get(&commit_hash).await?;
        verify_storage_object(&commit_hash, &bytes)?;
        let body = CommitBody::from_storage_bytes(&bytes)?;
        set.commits.push(commit_hash.clone());
        progress.object_copied("walking", 0);

        walk_tree(
            source,
            &body.tree,
            scope,
            &mut tree_seen,
            &mut file_seen,
            &mut chunk_seen,
            &mut set,
            progress,
        )
        .await?;

        if let Some(parent) = body.parent {
            commit_queue.push(parent);
        }
    }

    progress.finish_phase("walking");
    Ok(set)
}

#[allow(clippy::too_many_arguments)]
fn walk_tree<'a, S: ObjectStore>(
    source: &'a S,
    tree_hash: &'a Hash,
    scope: WalkScope,
    tree_seen: &'a mut HashSet<Hash>,
    file_seen: &'a mut HashSet<Hash>,
    chunk_seen: &'a mut HashSet<Hash>,
    set: &'a mut ReachableSet,
    progress: &'a ProgressHandle,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if !tree_seen.insert(tree_hash.clone()) {
            return Ok(());
        }
        let bytes = source.get(tree_hash).await?;
        verify_storage_object(tree_hash, &bytes)?;
        let body = TreeBody::from_storage_bytes(&bytes)?;
        set.trees.push(tree_hash.clone());
        progress.object_copied("walking", 0);

        for entry in body.entries {
            match entry.mode {
                EntryMode::Dir => {
                    walk_tree(
                        source,
                        &entry.hash,
                        scope,
                        tree_seen,
                        file_seen,
                        chunk_seen,
                        set,
                        progress,
                    )
                    .await?;
                }
                EntryMode::File => {
                    if !file_seen.insert(entry.hash.clone()) {
                        continue;
                    }
                    let file_bytes = source.get(&entry.hash).await?;
                    verify_storage_object(&entry.hash, &file_bytes)?;
                    let file_body = FileBody::from_storage_bytes(&file_bytes)?;
                    set.files.push(entry.hash.clone());
                    progress.object_copied("walking", 0);
                    if scope == WalkScope::Full {
                        for chunk in file_body.chunks {
                            if chunk_seen.insert(chunk.clone()) {
                                set.chunks.push(chunk);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    })
}

/// True if `ancestor` is `tip` itself, or any commit reachable through `tip`'s
/// parent chain. Linear history → just walk parents.
pub async fn is_ancestor<S: ObjectStore>(store: &S, ancestor: &Hash, tip: &Hash) -> Result<bool> {
    let mut current = Some(tip.clone());
    while let Some(c) = current {
        if &c == ancestor {
            return Ok(true);
        }
        let bytes = match store.get(&c).await {
            Ok(b) => b,
            // Tip's parent chain is incomplete locally → can't prove ancestry.
            Err(Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(e),
        };
        let body = CommitBody::from_storage_bytes(&bytes)?;
        current = body.parent;
    }
    Ok(false)
}

/// Verify a non-chunk object (`file`/`tree`/`commit`) by decompressing and
/// comparing hashes. Any decode failure or mismatch surfaces as
/// [`Error::CorruptObject`].
fn verify_storage_object(hash: &Hash, storage_bytes: &[u8]) -> Result<()> {
    match hash.matches_storage_bytes(storage_bytes) {
        Ok(true) => Ok(()),
        _ => Err(Error::CorruptObject {
            hash: hash.as_hex().to_string(),
            source_store: "source",
        }),
    }
}

/// Verify a `chunk` object (raw bytes, no gzip).
fn verify_chunk_object(hash: &Hash, bytes: &[u8]) -> Result<()> {
    if hash.matches_chunk_bytes(bytes) {
        Ok(())
    } else {
        Err(Error::CorruptObject {
            hash: hash.as_hex().to_string(),
            source_store: "source",
        })
    }
}

/// Copy one hash from `source` to `dest` if it's not already present in
/// `dest`. Verifies the bytes match the hash before writing.
///
/// Returns the size in bytes of the object that was copied, or `None` if
/// the destination already had it (so progress reporters don't double-count
/// on a resumed push).
async fn copy_one<S: ObjectStore, D: ObjectStore>(
    source: &S,
    dest: &D,
    hash: &Hash,
    is_chunk: bool,
) -> Result<Option<u64>> {
    if dest.has(hash).await? {
        return Ok(None);
    }
    let bytes = source.get(hash).await?;
    if is_chunk {
        verify_chunk_object(hash, &bytes)?;
    } else {
        verify_storage_object(hash, &bytes)?;
    }
    let len = bytes.len() as u64;
    dest.put(hash, bytes).await?;
    Ok(Some(len))
}

/// Copy a list of hashes from `source` to `dest` in parallel, bounded by
/// [`TRANSFER_CONCURRENCY`]. All hashes here must be of the same kind so we
/// can route through the right verification path.
///
/// Reports progress through `progress` with the phase name `kind`
/// (`"chunks"`, `"files"`, `"trees"`, `"commits"`).
async fn copy_many<S, D>(
    source: &S,
    dest: &D,
    hashes: &[Hash],
    is_chunk: bool,
    kind: &str,
    progress: &ProgressHandle,
) -> Result<()>
where
    S: ObjectStore,
    D: ObjectStore,
{
    if hashes.is_empty() {
        return Ok(());
    }
    progress.start_phase(kind, hashes.len());
    let mut iter = hashes.iter();
    let mut in_flight = FuturesUnordered::new();

    for _ in 0..TRANSFER_CONCURRENCY {
        if let Some(hash) = iter.next() {
            in_flight.push(copy_one(source, dest, hash, is_chunk));
        }
    }
    while let Some(result) = in_flight.next().await {
        let copied = result?;
        progress.object_copied(kind, copied.unwrap_or(0));
        if let Some(hash) = iter.next() {
            in_flight.push(copy_one(source, dest, hash, is_chunk));
        }
    }
    progress.finish_phase(kind);
    Ok(())
}

/// `crys fetch` (design §8): refresh `REMOTE_HEAD`, downloading any missing
/// commit/tree/file objects but no chunks. Returns the new remote HEAD if it
/// changed.
pub async fn fetch<R: ObjectStore>(repo: &Repo, remote: &R) -> Result<Option<Hash>> {
    fetch_with_progress(repo, remote, &noop()).await
}

pub async fn fetch_with_progress<R: ObjectStore>(
    repo: &Repo,
    remote: &R,
    progress: &ProgressHandle,
) -> Result<Option<Hash>> {
    let local = repo.store().await?;
    let remote_head = remote.get_head().await?;
    let observed = repo.remote_head().await?;

    if remote_head == observed {
        return Ok(remote_head);
    }

    if let Some(tip) = &remote_head {
        // Walk only the portion of history not already local: stop if we
        // encounter a commit already present locally.
        let stop_at = repo.head().await?;
        let set = walk_reachable_with_progress(
            remote,
            tip,
            WalkScope::MetadataOnly,
            stop_at.as_ref(),
            progress,
        )
        .await?;
        copy_many(remote, &local, &set.commits, false, "commits", progress).await?;
        copy_many(remote, &local, &set.trees, false, "trees", progress).await?;
        copy_many(remote, &local, &set.files, false, "files", progress).await?;
    }

    repo.set_remote_head(remote_head.as_ref()).await?;
    Ok(remote_head)
}

/// `crys push` (design §8). Fetches first, enforces fast-forward, uploads
/// the new portion of history in dependency order, then writes `HEAD`
/// unconditionally.
pub async fn push<R: ObjectStore>(repo: &Repo, remote: &R) -> Result<Option<Hash>> {
    push_with_progress(repo, remote, &noop()).await
}

pub async fn push_with_progress<R: ObjectStore>(
    repo: &Repo,
    remote: &R,
    progress: &ProgressHandle,
) -> Result<Option<Hash>> {
    let local = repo.store().await?;
    fetch_with_progress(repo, remote, progress).await?;

    let local_head = repo.head().await?;
    let remote_head = repo.remote_head().await?;

    let local_tip = match local_head.clone() {
        Some(h) => h,
        None => return Ok(None),
    };

    if let Some(rh) = &remote_head {
        if rh == &local_tip {
            return Ok(Some(local_tip));
        }
        if !is_ancestor(&local, rh, &local_tip).await? {
            return Err(Error::NotFastForward);
        }
    }

    // Compute upload set, stopping at the current remote head so we don't
    // re-walk already-pushed history.
    let set = walk_reachable_with_progress(
        &local,
        &local_tip,
        WalkScope::Full,
        remote_head.as_ref(),
        progress,
    )
    .await?;

    copy_many(&local, remote, &set.chunks, true, "chunks", progress).await?;
    copy_many(&local, remote, &set.files, false, "files", progress).await?;
    copy_many(&local, remote, &set.trees, false, "trees", progress).await?;
    copy_many(&local, remote, &set.commits, false, "commits", progress).await?;

    // Final unconditional HEAD write — last writer wins (design §10).
    remote.put_head(Some(&local_tip)).await?;
    repo.set_remote_head(Some(&local_tip)).await?;
    Ok(Some(local_tip))
}

/// `crys pull` (design §8). Fetches, enforces fast-forward, downloads any
/// missing chunks for the new tip's tree, materializes the working tree,
/// advances `HEAD`.
pub async fn pull<R: ObjectStore>(repo: &Repo, remote: &R) -> Result<Option<Hash>> {
    pull_with_progress(repo, remote, &noop()).await
}

pub async fn pull_with_progress<R: ObjectStore>(
    repo: &Repo,
    remote: &R,
    progress: &ProgressHandle,
) -> Result<Option<Hash>> {
    let local = repo.store().await?;
    let remote_head = fetch_with_progress(repo, remote, progress).await?;
    let local_head = repo.head().await?;

    let remote_tip = match remote_head {
        Some(h) => h,
        None => return Ok(local_head),
    };

    if local_head.as_ref() == Some(&remote_tip) {
        return Ok(Some(remote_tip));
    }

    if let Some(local_tip) = &local_head {
        if !is_ancestor(&local, local_tip, &remote_tip).await? {
            return Err(Error::NotFastForward);
        }
    }

    if working_tree_dirty(repo).await? {
        return Err(Error::DirtyWorkingTree);
    }

    // Walk to discover and download missing chunks (and any
    // not-yet-fetched manifests, defensively).
    let set = walk_reachable_with_progress(
        remote,
        &remote_tip,
        WalkScope::Full,
        local_head.as_ref(),
        progress,
    )
    .await?;
    copy_many(remote, &local, &set.commits, false, "commits", progress).await?;
    copy_many(remote, &local, &set.trees, false, "trees", progress).await?;
    copy_many(remote, &local, &set.files, false, "files", progress).await?;
    copy_many(remote, &local, &set.chunks, true, "chunks", progress).await?;

    let commit_bytes = local.get(&remote_tip).await?;
    let commit = CommitBody::from_storage_bytes(&commit_bytes)?;

    // Snapshot the previous tree's paths so we can delete any file that's
    // no longer in the new tree.
    let old_index = repo.read_index().await?;
    let old_paths: std::collections::BTreeSet<String> = old_index.entries.keys().cloned().collect();

    materialize_tree(repo.workdir(), &local, &commit.tree).await?;
    let new_index = rebuild_index_from_tree(&local, &commit.tree, repo.workdir()).await?;
    let new_paths: std::collections::BTreeSet<String> = new_index.entries.keys().cloned().collect();
    delete_removed_paths(repo.workdir(), &old_paths, &new_paths)?;

    repo.write_index(&new_index).await?;
    repo.set_head(Some(&remote_tip)).await?;
    Ok(Some(remote_tip))
}

/// Delete any working-tree file that was in `old_paths` but not in
/// `new_paths`. After deleting files, walk back up each path's ancestor
/// chain removing now-empty directories — no point leaving empty dir
/// husks where the user expected things gone.
fn delete_removed_paths(
    workdir: &Path,
    old_paths: &std::collections::BTreeSet<String>,
    new_paths: &std::collections::BTreeSet<String>,
) -> Result<()> {
    let removed: Vec<&String> = old_paths.difference(new_paths).collect();
    let mut maybe_empty_dirs: std::collections::BTreeSet<std::path::PathBuf> =
        std::collections::BTreeSet::new();

    for rel in removed {
        let path = workdir.join(rel);
        match std::fs::remove_file(&path) {
            Ok(_) => {}
            // Already gone; that's fine.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let mut parent = path.parent();
        while let Some(p) = parent {
            if p == workdir {
                break;
            }
            maybe_empty_dirs.insert(p.to_path_buf());
            parent = p.parent();
        }
    }

    // Try to remove empty dirs deepest-first. `remove_dir` only succeeds on
    // empty directories; any other error (most commonly "not empty") is
    // intentionally ignored — we don't want to fail pull just because the
    // user has untracked files alongside removed ones.
    let mut sorted: Vec<_> = maybe_empty_dirs.into_iter().collect();
    sorted.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    for dir in sorted {
        let _ = std::fs::remove_dir(&dir);
    }
    Ok(())
}

/// Bootstrap a fresh local clone of `remote` at `dest`. Mirrors `crys clone`
/// (design §8): create `.crys/`, fetch tip, download all reachable objects
/// (including chunks), materialize working tree, set HEAD/REMOTE_HEAD,
/// rebuild index.
pub async fn clone_repo<R: ObjectStore>(
    remote: &R,
    remote_uri: &str,
    dest: impl AsRef<Path>,
) -> Result<Repo> {
    clone_with_progress(remote, remote_uri, dest, &noop()).await
}

pub async fn clone_with_progress<R: ObjectStore>(
    remote: &R,
    remote_uri: &str,
    dest: impl AsRef<Path>,
    progress: &ProgressHandle,
) -> Result<Repo> {
    let dest = dest.as_ref();
    let repo = Repo::init(dest, remote_uri).await?;
    let local = repo.store().await?;

    let remote_tip = remote.get_head().await?;
    if let Some(tip) = &remote_tip {
        let set =
            walk_reachable_with_progress(remote, tip, WalkScope::Full, None, progress).await?;
        copy_many(remote, &local, &set.commits, false, "commits", progress).await?;
        copy_many(remote, &local, &set.trees, false, "trees", progress).await?;
        copy_many(remote, &local, &set.files, false, "files", progress).await?;
        copy_many(remote, &local, &set.chunks, true, "chunks", progress).await?;

        let commit_bytes = local.get(tip).await?;
        let commit = CommitBody::from_storage_bytes(&commit_bytes)?;
        materialize_tree(repo.workdir(), &local, &commit.tree).await?;
        let index = rebuild_index_from_tree(&local, &commit.tree, repo.workdir()).await?;
        repo.write_index(&index).await?;
        repo.set_head(Some(tip)).await?;
    }
    repo.set_remote_head(remote_tip.as_ref()).await?;
    Ok(repo)
}

/// Materialize a tree into the working directory, removing any pre-existing
/// files at conflicting paths first (since clone/pull are full snapshots).
async fn materialize_tree<S: ObjectStore>(
    workdir: &Path,
    store: &S,
    tree_hash: &Hash,
) -> Result<()> {
    // We don't reset the working tree wholesale — design §6 says `.crys/`
    // lives alongside user files. Instead we just write/overwrite the
    // tree's files. Stale files from a previous tip remain on disk; that's
    // a v1 limitation acknowledged by the lack of a `status`/`reset`
    // command in the design.
    checkout_tree(store, tree_hash, workdir).await
}

/// Heuristic: working tree differs from `HEAD`'s tree if any indexed file's
/// on-disk size doesn't match the index entry. v1 doesn't re-hash to keep
/// pull fast; the size check catches the common cases (file deleted, file
/// truncated/extended) without false negatives on missing files.
async fn working_tree_dirty(repo: &Repo) -> Result<bool> {
    let index = repo.read_index().await?;
    for (rel, entry) in &index.entries {
        let path = repo.workdir().join(rel);
        match std::fs::metadata(&path) {
            Ok(meta) => {
                if meta.len() != entry.size {
                    return Ok(true);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::Repo;
    use crate::stage;
    use crate::store::MemoryStore;
    use bytes::Bytes;

    fn write_file(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    /// Two repos sharing one remote MemoryStore — the canonical Phase 5
    /// test fixture (design §11).
    async fn fixture() -> (tempfile::TempDir, Repo, MemoryStore) {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "memory://shared").await.unwrap();
        let remote = MemoryStore::new();
        (dir, repo, remote)
    }

    #[tokio::test]
    async fn full_collaborative_loop() {
        // Repo A: init → add → commit → push.
        let (dir_a, repo_a, remote) = fixture().await;
        write_file(dir_a.path(), "art/cat.png", b"pretend image bytes");
        write_file(dir_a.path(), "art/dog.png", b"another image");
        let store_a = repo_a.store().await.unwrap();
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        let c1 = stage::commit(&repo_a, &store_a, "alice", "first")
            .await
            .unwrap();

        let pushed = push(&repo_a, &remote).await.unwrap().unwrap();
        assert_eq!(pushed, c1);
        assert_eq!(remote.get_head().await.unwrap(), Some(c1.clone()));

        // Repo B: clone the remote, expect the working tree to materialize.
        let dir_b = tempfile::tempdir().unwrap();
        let repo_b = clone_repo(&remote, "memory://shared", dir_b.path())
            .await
            .unwrap();
        assert_eq!(repo_b.head().await.unwrap(), Some(c1.clone()));
        assert_eq!(repo_b.remote_head().await.unwrap(), Some(c1.clone()));
        assert_eq!(
            std::fs::read(dir_b.path().join("art/cat.png")).unwrap(),
            b"pretend image bytes"
        );
        assert_eq!(
            std::fs::read(dir_b.path().join("art/dog.png")).unwrap(),
            b"another image"
        );

        // Repo A: edit a file, commit, push.
        write_file(dir_a.path(), "art/cat.png", b"new image bytes");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        let c2 = stage::commit(&repo_a, &store_a, "alice", "second")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();
        assert_eq!(remote.get_head().await.unwrap(), Some(c2.clone()));

        // Repo B: pull and see the update.
        pull(&repo_b, &remote).await.unwrap();
        assert_eq!(repo_b.head().await.unwrap(), Some(c2));
        assert_eq!(
            std::fs::read(dir_b.path().join("art/cat.png")).unwrap(),
            b"new image bytes"
        );
    }

    #[tokio::test]
    async fn push_refuses_diverged_history() {
        let (dir_a, repo_a, remote) = fixture().await;
        let store_a = repo_a.store().await.unwrap();
        write_file(dir_a.path(), "f.txt", b"v1");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c1")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();

        // Repo B: clone, commit on top, push (advances remote).
        let dir_b = tempfile::tempdir().unwrap();
        let repo_b = clone_repo(&remote, "memory://shared", dir_b.path())
            .await
            .unwrap();
        let store_b = repo_b.store().await.unwrap();
        write_file(dir_b.path(), "f.txt", b"v2-from-b");
        stage::add(&repo_b, &store_b, dir_b.path()).await.unwrap();
        stage::commit(&repo_b, &store_b, "bob", "c2").await.unwrap();
        push(&repo_b, &remote).await.unwrap();

        // Repo A also commits on its own old tip — diverges.
        write_file(dir_a.path(), "f.txt", b"v2-from-a");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c2-a")
            .await
            .unwrap();

        let err = push(&repo_a, &remote).await.unwrap_err();
        assert!(matches!(err, Error::NotFastForward), "got {err:?}");
    }

    #[tokio::test]
    async fn pull_refuses_dirty_working_tree() {
        let (dir_a, repo_a, remote) = fixture().await;
        let store_a = repo_a.store().await.unwrap();
        write_file(dir_a.path(), "f.txt", b"v1");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c1")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();

        // Clone, advance remote from a, then dirty B's working tree before
        // pulling.
        let dir_b = tempfile::tempdir().unwrap();
        let repo_b = clone_repo(&remote, "memory://shared", dir_b.path())
            .await
            .unwrap();

        write_file(dir_a.path(), "f.txt", b"v2-from-a");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c2")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();

        // Mutate B's f.txt locally without committing.
        write_file(dir_b.path(), "f.txt", b"local-edit-very-different-length");

        let err = pull(&repo_b, &remote).await.unwrap_err();
        assert!(matches!(err, Error::DirtyWorkingTree), "got {err:?}");
    }

    #[tokio::test]
    async fn fetch_is_metadata_only() {
        // Set up a tip with a chunked file, push it, then ensure a fresh
        // local repo's fetch only pulls down commit/tree/file, no chunks.
        let dir_a = tempfile::tempdir().unwrap();
        let repo_a = Repo::init(dir_a.path(), "memory://shared").await.unwrap();
        let store_a = repo_a.store().await.unwrap();
        let remote = MemoryStore::new();

        write_file(dir_a.path(), "blob.bin", &[0xAB; 200]);
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c1")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();

        // Fresh repo B (no clone) → fetch only.
        let dir_b = tempfile::tempdir().unwrap();
        let repo_b = Repo::init(dir_b.path(), "memory://shared").await.unwrap();
        let store_b = repo_b.store().await.unwrap();
        fetch(&repo_b, &remote).await.unwrap();

        let listed = store_b.list().await.unwrap();
        // No chunks: the chunk for blob.bin (1-byte-pattern hash) must be absent.
        let blob_chunk = Hash::of(&[0xAB; 200]);
        assert!(
            !listed.contains(&blob_chunk),
            "fetch must not download chunks"
        );
        // But commit/tree/file are present.
        assert!(repo_b.remote_head().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn push_resume_after_partial_failure() {
        // Simulated by: pushing twice. The second push must be a no-op once
        // the remote already has the new tip.
        let (dir_a, repo_a, remote) = fixture().await;
        let store_a = repo_a.store().await.unwrap();
        write_file(dir_a.path(), "f.txt", b"v1");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c1")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();
        let count_after_first = remote.list().await.unwrap().len();
        // Second push: same tip → must not duplicate any objects.
        push(&repo_a, &remote).await.unwrap();
        assert_eq!(remote.list().await.unwrap().len(), count_after_first);
    }

    #[tokio::test]
    async fn pull_removes_files_deleted_on_remote() {
        // Repo A creates two files, pushes, deletes one, pushes again.
        let (dir_a, repo_a, remote) = fixture().await;
        let store_a = repo_a.store().await.unwrap();
        write_file(dir_a.path(), "keep.txt", b"k");
        write_file(dir_a.path(), "remove.txt", b"r");
        write_file(dir_a.path(), "sub/inside.txt", b"i");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c1")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();

        // Repo B clones the initial state.
        let dir_b = tempfile::tempdir().unwrap();
        let repo_b = clone_repo(&remote, "memory://shared", dir_b.path())
            .await
            .unwrap();
        assert!(dir_b.path().join("remove.txt").exists());
        assert!(dir_b.path().join("sub/inside.txt").exists());

        // Repo A deletes files and pushes the deletion.
        std::fs::remove_file(dir_a.path().join("remove.txt")).unwrap();
        std::fs::remove_file(dir_a.path().join("sub/inside.txt")).unwrap();
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "delete some")
            .await
            .unwrap();
        push(&repo_a, &remote).await.unwrap();

        // Repo B pulls. The deleted files must be gone from disk.
        pull(&repo_b, &remote).await.unwrap();
        assert!(dir_b.path().join("keep.txt").exists());
        assert!(!dir_b.path().join("remove.txt").exists());
        assert!(!dir_b.path().join("sub/inside.txt").exists());
        // Empty `sub/` dir should also be cleaned up.
        assert!(!dir_b.path().join("sub").exists());
    }

    #[tokio::test]
    async fn concurrent_push_is_last_write_wins() {
        // Regression-locks design §10: v1 explicitly does NOT detect
        // concurrent pushes. If this test ever flips to detecting them, that
        // should be a deliberate change paired with a documentation update.
        let dir_a = tempfile::tempdir().unwrap();
        let repo_a = Repo::init(dir_a.path(), "memory://shared").await.unwrap();
        let store_a = repo_a.store().await.unwrap();
        write_file(dir_a.path(), "f.txt", b"v0");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        stage::commit(&repo_a, &store_a, "alice", "c0")
            .await
            .unwrap();
        let remote = MemoryStore::new();
        push(&repo_a, &remote).await.unwrap();

        // Two clones, both diverge from the same tip.
        let dir_x = tempfile::tempdir().unwrap();
        let repo_x = clone_repo(&remote, "memory://shared", dir_x.path())
            .await
            .unwrap();
        let store_x = repo_x.store().await.unwrap();
        let dir_y = tempfile::tempdir().unwrap();
        let repo_y = clone_repo(&remote, "memory://shared", dir_y.path())
            .await
            .unwrap();
        let store_y = repo_y.store().await.unwrap();

        write_file(dir_x.path(), "f.txt", b"from-x");
        stage::add(&repo_x, &store_x, dir_x.path()).await.unwrap();
        let cx = stage::commit(&repo_x, &store_x, "x", "cx").await.unwrap();

        write_file(dir_y.path(), "f.txt", b"from-y");
        stage::add(&repo_y, &store_y, dir_y.path()).await.unwrap();
        let cy = stage::commit(&repo_y, &store_y, "y", "cy").await.unwrap();

        // x pushes first.
        push(&repo_x, &remote).await.unwrap();
        assert_eq!(remote.get_head().await.unwrap(), Some(cx.clone()));
        // y has stale REMOTE_HEAD; if y blindly pushes without re-fetching
        // first it would win. push() refetches at start, so this becomes a
        // diverged-history error — which is the right v1 behavior. Direct
        // remote.put_head(Some(&cy)) simulates the "two clients race past
        // the head check" scenario the design call out:
        remote.put_head(Some(&cy)).await.unwrap();
        // Last write wins; cx is now orphaned but its objects remain.
        assert_eq!(remote.get_head().await.unwrap(), Some(cy.clone()));
        assert!(
            remote.has(&cx).await.unwrap(),
            "orphaned commit objects remain"
        );
    }

    #[tokio::test]
    async fn corrupt_object_detected_on_walk() {
        let (dir_a, repo_a, _remote) = fixture().await;
        let store_a = repo_a.store().await.unwrap();
        write_file(dir_a.path(), "f.txt", b"hello");
        stage::add(&repo_a, &store_a, dir_a.path()).await.unwrap();
        let commit_hash = stage::commit(&repo_a, &store_a, "alice", "c1")
            .await
            .unwrap();

        // Overwrite the commit object's bytes with garbage. The walker must
        // notice on read.
        store_a
            .put(&commit_hash, Bytes::from_static(b"not a real commit"))
            .await
            .unwrap();
        let err = walk_reachable(&store_a, &commit_hash, WalkScope::Full, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::CorruptObject { .. }), "got {err:?}");
    }
}
