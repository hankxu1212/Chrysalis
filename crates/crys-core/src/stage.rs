//! Staging and commit logic.
//!
//! Implements `crys add` and `crys commit` (design §8) against an
//! [`ObjectStore`]. Both operations are local-only — no network — and
//! produce the on-disk object DAG and HEAD chain the sync layer (Phase 5)
//! later pushes.
//!
//! # `add`
//!
//! Walks the working tree under each path argument, honoring `.crysignore`,
//! and for each file:
//!
//! 1. Slices the file into `chunk_size`-byte chunks via [`crate::chunker`].
//! 2. Writes any chunk hash not already present in the store.
//! 3. Builds and writes the [`FileBody`] manifest.
//! 4. Updates the index entry for that path.
//!
//! # `commit`
//!
//! 1. Diffs the index against `HEAD`'s tree. Empty diff → [`Error::NothingToCommit`].
//! 2. Builds a tree DAG bottom-up from the flat index.
//! 3. Writes a [`CommitBody`] with `parent = HEAD`.
//! 4. Advances `HEAD` to the new commit.
//!
//! Tree-from-index is the only non-trivial step. We group index paths by
//! their parent directory, recurse into subdirectories, then hash and write
//! each tree from the leaves up. Entry order within a tree is fixed by
//! [`TreeBody::new`] (lexicographic by name) so the resulting hash is
//! deterministic.

use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use chrono::Utc;
use ignore::WalkBuilder;

use crate::chunker::Chunker;
use crate::objects::{
    chunk_hash, CanonicalJson, CommitBody, EntryMode, FileBody, Hash, TreeBody, TreeEntry,
};
use crate::repo::{IndexEntry, IndexFile, Repo};
use crate::store::ObjectStore;
use crate::sync::{NoopProgress, ProgressHandle};
use crate::{Error, Result};

pub(crate) const CRYSIGNORE: &str = ".crysignore";

/// How many files to chunk + upload concurrently during `crys add`. Each
/// in-flight file holds one OS file handle plus, transiently, one chunk
/// in memory; the cap keeps a directory of tens of thousands of small
/// files from blowing through fd limits.
const ADD_PARALLELISM: usize = 8;

/// Stage a path (or directory tree) into the index.
///
/// `path` may be absolute or relative to the repo's working directory.
/// Returns the index entries written for each file under the path, keyed by
/// their working-tree-relative POSIX path.
pub async fn add<S: ObjectStore>(repo: &Repo, store: &S, path: &Path) -> Result<Vec<String>> {
    let progress: ProgressHandle = std::sync::Arc::new(NoopProgress);
    add_with_progress(repo, store, path, &progress).await
}

/// Like [`add`] but reports progress through the given handle. Emits a
/// `"walking"` spinner while enumerating files, then a `"files"` bar that
/// advances per file with the file's size as `bytes`.
pub async fn add_with_progress<S: ObjectStore>(
    repo: &Repo,
    store: &S,
    path: &Path,
    progress: &ProgressHandle,
) -> Result<Vec<String>> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        repo.workdir().join(path)
    };
    if !abs.exists() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("path does not exist: {}", abs.display()),
        )));
    }

    let chunk_size = repo.config().chunk_size as usize;
    let mut index = repo.read_index().await?;
    let mut staged = Vec::new();
    let mut seen_under_path: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Phase 1: enumerate files. Indeterminate — we don't know the total yet.
    progress.start_phase("walking", 0);
    // Show the input path so a multi-arg `crys add a b c/` makes it obvious
    // which iteration is currently in progress.
    progress.set_phase_label("walking", &abs.display().to_string());
    let walker = WalkBuilder::new(&abs)
        // .crysignore is the *only* source of ignore rules. Disable every
        // other knob the `ignore` crate has on by default — without this,
        // a global ~/.gitignore or stray .gitignore in any ancestor would
        // silently drop files (this bit us with `crys add *` on
        // ~/Downloads where macOS-side global excludes hide image files).
        .hidden(false)
        .ignore(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .parents(false)
        .require_git(false)
        .add_custom_ignore_filename(CRYSIGNORE)
        .filter_entry({
            let crys_dir = repo.crys_dir().to_path_buf();
            move |entry| entry.path() != crys_dir
        })
        .build();

    let mut files: Vec<(std::path::PathBuf, String)> = Vec::new();
    for entry in walker {
        let entry =
            entry.map_err(|e| Error::Io(std::io::Error::other(format!("walk error: {e}"))))?;
        let entry_path = entry.path();
        let file_type = match entry.file_type() {
            Some(ft) => ft,
            None => continue,
        };
        if !file_type.is_file() {
            continue;
        }

        let rel = entry_path.strip_prefix(repo.workdir()).map_err(|_| {
            Error::Io(std::io::Error::other(format!(
                "path {} is outside workdir {}",
                entry_path.display(),
                repo.workdir().display()
            )))
        })?;
        let rel_posix = posix_path(rel);
        progress.object_copied("walking", 0);
        files.push((entry_path.to_path_buf(), rel_posix));
    }
    progress.finish_phase("walking");

    // Phase 2: chunk + index each file. Total is now known. Run up to
    // ADD_PARALLELISM files concurrently; chunking is offloaded to the
    // blocking pool inside `stage_one_file`. The cap exists so a directory
    // with thousands of small files doesn't open thousands of fds at once.
    progress.start_phase("files", files.len());
    progress.set_phase_label("files", &abs.display().to_string());
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut in_flight = FuturesUnordered::new();
    let mut iter = files.into_iter();
    for _ in 0..ADD_PARALLELISM {
        if let Some((path, rel)) = iter.next() {
            in_flight.push(stage_with_path(store, path, rel, chunk_size));
        } else {
            break;
        }
    }
    while let Some(result) = in_flight.next().await {
        let (rel_posix, index_entry) = result?;
        let size = index_entry.size;
        index.entries.insert(rel_posix.clone(), index_entry);
        seen_under_path.insert(rel_posix.clone());
        staged.push(rel_posix);
        progress.object_copied("files", size);
        if let Some((path, rel)) = iter.next() {
            in_flight.push(stage_with_path(store, path, rel, chunk_size));
        }
    }
    progress.finish_phase("files");

    // Drop any indexed file that lives under `path` but no longer exists on
    // disk. Without this, `crys add .` after deleting a file silently leaves
    // a stale index entry, and the next `commit` won't reflect the deletion.
    //
    // We only prune entries whose POSIX path is under the staged subtree —
    // a `crys add subdir/` should never touch index entries outside `subdir/`.
    let workdir_prefix = posix_path_under(&abs, repo.workdir());
    let stale: Vec<String> = index
        .entries
        .keys()
        .filter(|k| match &workdir_prefix {
            Some(p) if !p.is_empty() => k.as_str() == p.as_str() || k.starts_with(&format!("{p}/")),
            _ => true, // staging the workdir root → consider all entries
        })
        .filter(|k| !seen_under_path.contains(k.as_str()))
        .cloned()
        .collect();
    for key in &stale {
        index.entries.remove(key);
        staged.push(key.clone());
    }

    repo.write_index(&index).await?;
    Ok(staged)
}

/// If `abs` is under `workdir`, return its POSIX-form relative path (empty
/// string if equal). Otherwise None.
fn posix_path_under(abs: &Path, workdir: &Path) -> Option<String> {
    let rel = abs.strip_prefix(workdir).ok()?;
    Some(posix_path(rel))
}

/// `stage_one_file` adapter that carries the relative path through so the
/// caller can pair completions with their index keys without needing
/// out-of-band state.
async fn stage_with_path<S: ObjectStore>(
    store: &S,
    path: std::path::PathBuf,
    rel: String,
    chunk_size: usize,
) -> Result<(String, IndexEntry)> {
    let entry = stage_one_file(store, &path, chunk_size).await?;
    Ok((rel, entry))
}

async fn stage_one_file<S: ObjectStore>(
    store: &S,
    path: &Path,
    chunk_size: usize,
) -> Result<IndexEntry> {
    let metadata = std::fs::metadata(path)?;
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| chrono::DateTime::<Utc>::from(t).to_rfc3339().into())
        .unwrap_or_else(|| Utc::now().to_rfc3339());

    // Chunking + hashing is sync and CPU-bound. Hand it to tokio's blocking
    // pool so multiple `stage_one_file` calls running concurrently actually
    // span multiple cores instead of stalling the async runtime.
    let path_buf = path.to_path_buf();
    let chunks: Vec<(Hash, Vec<u8>)> = tokio::task::spawn_blocking(move || -> Result<_> {
        let file = std::fs::File::open(&path_buf)?;
        let mut chunker = Chunker::new(file, chunk_size);
        let mut out = Vec::new();
        while let Some(chunk) = chunker.next_chunk()? {
            let hash = chunk_hash(&chunk);
            out.push((hash, chunk));
        }
        Ok(out)
    })
    .await
    .map_err(|e| Error::Io(std::io::Error::other(format!("blocking task: {e}"))))??;

    let mut chunk_hashes = Vec::with_capacity(chunks.len());
    for (hash, data) in chunks {
        if !store.has(&hash).await? {
            store.put(&hash, Bytes::from(data)).await?;
        }
        chunk_hashes.push(hash);
    }

    // Build the file manifest, write if new.
    let body = FileBody {
        chunk_size: chunk_size as u64,
        chunks: chunk_hashes,
        size,
    };
    let file_hash = body.hash()?;
    if !store.has(&file_hash).await? {
        store
            .put(&file_hash, Bytes::from(body.storage_bytes()?))
            .await?;
    }

    Ok(IndexEntry {
        file_hash,
        mtime,
        size,
    })
}

/// Build trees bottom-up from a flat index and write them to the store.
/// Returns the root tree's hash.
pub async fn build_tree_from_index<S: ObjectStore>(store: &S, index: &IndexFile) -> Result<Hash> {
    // Group entries by directory; an empty index produces an empty root tree.
    let root = directory_tree(index);
    write_tree(store, &root).await
}

/// In-memory directory representation built from the flat index. Each node
/// either holds a file hash (leaf) or child nodes (subdirectory).
#[derive(Debug, Default)]
struct DirNode {
    files: BTreeMap<String, Hash>,
    dirs: BTreeMap<String, DirNode>,
}

fn directory_tree(index: &IndexFile) -> DirNode {
    let mut root = DirNode::default();
    for (path, entry) in &index.entries {
        insert_path(&mut root, path, entry.file_hash.clone());
    }
    root
}

fn insert_path(root: &mut DirNode, path: &str, hash: Hash) {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return;
    }
    let mut node = root;
    for part in &parts[..parts.len() - 1] {
        node = node.dirs.entry((*part).to_string()).or_default();
    }
    let leaf = parts.last().unwrap();
    node.files.insert((*leaf).to_string(), hash);
}

fn write_tree<'a, S: ObjectStore>(
    store: &'a S,
    node: &'a DirNode,
) -> futures::future::BoxFuture<'a, Result<Hash>> {
    Box::pin(async move {
        let mut entries = Vec::with_capacity(node.files.len() + node.dirs.len());

        for (name, hash) in &node.files {
            entries.push(TreeEntry {
                hash: hash.clone(),
                mode: EntryMode::File,
                name: name.clone(),
            });
        }
        for (name, child) in &node.dirs {
            let child_hash = write_tree(store, child).await?;
            entries.push(TreeEntry {
                hash: child_hash,
                mode: EntryMode::Dir,
                name: name.clone(),
            });
        }

        let body = TreeBody::new(entries);
        let hash = body.hash()?;
        if !store.has(&hash).await? {
            store.put(&hash, Bytes::from(body.storage_bytes()?)).await?;
        }
        Ok(hash)
    })
}

/// Build a commit from the current index and advance HEAD.
///
/// Returns the new commit hash. Errors with [`Error::NothingToCommit`] if
/// the index's resulting tree is identical to `HEAD`'s tree.
pub async fn commit<S: ObjectStore>(
    repo: &Repo,
    store: &S,
    author: &str,
    message: &str,
) -> Result<Hash> {
    let index = repo.read_index().await?;
    let new_tree_hash = build_tree_from_index(store, &index).await?;

    let parent = repo.head().await?;
    if let Some(parent_hash) = &parent {
        let parent_tree = parent_tree_hash(store, parent_hash).await?;
        if parent_tree == new_tree_hash {
            return Err(Error::NothingToCommit);
        }
    }

    let body = CommitBody {
        author: author.to_string(),
        message: message.to_string(),
        parent: parent.clone(),
        timestamp: Utc::now().to_rfc3339(),
        tree: new_tree_hash,
    };
    let commit_hash = body.hash()?;
    store
        .put(&commit_hash, Bytes::from(body.storage_bytes()?))
        .await?;
    repo.set_head(Some(&commit_hash)).await?;
    Ok(commit_hash)
}

async fn parent_tree_hash<S: ObjectStore>(store: &S, commit_hash: &Hash) -> Result<Hash> {
    let bytes = store.get(commit_hash).await?;
    let body = CommitBody::from_storage_bytes(&bytes)?;
    Ok(body.tree)
}

/// Three modes of `crys reset`, mirroring git semantics.
#[derive(Debug, Clone, Copy)]
pub enum ResetMode {
    /// Move HEAD only. Index and working tree are untouched.
    Soft,
    /// Move HEAD and rebuild the index from the target commit's tree.
    /// Working tree is left alone — this is what unstages files.
    Mixed,
    /// Move HEAD, rebuild the index, *and* overwrite the working tree
    /// with the target commit's tree.
    Hard,
}

/// `crys reset`. `target` is the commit to move HEAD to; `None` means HEAD
/// itself (used by the common "unstage everything" invocation).
pub async fn reset<S: ObjectStore>(
    repo: &Repo,
    store: &S,
    target: Option<&Hash>,
    mode: ResetMode,
) -> Result<Option<Hash>> {
    // Resolve target: explicit hash, or current HEAD (which may be None on a
    // fresh repo). A `None` target on a repo with no commits is allowed for
    // mixed/hard — it just empties the index/working tree.
    let head = repo.head().await?;
    let target = match target {
        Some(h) => Some(h.clone()),
        None => head.clone(),
    };

    if matches!(mode, ResetMode::Soft) {
        // Soft just moves HEAD; refuse if target is missing entirely since
        // there's nothing to move to.
        if let Some(h) = &target {
            repo.set_head(Some(h)).await?;
        } else {
            repo.set_head(None).await?;
        }
        return Ok(target);
    }

    // Mixed and hard both rebuild the index from the target's tree (or empty
    // if target is None).
    let new_index = match &target {
        Some(h) => {
            let bytes = store.get(h).await?;
            let body = CommitBody::from_storage_bytes(&bytes)?;
            rebuild_index_from_tree(store, &body.tree, repo.workdir()).await?
        }
        None => IndexFile::default(),
    };

    if matches!(mode, ResetMode::Hard) {
        // Wipe tracked files in the *current* working tree before checking
        // out the target. We don't sweep untracked files (that's `crys clean`).
        let current_index = repo.read_index().await?;
        for path in current_index.entries.keys() {
            let abs = repo.workdir().join(path);
            if abs.exists() {
                let _ = std::fs::remove_file(&abs);
            }
        }
        if let Some(h) = &target {
            let bytes = store.get(h).await?;
            let body = CommitBody::from_storage_bytes(&bytes)?;
            checkout_tree(store, &body.tree, repo.workdir()).await?;
        }
    }

    repo.write_index(&new_index).await?;
    repo.set_head(target.as_ref()).await?;
    Ok(target)
}

pub(crate) fn posix_path(p: &Path) -> String {
    let mut parts = Vec::new();
    for component in p.components() {
        if let std::path::Component::Normal(s) = component {
            parts.push(s.to_string_lossy().into_owned());
        }
    }
    parts.join("/")
}

/// Recursively materialize a tree into `dest_dir`. Used by Phase 5's clone
/// and pull paths; lives here because it's the inverse of [`commit`]'s
/// tree-building logic.
pub fn checkout_tree<'a, S: ObjectStore + 'a>(
    store: &'a S,
    tree_hash: &'a Hash,
    dest_dir: &'a Path,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let bytes = store.get(tree_hash).await?;
        let body = TreeBody::from_storage_bytes(&bytes)?;
        std::fs::create_dir_all(dest_dir)?;
        for entry in body.entries {
            let child = dest_dir.join(&entry.name);
            match entry.mode {
                EntryMode::Dir => {
                    checkout_tree(store, &entry.hash, &child).await?;
                }
                EntryMode::File => {
                    checkout_file(store, &entry.hash, &child).await?;
                }
            }
        }
        Ok(())
    })
}

/// Walk a tree, rebuilding a flat index that mirrors the working-tree state
/// at that snapshot. Used by `clone`/`pull` to set `.crys/index` after
/// materializing files.
pub fn rebuild_index_from_tree<'a, S: ObjectStore + 'a>(
    store: &'a S,
    tree_hash: &'a Hash,
    workdir: &'a Path,
) -> futures::future::BoxFuture<'a, Result<IndexFile>> {
    Box::pin(async move {
        let mut index = IndexFile::default();
        rebuild_index_walk(store, tree_hash, workdir, "", &mut index).await?;
        Ok(index)
    })
}

fn rebuild_index_walk<'a, S: ObjectStore + 'a>(
    store: &'a S,
    tree_hash: &'a Hash,
    workdir: &'a Path,
    rel_prefix: &'a str,
    index: &'a mut IndexFile,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let bytes = store.get(tree_hash).await?;
        let body = TreeBody::from_storage_bytes(&bytes)?;
        for entry in body.entries {
            let rel = if rel_prefix.is_empty() {
                entry.name.clone()
            } else {
                format!("{rel_prefix}/{}", entry.name)
            };
            match entry.mode {
                EntryMode::Dir => {
                    rebuild_index_walk(store, &entry.hash, workdir, &rel, index).await?;
                }
                EntryMode::File => {
                    let file_bytes = store.get(&entry.hash).await?;
                    let file_body = FileBody::from_storage_bytes(&file_bytes)?;
                    let on_disk = workdir.join(&rel);
                    let mtime = std::fs::metadata(&on_disk)
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .map(|t| chrono::DateTime::<Utc>::from(t).to_rfc3339())
                        .unwrap_or_else(|| Utc::now().to_rfc3339());
                    index.entries.insert(
                        rel,
                        IndexEntry {
                            file_hash: entry.hash,
                            mtime,
                            size: file_body.size,
                        },
                    );
                }
            }
        }
        Ok(())
    })
}

async fn checkout_file<S: ObjectStore>(store: &S, file_hash: &Hash, dest: &Path) -> Result<()> {
    let bytes = store.get(file_hash).await?;
    let body = FileBody::from_storage_bytes(&bytes)?;
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(dest)?;
    use std::io::Write;
    for chunk in body.chunks {
        let chunk_bytes = store.get(&chunk).await?;
        file.write_all(&chunk_bytes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::Repo;
    use crate::store::MemoryStore;

    async fn fresh_repo() -> (tempfile::TempDir, Repo, MemoryStore) {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "s3://test/repo").await.unwrap();
        (dir, repo, MemoryStore::new())
    }

    fn write_file(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    #[tokio::test]
    async fn add_then_commit_creates_chain() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"hello");
        write_file(dir.path(), "src/lib.rs", b"fn main(){}");

        add(&repo, &store, dir.path()).await.unwrap();
        let index = repo.read_index().await.unwrap();
        assert_eq!(index.entries.len(), 2);
        assert!(index.entries.contains_key("a.txt"));
        assert!(index.entries.contains_key("src/lib.rs"));

        let c1 = commit(&repo, &store, "tester", "first").await.unwrap();
        assert_eq!(repo.head().await.unwrap(), Some(c1.clone()));

        // Second commit on the same index → NothingToCommit.
        let err = commit(&repo, &store, "tester", "no-op").await.unwrap_err();
        assert!(matches!(err, Error::NothingToCommit));

        // Modify a file, re-add, commit again — parent chain length 2.
        write_file(dir.path(), "a.txt", b"hello world");
        add(&repo, &store, dir.path()).await.unwrap();
        let c2 = commit(&repo, &store, "tester", "second").await.unwrap();
        assert_ne!(c1, c2);

        let bytes = store.get(&c2).await.unwrap();
        let body = CommitBody::from_storage_bytes(&bytes).unwrap();
        assert_eq!(body.parent, Some(c1));
        assert_eq!(body.message, "second");
    }

    #[tokio::test]
    async fn re_add_unchanged_file_is_no_op_for_objects() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"abc");
        add(&repo, &store, dir.path()).await.unwrap();
        let object_count = store.list().await.unwrap().len();

        // Re-add same file, no content changed.
        add(&repo, &store, dir.path()).await.unwrap();
        assert_eq!(store.list().await.unwrap().len(), object_count);
    }

    #[tokio::test]
    async fn add_skips_crys_dir() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "real.txt", b"x");
        // The .crys/ dir was already created by Repo::init; make sure walking
        // doesn't try to stage its internals.
        add(&repo, &store, dir.path()).await.unwrap();
        let index = repo.read_index().await.unwrap();
        assert_eq!(index.entries.len(), 1);
        assert!(index.entries.contains_key("real.txt"));
    }

    #[tokio::test]
    async fn add_honors_crysignore() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), ".crysignore", b"ignored.txt\nbuild/\n");
        write_file(dir.path(), "kept.txt", b"x");
        write_file(dir.path(), "ignored.txt", b"y");
        write_file(dir.path(), "build/artifact", b"z");

        add(&repo, &store, dir.path()).await.unwrap();
        let index = repo.read_index().await.unwrap();
        assert!(index.entries.contains_key("kept.txt"));
        assert!(!index.entries.contains_key("ignored.txt"));
        assert!(!index.entries.contains_key("build/artifact"));
    }

    #[tokio::test]
    async fn commit_with_empty_index_works_first_time() {
        // First commit on an empty repo with an empty index → empty tree.
        let (_dir, repo, store) = fresh_repo().await;
        let c1 = commit(&repo, &store, "tester", "empty").await.unwrap();
        assert_eq!(repo.head().await.unwrap(), Some(c1.clone()));

        // Second empty-index commit must be NothingToCommit.
        let err = commit(&repo, &store, "tester", "still empty")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NothingToCommit));
    }

    #[tokio::test]
    async fn checkout_tree_round_trips() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"hello");
        write_file(dir.path(), "src/lib.rs", b"fn main(){}");
        write_file(dir.path(), "src/sub/deep.txt", b"deep");
        add(&repo, &store, dir.path()).await.unwrap();
        commit(&repo, &store, "tester", "snapshot").await.unwrap();

        let head = repo.head().await.unwrap().unwrap();
        let bytes = store.get(&head).await.unwrap();
        let commit_body = CommitBody::from_storage_bytes(&bytes).unwrap();

        let restore_dir = tempfile::tempdir().unwrap();
        checkout_tree(&store, &commit_body.tree, restore_dir.path())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(restore_dir.path().join("a.txt")).unwrap(),
            b"hello"
        );
        assert_eq!(
            std::fs::read(restore_dir.path().join("src/lib.rs")).unwrap(),
            b"fn main(){}"
        );
        assert_eq!(
            std::fs::read(restore_dir.path().join("src/sub/deep.txt")).unwrap(),
            b"deep"
        );
    }

    #[tokio::test]
    async fn add_picks_up_deletions_within_path() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"x");
        write_file(dir.path(), "sub/b.txt", b"x");
        write_file(dir.path(), "outside/c.txt", b"x");
        add(&repo, &store, dir.path()).await.unwrap();

        // Delete a tracked file under sub/ and re-add only sub/.
        std::fs::remove_file(dir.path().join("sub/b.txt")).unwrap();
        add(&repo, &store, &dir.path().join("sub")).await.unwrap();

        let index = repo.read_index().await.unwrap();
        // sub/b.txt is gone from the index.
        assert!(!index.entries.contains_key("sub/b.txt"));
        // But files outside the staged path are untouched.
        assert!(index.entries.contains_key("a.txt"));
        assert!(index.entries.contains_key("outside/c.txt"));
    }

    #[tokio::test]
    async fn add_root_picks_up_all_deletions() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"x");
        write_file(dir.path(), "b.txt", b"x");
        add(&repo, &store, dir.path()).await.unwrap();
        std::fs::remove_file(dir.path().join("a.txt")).unwrap();
        add(&repo, &store, dir.path()).await.unwrap();

        let index = repo.read_index().await.unwrap();
        assert!(!index.entries.contains_key("a.txt"));
        assert!(index.entries.contains_key("b.txt"));
    }

    #[tokio::test]
    async fn reset_mixed_unstages_changes_keeps_working_tree() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"v1");
        add(&repo, &store, dir.path()).await.unwrap();
        commit(&repo, &store, "tester", "first").await.unwrap();

        // Modify and stage the change; the index now diverges from HEAD's tree.
        write_file(dir.path(), "a.txt", b"v2");
        add(&repo, &store, dir.path()).await.unwrap();
        let staged_hash = repo.read_index().await.unwrap().entries["a.txt"]
            .file_hash
            .clone();

        reset(&repo, &store, None, ResetMode::Mixed).await.unwrap();

        // Index reverted to HEAD's tree...
        let after = repo.read_index().await.unwrap();
        assert_ne!(after.entries["a.txt"].file_hash, staged_hash);
        // ...but working tree still has the modified content.
        assert_eq!(std::fs::read(dir.path().join("a.txt")).unwrap(), b"v2");
    }

    #[tokio::test]
    async fn reset_soft_moves_head_only() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"v1");
        add(&repo, &store, dir.path()).await.unwrap();
        let c1 = commit(&repo, &store, "tester", "first").await.unwrap();

        write_file(dir.path(), "a.txt", b"v2");
        add(&repo, &store, dir.path()).await.unwrap();
        let c2 = commit(&repo, &store, "tester", "second").await.unwrap();
        assert_eq!(repo.head().await.unwrap(), Some(c2));

        // Soft reset to c1 — HEAD moves back, index/working tree untouched.
        let index_before = repo.read_index().await.unwrap();
        reset(&repo, &store, Some(&c1), ResetMode::Soft).await.unwrap();
        assert_eq!(repo.head().await.unwrap(), Some(c1));
        let index_after = repo.read_index().await.unwrap();
        assert_eq!(
            index_before.entries["a.txt"].file_hash,
            index_after.entries["a.txt"].file_hash
        );
        assert_eq!(std::fs::read(dir.path().join("a.txt")).unwrap(), b"v2");
    }

    #[tokio::test]
    async fn reset_hard_overwrites_working_tree() {
        let (dir, repo, store) = fresh_repo().await;
        write_file(dir.path(), "a.txt", b"v1");
        add(&repo, &store, dir.path()).await.unwrap();
        let c1 = commit(&repo, &store, "tester", "first").await.unwrap();

        // Modify the file but don't commit.
        write_file(dir.path(), "a.txt", b"v2-uncommitted");

        reset(&repo, &store, Some(&c1), ResetMode::Hard).await.unwrap();
        assert_eq!(std::fs::read(dir.path().join("a.txt")).unwrap(), b"v1");
        assert_eq!(repo.head().await.unwrap(), Some(c1));
    }

    #[tokio::test]
    async fn add_missing_path_returns_io_error() {
        let (dir, repo, store) = fresh_repo().await;
        let missing = dir.path().join("does-not-exist");
        let err = add(&repo, &store, &missing).await.unwrap_err();
        assert!(matches!(err, Error::Io(_)));
    }
}
