//! `crys status` — diff the three states.
//!
//! Three diffs, mirroring git:
//!
//! 1. **Staged**: HEAD's tree vs the index. Files that will be in the next
//!    commit if it ran right now.
//! 2. **Unstaged**: index vs the working tree. Files modified or deleted
//!    on disk since they were last `crys add`ed. We use a size-based
//!    heuristic (same as [`crate::sync`]) — re-hashing every working-tree
//!    file on every status would be too slow for the binary-asset case.
//! 3. **Untracked**: files in the working tree that aren't in the index
//!    and aren't ignored by `.crysignore`.

use std::collections::{BTreeMap, BTreeSet};

use ignore::WalkBuilder;

use crate::objects::{CanonicalJson, CommitBody, EntryMode, TreeBody};
use crate::repo::{IndexFile, Repo};
use crate::stage::posix_path;
use crate::store::ObjectStore;
use crate::Result;

/// What changed between two snapshots of a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// New file (not in `before`, present in `after`).
    Added,
    /// Existing file with different content.
    Modified,
    /// Removed file (present in `before`, missing in `after`).
    Deleted,
}

/// Result of a `crys status` walk.
#[derive(Debug, Default, Clone)]
pub struct Status {
    /// HEAD tree → index.
    pub staged: BTreeMap<String, Change>,
    /// Index → working tree (size-based; treats unchanged-size as
    /// unchanged content).
    pub unstaged: BTreeMap<String, Change>,
    /// In the working tree but not in the index. Sorted for stable output.
    pub untracked: BTreeSet<String>,
    /// Current branch tip, for header rendering.
    pub head: Option<crate::Hash>,
}

impl Status {
    pub fn is_clean(&self) -> bool {
        self.staged.is_empty() && self.unstaged.is_empty() && self.untracked.is_empty()
    }
}

/// Compute the three diffs for `repo`. `store` is needed to read HEAD's tree.
pub async fn status<S: ObjectStore>(repo: &Repo, store: &S) -> Result<Status> {
    let index = repo.read_index().await?;
    let head = repo.head().await?;

    let head_tree = match &head {
        Some(commit_hash) => {
            let bytes = store.get(commit_hash).await?;
            let commit = CommitBody::from_storage_bytes(&bytes)?;
            collect_tree(store, &commit.tree, "").await?
        }
        None => BTreeMap::new(),
    };

    let staged = diff_index_vs_tree(&index, &head_tree);
    let (unstaged, untracked) = diff_working_tree(repo, &index)?;

    Ok(Status {
        staged,
        unstaged,
        untracked,
        head,
    })
}

/// Walk a tree object and flatten it to `path → file_hash` for diffing.
fn collect_tree<'a, S: ObjectStore + 'a>(
    store: &'a S,
    tree_hash: &'a crate::Hash,
    rel_prefix: &'a str,
) -> futures::future::BoxFuture<'a, Result<BTreeMap<String, crate::Hash>>> {
    Box::pin(async move {
        let mut out = BTreeMap::new();
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
                    let sub = collect_tree(store, &entry.hash, &rel).await?;
                    out.extend(sub);
                }
                EntryMode::File => {
                    out.insert(rel, entry.hash);
                }
            }
        }
        Ok(out)
    })
}

fn diff_index_vs_tree(
    index: &IndexFile,
    tree: &BTreeMap<String, crate::Hash>,
) -> BTreeMap<String, Change> {
    let mut out = BTreeMap::new();
    for (path, entry) in &index.entries {
        match tree.get(path) {
            None => {
                out.insert(path.clone(), Change::Added);
            }
            Some(tree_hash) if tree_hash != &entry.file_hash => {
                out.insert(path.clone(), Change::Modified);
            }
            _ => {}
        }
    }
    for path in tree.keys() {
        if !index.entries.contains_key(path) {
            out.insert(path.clone(), Change::Deleted);
        }
    }
    out
}

/// Walk the working tree, returning (unstaged changes, untracked paths).
///
/// Heuristic for `Modified`: size differs from the index entry. The index
/// also stores `mtime`, but using mtime for change detection is famously
/// brittle (filesystems with second-resolution timestamps, copy-on-write
/// preserving mtime, etc.) so we match what `crys sync` already uses.
fn diff_working_tree(
    repo: &Repo,
    index: &IndexFile,
) -> Result<(BTreeMap<String, Change>, BTreeSet<String>)> {
    let mut unstaged = BTreeMap::new();
    let mut untracked = BTreeSet::new();
    let mut seen_in_walk = BTreeSet::new();

    let walker = WalkBuilder::new(repo.workdir())
        .hidden(false)
        .add_custom_ignore_filename(".crysignore")
        .filter_entry({
            let crys_dir = repo.crys_dir().to_path_buf();
            move |entry| entry.path() != crys_dir
        })
        .build();

    for entry in walker {
        let entry = entry
            .map_err(|e| crate::Error::Io(std::io::Error::other(format!("walk error: {e}"))))?;
        let Some(ft) = entry.file_type() else {
            continue;
        };
        if !ft.is_file() {
            continue;
        }
        let rel = match entry.path().strip_prefix(repo.workdir()) {
            Ok(r) => posix_path(r),
            Err(_) => continue,
        };
        seen_in_walk.insert(rel.clone());

        match index.entries.get(&rel) {
            None => {
                untracked.insert(rel);
            }
            Some(idx_entry) => {
                let size = std::fs::metadata(entry.path())
                    .map(|m| m.len())
                    .unwrap_or(0);
                if size != idx_entry.size {
                    unstaged.insert(rel, Change::Modified);
                }
            }
        }
    }

    // Anything in the index that we didn't see on disk is deleted from the
    // working tree.
    for path in index.entries.keys() {
        if !seen_in_walk.contains(path) {
            unstaged.insert(path.clone(), Change::Deleted);
        }
    }

    Ok((unstaged, untracked))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::Repo;
    use crate::stage;
    use crate::store::MemoryStore;
    use std::path::Path;

    fn write_file(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    async fn fixture() -> (tempfile::TempDir, Repo, MemoryStore) {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::init(dir.path(), "memory://test").await.unwrap();
        (dir, repo, MemoryStore::new())
    }

    #[tokio::test]
    async fn empty_repo_is_clean() {
        let (_dir, repo, store) = fixture().await;
        let s = status(&repo, &store).await.unwrap();
        assert!(s.is_clean());
        assert!(s.head.is_none());
    }

    #[tokio::test]
    async fn untracked_file_shows_up() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"hello");
        let s = status(&repo, &store).await.unwrap();
        assert!(s.staged.is_empty());
        assert!(s.unstaged.is_empty());
        assert_eq!(s.untracked, BTreeSet::from(["a.txt".to_string()]));
    }

    #[tokio::test]
    async fn add_moves_untracked_to_staged() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"hello");
        stage::add(&repo, &store, dir.path()).await.unwrap();

        let s = status(&repo, &store).await.unwrap();
        assert_eq!(s.staged.get("a.txt"), Some(&Change::Added));
        assert!(s.unstaged.is_empty());
        assert!(s.untracked.is_empty());
    }

    #[tokio::test]
    async fn commit_clears_staged() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"hello");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        stage::commit(&repo, &store, "tester", "c1").await.unwrap();

        let s = status(&repo, &store).await.unwrap();
        assert!(s.is_clean());
        assert!(s.head.is_some());
    }

    #[tokio::test]
    async fn modified_file_shows_unstaged_then_staged() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"hello");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        stage::commit(&repo, &store, "tester", "c1").await.unwrap();

        // Modify on disk → unstaged Modified.
        write_file(dir.path(), "a.txt", b"hello world!!");
        let s = status(&repo, &store).await.unwrap();
        assert!(s.staged.is_empty());
        assert_eq!(s.unstaged.get("a.txt"), Some(&Change::Modified));

        // Re-add → staged Modified.
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let s = status(&repo, &store).await.unwrap();
        assert_eq!(s.staged.get("a.txt"), Some(&Change::Modified));
        assert!(s.unstaged.is_empty());
    }

    #[tokio::test]
    async fn deleted_file_shows_unstaged_deleted() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"hello");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        stage::commit(&repo, &store, "tester", "c1").await.unwrap();

        std::fs::remove_file(dir.path().join("a.txt")).unwrap();
        let s = status(&repo, &store).await.unwrap();
        assert_eq!(s.unstaged.get("a.txt"), Some(&Change::Deleted));
    }

    #[tokio::test]
    async fn crysignore_excludes_untracked() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), ".crysignore", b"build/\n");
        write_file(dir.path(), "build/artifact", b"x");
        write_file(dir.path(), "kept.txt", b"x");

        let s = status(&repo, &store).await.unwrap();
        assert!(s.untracked.contains("kept.txt"));
        assert!(s.untracked.contains(".crysignore"));
        assert!(!s.untracked.iter().any(|p| p.starts_with("build/")));
    }
}
