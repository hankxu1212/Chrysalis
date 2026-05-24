//! `crys clean` — remove working-tree files not tracked in the index.
//!
//! Useful for recovering from the state where a buggy pull (pre-fix) wrote
//! a new tree but failed to delete files removed from upstream. Also handy
//! to drop accidentally-created files without committing them.
//!
//! Honors `.crysignore` so users don't accidentally `clean` files they meant
//! to keep but never staged.

use std::collections::BTreeSet;
use std::path::PathBuf;

use ignore::WalkBuilder;

use crate::repo::Repo;
use crate::stage::posix_path;
use crate::Result;

/// Result of a clean walk. `dry_run = true` populates the same list without
/// touching disk.
#[derive(Debug, Default)]
pub struct CleanReport {
    pub removed: Vec<String>,
}

pub async fn clean(repo: &Repo, dry_run: bool) -> Result<CleanReport> {
    let index = repo.read_index().await?;
    let tracked: BTreeSet<&String> = index.entries.keys().collect();

    let walker = WalkBuilder::new(repo.workdir())
        .hidden(false)
        .add_custom_ignore_filename(".crysignore")
        .filter_entry({
            let crys_dir = repo.crys_dir().to_path_buf();
            move |entry| entry.path() != crys_dir
        })
        .build();

    let mut to_remove: Vec<(PathBuf, String)> = Vec::new();
    for entry in walker {
        let entry = entry
            .map_err(|e| crate::Error::Io(std::io::Error::other(format!("walk error: {e}"))))?;
        let Some(ft) = entry.file_type() else {
            continue;
        };
        if !ft.is_file() {
            continue;
        }
        let abs = entry.path().to_path_buf();
        let rel = match abs.strip_prefix(repo.workdir()) {
            Ok(r) => posix_path(r),
            Err(_) => continue,
        };
        if !tracked.contains(&rel) {
            to_remove.push((abs, rel));
        }
    }

    let mut report = CleanReport::default();
    let mut maybe_empty_dirs: BTreeSet<PathBuf> = BTreeSet::new();
    for (abs, rel) in &to_remove {
        if !dry_run {
            match std::fs::remove_file(abs) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            let mut parent = abs.parent();
            while let Some(p) = parent {
                if p == repo.workdir() {
                    break;
                }
                maybe_empty_dirs.insert(p.to_path_buf());
                parent = p.parent();
            }
        }
        report.removed.push(rel.clone());
    }

    if !dry_run {
        let mut sorted: Vec<_> = maybe_empty_dirs.into_iter().collect();
        sorted.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
        for dir in sorted {
            let _ = std::fs::remove_dir(&dir);
        }
    }

    report.removed.sort();
    Ok(report)
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
    async fn removes_untracked_files() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "tracked.txt", b"x");
        stage::add(&repo, &store, dir.path()).await.unwrap();

        write_file(dir.path(), "junk.txt", b"y");
        write_file(dir.path(), "sub/nested.txt", b"z");

        let report = clean(&repo, false).await.unwrap();
        assert_eq!(report.removed, vec!["junk.txt", "sub/nested.txt"]);
        assert!(dir.path().join("tracked.txt").exists());
        assert!(!dir.path().join("junk.txt").exists());
        assert!(!dir.path().join("sub").exists());
    }

    #[tokio::test]
    async fn dry_run_lists_but_does_not_delete() {
        let (dir, repo, _store) = fixture().await;
        write_file(dir.path(), "junk.txt", b"y");
        let report = clean(&repo, true).await.unwrap();
        assert_eq!(report.removed, vec!["junk.txt"]);
        assert!(dir.path().join("junk.txt").exists());
    }

    #[tokio::test]
    async fn honors_crysignore() {
        let (dir, repo, _store) = fixture().await;
        write_file(dir.path(), ".crysignore", b"keep-me/\n");
        write_file(dir.path(), "keep-me/file.txt", b"x");
        write_file(dir.path(), "junk.txt", b"y");

        let report = clean(&repo, false).await.unwrap();
        assert!(report.removed.contains(&"junk.txt".to_string()));
        assert!(!report.removed.iter().any(|p| p.starts_with("keep-me/")));
        assert!(dir.path().join("keep-me/file.txt").exists());
    }
}
