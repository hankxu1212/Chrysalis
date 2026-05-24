//! `crys log` — walk HEAD's parent chain.
//!
//! Linear history (design §4) → traversal is just "follow `parent` until
//! `None`". We materialize each `CommitBody` so the CLI can render it; the
//! reachability walker in [`crate::sync`] is a different thing (it also
//! walks trees and chunks).

use std::collections::HashSet;

use crate::objects::{CanonicalJson, CommitBody, Hash};
use crate::repo::Repo;
use crate::store::ObjectStore;
use crate::{Error, Result};

/// One entry in the log, with the commit's hash for display.
///
/// `in_local` / `in_remote` reflect what was true at the time `log()` ran.
/// `in_remote` is computed from `.crys/REMOTE_HEAD` — it's only as fresh as
/// the last `crys fetch`/`pull`/`push`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub hash: Hash,
    pub commit: CommitBody,
    pub in_local: bool,
    pub in_remote: bool,
}

/// Walk `repo`'s HEAD chain, returning commits newest-first. `limit` caps
/// the result; `None` means "all".
///
/// If HEAD is unset (no commits yet), returns an empty Vec.
///
/// Output ordering: any commits reachable from `REMOTE_HEAD` but *not* from
/// local `HEAD` come first (these are commits a `pull` would bring in),
/// followed by commits from local `HEAD`'s chain newest-first. This matches
/// what `git log` does after a fetch.
pub async fn log<S: ObjectStore>(
    repo: &Repo,
    store: &S,
    limit: Option<usize>,
) -> Result<Vec<LogEntry>> {
    let local_head = repo.head().await?;
    let remote_head = repo.remote_head().await?;

    // First, build the set of commits reachable from REMOTE_HEAD that are
    // present locally (i.e. that fetch has populated). We use this both to
    // mark `in_remote` on local entries and to discover remote-only
    // commits.
    let remote_ancestors = walk_chain_locally(store, remote_head.as_ref()).await?;

    // Build the local chain entries.
    let mut entries = Vec::new();
    let mut local_seen = HashSet::new();
    let mut current = local_head;
    while let Some(hash) = current {
        if let Some(cap) = limit {
            if entries.len() >= cap {
                break;
            }
        }
        let bytes = store.get(&hash).await?;
        let commit = CommitBody::from_storage_bytes(&bytes)?;
        let parent = commit.parent.clone();
        local_seen.insert(hash.clone());
        let in_remote = remote_ancestors.contains(&hash);
        entries.push(LogEntry {
            hash,
            commit,
            in_local: true,
            in_remote,
        });
        current = parent;
    }

    // Now find any commit in remote_ancestors that isn't in local_seen.
    // These are the "remote-only" commits — usually one or two new commits
    // that arrived via fetch and haven't been pulled yet. Walk REMOTE_HEAD's
    // chain in order so they come out newest-first.
    let mut remote_only = Vec::new();
    let mut current = remote_head;
    while let Some(hash) = current {
        if local_seen.contains(&hash) {
            break;
        }
        let bytes = match store.get(&hash).await {
            Ok(b) => b,
            // Fetch hasn't downloaded this yet; can't render it. Stop.
            Err(Error::NotFound { .. }) => break,
            Err(e) => return Err(e),
        };
        let commit = CommitBody::from_storage_bytes(&bytes)?;
        let parent = commit.parent.clone();
        remote_only.push(LogEntry {
            hash,
            commit,
            in_local: false,
            in_remote: true,
        });
        current = parent;
    }

    // Splice remote-only entries in front, then re-apply the limit.
    let mut combined = remote_only;
    combined.extend(entries);
    if let Some(cap) = limit {
        combined.truncate(cap);
    }
    Ok(combined)
}

/// Walk a commit's parent chain using only local store contents. Stops at
/// any missing object (treated as "fetch didn't go that far"). Returns the
/// set of hashes encountered.
async fn walk_chain_locally<S: ObjectStore>(
    store: &S,
    tip: Option<&Hash>,
) -> Result<HashSet<Hash>> {
    let mut seen = HashSet::new();
    let mut current = tip.cloned();
    while let Some(hash) = current {
        if !seen.insert(hash.clone()) {
            break;
        }
        let bytes = match store.get(&hash).await {
            Ok(b) => b,
            Err(Error::NotFound { .. }) => break,
            Err(e) => return Err(e),
        };
        let commit = CommitBody::from_storage_bytes(&bytes)?;
        current = commit.parent;
    }
    Ok(seen)
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
    async fn empty_repo_returns_empty_log() {
        let (_dir, repo, store) = fixture().await;
        assert!(log(&repo, &store, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn three_commits_yield_newest_first() {
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"1");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c1 = stage::commit(&repo, &store, "tester", "first")
            .await
            .unwrap();

        write_file(dir.path(), "a.txt", b"22");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c2 = stage::commit(&repo, &store, "tester", "second")
            .await
            .unwrap();

        write_file(dir.path(), "a.txt", b"333");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c3 = stage::commit(&repo, &store, "tester", "third")
            .await
            .unwrap();

        let entries = log(&repo, &store, None).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].hash, c3);
        assert_eq!(entries[0].commit.message, "third");
        assert_eq!(entries[1].hash, c2);
        assert_eq!(entries[2].hash, c1);
        assert!(entries[2].commit.parent.is_none());
        // Without REMOTE_HEAD set, every commit is local-only.
        for e in &entries {
            assert!(e.in_local);
            assert!(!e.in_remote);
        }
    }

    #[tokio::test]
    async fn local_and_remote_flags_reflect_state() {
        // Set up: c1 → c2 are local *and* remote (matching REMOTE_HEAD).
        // c3 is local only (committed but not pushed).
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"1");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c1 = stage::commit(&repo, &store, "tester", "first")
            .await
            .unwrap();
        write_file(dir.path(), "a.txt", b"22");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c2 = stage::commit(&repo, &store, "tester", "second")
            .await
            .unwrap();
        // Pretend c2 is what's on the remote.
        repo.set_remote_head(Some(&c2)).await.unwrap();

        // Now make a local commit on top.
        write_file(dir.path(), "a.txt", b"333");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c3 = stage::commit(&repo, &store, "tester", "third")
            .await
            .unwrap();

        let entries = log(&repo, &store, None).await.unwrap();
        let by_hash: std::collections::HashMap<_, _> =
            entries.iter().map(|e| (&e.hash, e)).collect();
        assert!(by_hash[&c1].in_local && by_hash[&c1].in_remote);
        assert!(by_hash[&c2].in_local && by_hash[&c2].in_remote);
        assert!(by_hash[&c3].in_local && !by_hash[&c3].in_remote);
    }

    #[tokio::test]
    async fn remote_only_commits_appear_first() {
        // Simulate a fetched-but-not-pulled remote commit: REMOTE_HEAD
        // points to a commit that *is* in local objects but is NOT in local
        // HEAD's chain.
        let (dir, repo, store) = fixture().await;
        write_file(dir.path(), "a.txt", b"1");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c1 = stage::commit(&repo, &store, "tester", "shared")
            .await
            .unwrap();

        // Build c2_remote on top of c1 *without* moving local HEAD.
        write_file(dir.path(), "a.txt", b"remote");
        stage::add(&repo, &store, dir.path()).await.unwrap();
        let c2_remote = stage::commit(&repo, &store, "tester", "remote-only")
            .await
            .unwrap();
        // Roll local HEAD back to c1 to simulate "fetch updated REMOTE_HEAD,
        // user hasn't pulled yet".
        repo.set_head(Some(&c1)).await.unwrap();
        repo.set_remote_head(Some(&c2_remote)).await.unwrap();

        let entries = log(&repo, &store, None).await.unwrap();
        assert_eq!(entries.len(), 2);
        // Remote-only first.
        assert_eq!(entries[0].hash, c2_remote);
        assert!(!entries[0].in_local);
        assert!(entries[0].in_remote);
        // Then the shared commit.
        assert_eq!(entries[1].hash, c1);
        assert!(entries[1].in_local && entries[1].in_remote);
    }

    #[tokio::test]
    async fn limit_caps_result() {
        let (dir, repo, store) = fixture().await;
        for i in 0..5 {
            write_file(dir.path(), "a.txt", &vec![b'a'; i + 1]);
            stage::add(&repo, &store, dir.path()).await.unwrap();
            stage::commit(&repo, &store, "tester", &format!("c{i}"))
                .await
                .unwrap();
        }
        let entries = log(&repo, &store, Some(2)).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].commit.message, "c4");
        assert_eq!(entries[1].commit.message, "c3");
    }
}
