//! Content-addressed object model.
//!
//! See design §4 for the wire format. This module owns:
//!
//! - The four object kinds (`chunk`, `file`, `tree`, `commit`).
//! - Canonical JSON serialization for `file`/`tree`/`commit`.
//! - SHA-256 hashing over the canonical *uncompressed* bytes.
//! - The on-disk/on-S3 gzip wrapper for non-chunk bodies.
//! - The `<ab>/<cdef...>` storage path layout.
//!
//! Canonical-form rules (must hold for hashing to stay deterministic):
//!
//! 1. Object structs declare fields in alphabetical order so `serde_json`
//!    serializes in that order — `serde_json` preserves declaration order.
//! 2. Tree entries are sorted by `name` at construction time
//!    (see [`TreeBody::new`]).
//! 3. JSON is emitted with `to_vec` (no insignificant whitespace, no trailing
//!    newline, UTF-8).
//!
//! Tests in this file lock the on-disk format with golden hashes — changing
//! the canonical form is a deliberate breaking change.

use std::io::{Read, Write};

use bytes::Bytes;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

/// SHA-256 digest of an object's canonical uncompressed bytes, hex-encoded
/// (lowercase, 64 chars).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash(String);

impl Hash {
    /// Construct from raw bytes by hashing them with SHA-256.
    pub fn of(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        Self(hex::encode(digest))
    }

    /// Validate and wrap an existing hex string.
    pub fn from_hex(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        if s.len() != 64
            || !s
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::InvalidHash(s));
        }
        Ok(Self(s))
    }

    /// 64-char lowercase hex.
    pub fn as_hex(&self) -> &str {
        &self.0
    }

    /// `<ab>/<cdef...>` storage path. The first two hex chars become a
    /// directory prefix to avoid huge flat directories (design §4).
    pub fn storage_path(&self) -> String {
        format!("{}/{}", &self.0[..2], &self.0[2..])
    }

    /// True if this hash is the SHA-256 of `bytes`. Used to verify chunk
    /// objects on read (chunks are never gzipped, so the hash is over the
    /// raw payload).
    pub fn matches_chunk_bytes(&self, bytes: &[u8]) -> bool {
        Hash::of(bytes) == *self
    }

    /// True if this hash is the SHA-256 of the *uncompressed* `storage_bytes`.
    /// Used to verify `file`/`tree`/`commit` objects on read.
    pub fn matches_storage_bytes(&self, storage_bytes: &[u8]) -> Result<bool> {
        let mut decoder = GzDecoder::new(storage_bytes);
        let mut canonical = Vec::new();
        decoder.read_to_end(&mut canonical)?;
        Ok(Hash::of(&canonical) == *self)
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The four object types. The wire format never embeds this tag — kind is
/// determined by where the object is referenced (commit's `tree` slot points
/// at a `tree`, etc.). The enum exists for routing/logging, not for
/// serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Chunk,
    File,
    Tree,
    Commit,
}

impl ObjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Chunk => "chunk",
            ObjectKind::File => "file",
            ObjectKind::Tree => "tree",
            ObjectKind::Commit => "commit",
        }
    }
}

/// Reassembly recipe for one file.
///
/// Per design §4, even files smaller than `chunk_size` produce a one-chunk
/// manifest so the model stays uniform.
///
/// JSON fields (alphabetical, matching canonical order):
/// `chunk_size`, `chunks`, `size`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileBody {
    pub chunk_size: u64,
    pub chunks: Vec<Hash>,
    pub size: u64,
}

/// Whether a tree entry refers to a file or a subdirectory tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryMode {
    File,
    Dir,
}

/// One entry in a tree: a child file or subdirectory.
///
/// Field order in the struct matches canonical JSON order: `hash`, `mode`,
/// `name`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEntry {
    pub hash: Hash,
    pub mode: EntryMode,
    pub name: String,
}

/// One directory snapshot.
///
/// Entries are sorted by `name` at construction time. Mutating the field
/// directly bypasses the invariant — prefer [`TreeBody::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeBody {
    pub entries: Vec<TreeEntry>,
}

impl TreeBody {
    /// Build a tree with entries sorted by `name`. Caller should not pass
    /// duplicate names; this constructor does not deduplicate.
    pub fn new(mut entries: Vec<TreeEntry>) -> Self {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Self { entries }
    }
}

/// One commit.
///
/// `parent` is `None` for the initial commit (linear history → at most one
/// parent, design §4). `timestamp` is RFC 3339 UTC; the format is the
/// caller's responsibility but staying consistent matters for canonical-form
/// stability.
///
/// Field order: `author`, `message`, `parent`, `timestamp`, `tree`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitBody {
    pub author: String,
    pub message: String,
    pub parent: Option<Hash>,
    pub timestamp: String,
    pub tree: Hash,
}

/// Trait for the three JSON-bodied object kinds. Provides canonical
/// serialization, hashing, and gzip-wrapped storage form.
pub trait CanonicalJson: Serialize + for<'de> Deserialize<'de> + Sized {
    /// Which kind this body represents — used for error messages.
    const KIND: ObjectKind;

    /// Canonical UTF-8 JSON bytes (no insignificant whitespace, no trailing
    /// newline, fields in declaration order).
    fn canonical_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Hash over the canonical uncompressed bytes (design §4).
    fn hash(&self) -> Result<Hash> {
        Ok(Hash::of(&self.canonical_bytes()?))
    }

    /// Bytes that land on disk and on S3 — gzip(canonical JSON).
    fn storage_bytes(&self) -> Result<Vec<u8>> {
        let canonical = self.canonical_bytes()?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&canonical)?;
        Ok(encoder.finish()?)
    }

    /// Inverse of [`Self::storage_bytes`]: gunzip then JSON-decode.
    fn from_storage_bytes(bytes: &[u8]) -> Result<Self> {
        let mut decoder = GzDecoder::new(bytes);
        let mut canonical = Vec::new();
        decoder.read_to_end(&mut canonical)?;
        Ok(serde_json::from_slice(&canonical)?)
    }
}

impl CanonicalJson for FileBody {
    const KIND: ObjectKind = ObjectKind::File;
}

impl CanonicalJson for TreeBody {
    const KIND: ObjectKind = ObjectKind::Tree;
}

impl CanonicalJson for CommitBody {
    const KIND: ObjectKind = ObjectKind::Commit;
}

/// A `chunk` object's "canonical bytes" are just its raw payload — chunks
/// are never gzipped (design §4: "stored raw so multipart upload streams
/// straight from disk and S3-side checksums work cleanly").
pub fn chunk_hash(payload: &[u8]) -> Hash {
    Hash::of(payload)
}

/// Storage form of a chunk: identity. Exists for symmetry with
/// [`CanonicalJson::storage_bytes`] at call sites that don't know the kind
/// statically.
pub fn chunk_storage_bytes(payload: Bytes) -> Bytes {
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hash(b: u8) -> Hash {
        Hash::of(&[b])
    }

    // --- Hash --------------------------------------------------------------

    #[test]
    fn hash_of_known_input() {
        // SHA-256("") — the canonical empty-string digest.
        let h = Hash::of(b"");
        assert_eq!(
            h.as_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hash_storage_path_splits_first_two_chars() {
        let h = Hash::of(b"hello");
        let p = h.storage_path();
        assert_eq!(p.len(), 65); // 2 + '/' + 62
        assert_eq!(&p[..2], &h.as_hex()[..2]);
        assert_eq!(&p[3..], &h.as_hex()[2..]);
    }

    #[test]
    fn hash_from_hex_validates() {
        assert!(Hash::from_hex("a".repeat(64)).is_ok());
        assert!(Hash::from_hex("A".repeat(64)).is_err()); // uppercase rejected
        assert!(Hash::from_hex("a".repeat(63)).is_err()); // too short
        assert!(Hash::from_hex("g".repeat(64)).is_err()); // non-hex
    }

    // --- Round-trip --------------------------------------------------------

    #[test]
    fn file_body_round_trip() {
        let body = FileBody {
            chunk_size: 8 * 1024 * 1024,
            chunks: vec![make_hash(0), make_hash(1)],
            size: 16_000_000,
        };
        let storage = body.storage_bytes().unwrap();
        let decoded = FileBody::from_storage_bytes(&storage).unwrap();
        assert_eq!(body, decoded);
    }

    #[test]
    fn tree_body_round_trip_sorts_entries() {
        let body = TreeBody::new(vec![
            TreeEntry {
                hash: make_hash(2),
                mode: EntryMode::File,
                name: "z.txt".into(),
            },
            TreeEntry {
                hash: make_hash(1),
                mode: EntryMode::Dir,
                name: "a".into(),
            },
        ]);
        // Construction sorted entries.
        assert_eq!(body.entries[0].name, "a");
        assert_eq!(body.entries[1].name, "z.txt");

        let storage = body.storage_bytes().unwrap();
        let decoded = TreeBody::from_storage_bytes(&storage).unwrap();
        assert_eq!(body, decoded);
    }

    #[test]
    fn commit_body_round_trip_with_and_without_parent() {
        let with_parent = CommitBody {
            author: "Hank <hank@example.com>".into(),
            message: "first commit".into(),
            parent: Some(make_hash(7)),
            timestamp: "2026-05-24T12:00:00Z".into(),
            tree: make_hash(8),
        };
        let storage = with_parent.storage_bytes().unwrap();
        assert_eq!(
            CommitBody::from_storage_bytes(&storage).unwrap(),
            with_parent
        );

        let root_commit = CommitBody {
            author: "Hank".into(),
            message: "root".into(),
            parent: None,
            timestamp: "2026-05-24T12:00:00Z".into(),
            tree: make_hash(9),
        };
        let storage = root_commit.storage_bytes().unwrap();
        assert_eq!(
            CommitBody::from_storage_bytes(&storage).unwrap(),
            root_commit
        );
    }

    // --- Canonical-form stability (golden tests) ---------------------------
    //
    // These lock the on-disk format. Changing them is a deliberate breaking
    // change — bump the format_version in repo config (§5) at the same time.

    #[test]
    fn canonical_json_field_order_is_alphabetical() {
        let body = FileBody {
            chunk_size: 8,
            chunks: vec![],
            size: 0,
        };
        let json = String::from_utf8(body.canonical_bytes().unwrap()).unwrap();
        assert_eq!(json, r#"{"chunk_size":8,"chunks":[],"size":0}"#);
    }

    #[test]
    fn canonical_json_no_trailing_whitespace() {
        let body = TreeBody { entries: vec![] };
        let bytes = body.canonical_bytes().unwrap();
        assert_eq!(&bytes, br#"{"entries":[]}"#);
        assert!(!bytes.ends_with(b"\n"));
    }

    #[test]
    fn commit_canonical_form_with_null_parent() {
        let body = CommitBody {
            author: "a".into(),
            message: "m".into(),
            parent: None,
            timestamp: "t".into(),
            tree: Hash::from_hex("0".repeat(64)).unwrap(),
        };
        let json = String::from_utf8(body.canonical_bytes().unwrap()).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{"author":"a","message":"m","parent":null,"timestamp":"t","tree":"{}"}}"#,
                "0".repeat(64)
            )
        );
    }

    #[test]
    fn golden_hash_empty_file_body() {
        // Empty `FileBody` must always hash to this exact value. If this test
        // breaks, the on-disk format changed.
        let body = FileBody {
            chunk_size: 0,
            chunks: vec![],
            size: 0,
        };
        let canonical = body.canonical_bytes().unwrap();
        assert_eq!(&canonical, br#"{"chunk_size":0,"chunks":[],"size":0}"#);
        let h = body.hash().unwrap();
        // SHA-256 of the literal canonical bytes above.
        let expected = Hash::of(br#"{"chunk_size":0,"chunks":[],"size":0}"#);
        assert_eq!(h, expected);
    }

    #[test]
    fn hash_is_stable_across_re_serialization() {
        let body = FileBody {
            chunk_size: 8,
            chunks: vec![make_hash(0xAA), make_hash(0xBB)],
            size: 16,
        };
        let h1 = body.hash().unwrap();
        // Round-trip through gzip + parse, hash again.
        let storage = body.storage_bytes().unwrap();
        let decoded = FileBody::from_storage_bytes(&storage).unwrap();
        let h2 = decoded.hash().unwrap();
        assert_eq!(h1, h2);
    }

    // --- Chunks ------------------------------------------------------------

    #[test]
    fn chunk_hash_matches_raw_sha256() {
        let payload = b"raw chunk payload, no wrapper";
        let h = chunk_hash(payload);
        assert_eq!(h, Hash::of(payload));
    }

    #[test]
    fn chunk_storage_is_identity() {
        let payload = Bytes::from_static(b"abc");
        let stored = chunk_storage_bytes(payload.clone());
        assert_eq!(stored, payload);
    }
}
