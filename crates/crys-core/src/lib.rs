//! Chrysalis core library.
//!
//! See `docs/superpowers/specs/2026-05-24-chrysalis-design.md` for the
//! architecture this crate implements.

pub mod chunker;
pub mod clean;
mod error;
pub mod global_config;
pub mod log;
pub mod objects;
pub mod repo;
pub mod s3;
pub mod stage;
pub mod status;
pub mod store;
pub mod sync;

pub use error::{Error, Result};
pub use objects::{CommitBody, EntryMode, FileBody, Hash, ObjectKind, TreeBody, TreeEntry};
pub use repo::{Config, IndexEntry, IndexFile, Repo};
pub use s3::{S3Client, S3Uri};
pub use store::{LocalStore, MemoryStore, ObjectStore, S3Store};

/// Crate version, surfaced to the CLI for `crys --version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
