//! Chrysalis core library.
//!
//! See `docs/superpowers/specs/2026-05-24-chrysalis-design.md` for the
//! architecture this crate implements.

mod error;
pub mod objects;
pub mod s3;

pub use error::{Error, Result};
pub use objects::{CommitBody, EntryMode, FileBody, Hash, ObjectKind, TreeBody, TreeEntry};
pub use s3::{S3Client, S3Uri};

/// Crate version, surfaced to the CLI for `crys --version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
