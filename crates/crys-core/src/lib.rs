//! Chrysalis core library.
//!
//! See `docs/superpowers/specs/2026-05-24-chrysalis-design.md` for the
//! architecture this crate implements.

mod error;

pub use error::{Error, Result};

/// Crate version, surfaced to the CLI for `crys --version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
