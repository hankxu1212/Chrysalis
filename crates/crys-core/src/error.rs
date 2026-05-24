use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Top-level error type for the Chrysalis core library.
///
/// Variants are stable surface for CLI exit-code mapping (Phase 6) and for
/// library callers that want to match exhaustively. Phases 1+ extend this enum
/// as new failure modes appear; the CLI maps each variant to an exit code.
#[derive(Debug, Error)]
pub enum Error {
    #[error("repository already exists at {0}")]
    RepoExists(String),

    #[error("nothing to commit")]
    NothingToCommit,

    #[error("working tree has uncommitted changes")]
    DirtyWorkingTree,

    #[error("remote has changes; pull first")]
    NotFastForward,

    #[error("corrupt object {hash} in {source_store}")]
    CorruptObject {
        hash: String,
        source_store: &'static str,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_one_line() {
        let cases: Vec<Error> = vec![
            Error::RepoExists("s3://bucket/p".into()),
            Error::NothingToCommit,
            Error::DirtyWorkingTree,
            Error::NotFastForward,
            Error::CorruptObject {
                hash: "deadbeef".into(),
                source_store: "local",
            },
        ];
        for err in cases {
            let s = err.to_string();
            assert!(!s.contains('\n'), "error message must be one line: {s:?}");
            assert!(!s.is_empty());
        }
    }
}
