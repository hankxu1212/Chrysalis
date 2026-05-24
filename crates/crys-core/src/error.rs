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

    #[error("invalid s3 uri: {0}")]
    InvalidS3Uri(String),

    #[error("object not found: s3://{bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("precondition failed (object already exists): s3://{bucket}/{key}")]
    PreconditionFailed { bucket: String, key: String },

    #[error("s3 error: {0}")]
    S3(String),

    #[error("invalid hash: {0}")]
    InvalidHash(String),
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
            Error::InvalidS3Uri("not-an-s3-uri".into()),
            Error::NotFound {
                bucket: "b".into(),
                key: "k".into(),
            },
            Error::PreconditionFailed {
                bucket: "b".into(),
                key: "k".into(),
            },
            Error::S3("boom".into()),
        ];
        for err in cases {
            let s = err.to_string();
            assert!(!s.contains('\n'), "error message must be one line: {s:?}");
            assert!(!s.is_empty());
        }
    }
}
