use crys_core::Error;

#[derive(Debug)]
pub enum CliError {
    User(String),
    Network(String),
    Corruption(String),
    Other(String),
}

impl From<Error> for CliError {
    fn from(value: Error) -> Self {
        match value {
            // User errors (design §10).
            Error::RepoExists(_)
            | Error::NothingToCommit
            | Error::DirtyWorkingTree
            | Error::NotFastForward
            | Error::NotARepo(_)
            | Error::InvalidS3Uri(_)
            | Error::InvalidHash(_) => CliError::User(value.to_string()),
            // Corruption.
            Error::CorruptObject { .. } => CliError::Corruption(value.to_string()),
            // Network / S3.
            Error::S3(_) | Error::NotFound { .. } | Error::PreconditionFailed { .. } => {
                CliError::Network(value.to_string())
            }
            // Local I/O / JSON: lump under "user" since they're typically
            // misuse (missing path, bad config).
            Error::Io(_) | Error::Json(_) => CliError::User(value.to_string()),
        }
    }
}

impl From<anyhow::Error> for CliError {
    fn from(value: anyhow::Error) -> Self {
        CliError::Other(value.to_string())
    }
}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        CliError::User(value.to_string())
    }
}
