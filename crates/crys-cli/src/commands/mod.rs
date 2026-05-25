pub mod config;
pub mod index;
pub mod repo;
pub mod sync;

use crys_core::global_config;
use crys_core::repo::Repo;
use crys_core::s3::{S3Client, S3Uri};
use crys_core::store::S3Store;

use crate::error::CliError;

/// Open the repo at CWD and build an `S3Store` against the configured remote.
/// Used by every sync-style command (fetch/push/pull).
pub(crate) async fn open_repo_and_remote() -> Result<(Repo, S3Store), CliError> {
    tracing::info!("open_repo_and_remote: reading current_dir");
    let cwd = cwd()?;
    tracing::info!(cwd = %cwd.display(), "open_repo_and_remote: opening repo");
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(
        crys_dir = %repo.crys_dir().display(),
        remote = %repo.config().remote,
        "open_repo_and_remote: repo opened"
    );
    let uri = S3Uri::parse(&repo.config().remote).map_err(CliError::from)?;
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved = global_config::resolve_aws(
        None,
        None,
        repo.config().aws_profile.as_deref(),
        repo.config().region.as_deref(),
        &global,
    );
    tracing::info!(
        bucket = %uri.bucket,
        key = %uri.key,
        profile = ?resolved.profile,
        region = ?resolved.region,
        "open_repo_and_remote: building S3 client"
    );
    let client =
        S3Client::with_profile_and_region(resolved.profile.as_deref(), resolved.region.as_deref())
            .await;
    Ok((repo, S3Store::new(client, uri)))
}

/// Wrapper around `std::env::current_dir` that logs the underlying error
/// (typically a macOS TCC denial when running from `~/Desktop`/`~/Documents`).
pub(crate) fn cwd() -> Result<std::path::PathBuf, CliError> {
    std::env::current_dir().map_err(|e| {
        tracing::info!(
            kind = ?e.kind(),
            raw_os_error = ?e.raw_os_error(),
            "current_dir failed (on macOS this is often a TCC denial; try a non-Desktop/Documents dir or grant the terminal Full Disk Access)"
        );
        CliError::from(e)
    })
}
