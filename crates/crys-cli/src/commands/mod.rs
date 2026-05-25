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
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let uri = S3Uri::parse(&repo.config().remote).map_err(CliError::from)?;
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved = global_config::resolve_aws(
        None,
        None,
        repo.config().aws_profile.as_deref(),
        repo.config().region.as_deref(),
        &global,
    );
    let client = S3Client::with_profile_and_region(
        resolved.profile.as_deref(),
        resolved.region.as_deref(),
    )
    .await;
    Ok((repo, S3Store::new(client, uri)))
}
