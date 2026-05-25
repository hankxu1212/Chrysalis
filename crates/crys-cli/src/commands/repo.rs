use std::path::PathBuf;

use crys_core::global_config;
use crys_core::repo::{init_remote, Repo};
use crys_core::s3::{S3Client, S3Uri};
use crys_core::store::S3Store;
use crys_core::sync::clone_with_progress;

use crate::commands::cwd;
use crate::error::CliError;
use crate::progress::{print_progress_summary, ProgressBundle};

pub async fn init(
    s3_uri: String,
    local_only: bool,
    profile: Option<String>,
    region: Option<String>,
) -> Result<(), CliError> {
    tracing::info!(s3_uri = %s3_uri, "init: parsing s3 uri");
    let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
    tracing::info!("init: reading current_dir");
    let cwd = cwd()?;
    tracing::info!(cwd = %cwd.display(), "init: cwd resolved");
    tracing::info!("init: creating local repo via Repo::init_with");
    let repo = Repo::init_with(&cwd, &s3_uri, profile.clone(), region.clone())
        .await
        .map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "init: local repo created");
    if !local_only {
        tracing::info!("init: loading global config");
        let global = global_config::load().await.map_err(CliError::from)?;
        let resolved = global_config::resolve_aws(
            profile.as_deref(),
            region.as_deref(),
            repo.config().aws_profile.as_deref(),
            repo.config().region.as_deref(),
            &global,
        );
        tracing::info!(
            profile = ?resolved.profile,
            region = ?resolved.region,
            "init: resolved aws settings"
        );
        let client = S3Client::with_profile_and_region(
            resolved.profile.as_deref(),
            resolved.region.as_deref(),
        )
        .await;
        tracing::info!(bucket = %uri.bucket, key = %uri.key, "init: bootstrapping remote");
        if let Err(e) = init_remote(&client, &uri, repo.config().chunk_size).await {
            tracing::info!(error = %e, "init: init_remote failed; rolling back local .crys");
            let _ = std::fs::remove_dir_all(repo.crys_dir());
            return Err(e.into());
        }
        tracing::info!("init: remote bootstrap ok");
    }
    println!(
        "initialized empty Chrysalis repository in {} (remote {})",
        repo.crys_dir().display(),
        if local_only { "skipped" } else { &s3_uri }
    );
    Ok(())
}

pub async fn clone(
    s3_uri: String,
    dest: Option<PathBuf>,
    profile: Option<String>,
    region: Option<String>,
) -> Result<(), CliError> {
    tracing::info!(s3_uri = %s3_uri, "clone: parsing s3 uri");
    let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
    let dest = match dest {
        Some(d) => d,
        None => {
            let segment = uri.key.trim_end_matches('/').rsplit('/').next();
            match segment {
                Some(s) if !s.is_empty() => PathBuf::from(s),
                _ => PathBuf::from(&uri.bucket),
            }
        }
    };
    tracing::info!(dest = %dest.display(), "clone: ensuring dest dir");
    std::fs::create_dir_all(&dest)?;
    tracing::info!("clone: loading global config");
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved =
        global_config::resolve_aws(profile.as_deref(), region.as_deref(), None, None, &global);
    tracing::info!(
        profile = ?resolved.profile,
        region = ?resolved.region,
        "clone: building S3 client"
    );
    let client = S3Client::with_profile_and_region(
        resolved.profile.as_deref(),
        resolved.region.as_deref(),
    )
    .await;
    let remote = S3Store::new(client, uri);
    let progress = ProgressBundle::new();
    tracing::info!("clone: starting clone_with_progress");
    let repo = clone_with_progress(&remote, &s3_uri, &dest, &progress.handle)
        .await
        .map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "clone: clone complete");
    print_progress_summary(&progress);
    if profile.is_some() || region.is_some() {
        tracing::info!("clone: persisting profile/region overrides to repo config");
        let mut repo = repo;
        let mut new_config = repo.config().clone();
        if let Some(p) = profile {
            new_config.aws_profile = Some(p);
        }
        if let Some(r) = region {
            new_config.region = Some(r);
        }
        repo.write_config(new_config)
            .await
            .map_err(CliError::from)?;
    }
    println!("cloned");
    Ok(())
}
