use std::path::PathBuf;

use crys_core::global_config;
use crys_core::repo::{init_remote, Repo};
use crys_core::s3::{S3Client, S3Uri};
use crys_core::store::S3Store;
use crys_core::sync::clone_with_progress;

use crate::error::CliError;
use crate::progress::{print_progress_summary, ProgressBundle};

pub async fn init(
    s3_uri: String,
    local_only: bool,
    profile: Option<String>,
    region: Option<String>,
) -> Result<(), CliError> {
    let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
    let cwd = std::env::current_dir()?;
    let repo = Repo::init_with(&cwd, &s3_uri, profile.clone(), region.clone())
        .await
        .map_err(CliError::from)?;
    if !local_only {
        let global = global_config::load().await.map_err(CliError::from)?;
        let resolved = global_config::resolve_aws(
            profile.as_deref(),
            region.as_deref(),
            repo.config().aws_profile.as_deref(),
            repo.config().region.as_deref(),
            &global,
        );
        let client = S3Client::with_profile_and_region(
            resolved.profile.as_deref(),
            resolved.region.as_deref(),
        )
        .await;
        if let Err(e) = init_remote(&client, &uri, repo.config().chunk_size).await {
            let _ = std::fs::remove_dir_all(repo.crys_dir());
            return Err(e.into());
        }
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
    std::fs::create_dir_all(&dest)?;
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved =
        global_config::resolve_aws(profile.as_deref(), region.as_deref(), None, None, &global);
    let client = S3Client::with_profile_and_region(
        resolved.profile.as_deref(),
        resolved.region.as_deref(),
    )
    .await;
    let remote = S3Store::new(client, uri);
    let progress = ProgressBundle::new();
    let repo = clone_with_progress(&remote, &s3_uri, &dest, &progress.handle)
        .await
        .map_err(CliError::from)?;
    print_progress_summary(&progress);
    if profile.is_some() || region.is_some() {
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
