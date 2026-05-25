use crys_core::sync::{fetch_with_progress, pull_with_progress, push_with_progress};

use crate::commands::open_repo_and_remote;
use crate::error::CliError;
use crate::progress::{print_progress_summary, ProgressBundle};

pub async fn fetch() -> Result<(), CliError> {
    tracing::info!("fetch: starting");
    let (repo, remote) = open_repo_and_remote().await?;
    let progress = ProgressBundle::new();
    tracing::info!("fetch: running fetch_with_progress");
    let head = fetch_with_progress(&repo, &remote, &progress.handle)
        .await
        .map_err(CliError::from)?;
    tracing::info!(head = ?head.as_ref().map(|h| h.as_hex()), "fetch: complete");
    print_progress_summary(&progress);
    match head {
        Some(h) => println!("fetched {}", &h.as_hex()[..12]),
        None => println!("remote is empty"),
    }
    Ok(())
}

pub async fn push() -> Result<(), CliError> {
    tracing::info!("push: starting");
    let (repo, remote) = open_repo_and_remote().await?;
    let progress = ProgressBundle::new();
    tracing::info!("push: running push_with_progress");
    let head = push_with_progress(&repo, &remote, &progress.handle)
        .await
        .map_err(CliError::from)?;
    tracing::info!(head = ?head.as_ref().map(|h| h.as_hex()), "push: complete");
    print_progress_summary(&progress);
    match head {
        Some(h) => println!("pushed to {}", &h.as_hex()[..12]),
        None => println!("nothing to push"),
    }
    Ok(())
}

pub async fn pull() -> Result<(), CliError> {
    tracing::info!("pull: starting");
    let (repo, remote) = open_repo_and_remote().await?;
    let progress = ProgressBundle::new();
    tracing::info!("pull: running pull_with_progress");
    let head = pull_with_progress(&repo, &remote, &progress.handle)
        .await
        .map_err(CliError::from)?;
    tracing::info!(head = ?head.as_ref().map(|h| h.as_hex()), "pull: complete");
    print_progress_summary(&progress);
    match head {
        Some(h) => println!("pulled to {}", &h.as_hex()[..12]),
        None => println!("nothing to pull"),
    }
    Ok(())
}
