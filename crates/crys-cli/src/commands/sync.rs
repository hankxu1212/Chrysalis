use crys_core::sync::{fetch_with_progress, pull_with_progress, push_with_progress};

use crate::commands::open_repo_and_remote;
use crate::error::CliError;
use crate::progress::{print_progress_summary, ProgressBundle};

pub async fn fetch() -> Result<(), CliError> {
    let (repo, remote) = open_repo_and_remote().await?;
    let progress = ProgressBundle::new();
    let head = fetch_with_progress(&repo, &remote, &progress.handle)
        .await
        .map_err(CliError::from)?;
    print_progress_summary(&progress);
    match head {
        Some(h) => println!("fetched {}", &h.as_hex()[..12]),
        None => println!("remote is empty"),
    }
    Ok(())
}

pub async fn push() -> Result<(), CliError> {
    let (repo, remote) = open_repo_and_remote().await?;
    let progress = ProgressBundle::new();
    let head = push_with_progress(&repo, &remote, &progress.handle)
        .await
        .map_err(CliError::from)?;
    print_progress_summary(&progress);
    match head {
        Some(h) => println!("pushed to {}", &h.as_hex()[..12]),
        None => println!("nothing to push"),
    }
    Ok(())
}

pub async fn pull() -> Result<(), CliError> {
    let (repo, remote) = open_repo_and_remote().await?;
    let progress = ProgressBundle::new();
    let head = pull_with_progress(&repo, &remote, &progress.handle)
        .await
        .map_err(CliError::from)?;
    print_progress_summary(&progress);
    match head {
        Some(h) => println!("pulled to {}", &h.as_hex()[..12]),
        None => println!("nothing to pull"),
    }
    Ok(())
}
