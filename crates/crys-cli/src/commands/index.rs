use std::path::PathBuf;

use crys_core::log::log;
use crys_core::repo::Repo;
use crys_core::stage;
use crys_core::status::status;

use crate::error::CliError;
use crate::output::{print_log_entry, print_status};

pub async fn add(paths: Vec<PathBuf>) -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let store = repo.store().await.map_err(CliError::from)?;
    let mut total = 0usize;
    for path in paths {
        let staged = stage::add(&repo, &store, &path)
            .await
            .map_err(CliError::from)?;
        total += staged.len();
    }
    println!("staged {total} file(s)");
    Ok(())
}

pub async fn commit(message: String, author: Option<String>) -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let store = repo.store().await.map_err(CliError::from)?;
    let author = author
        .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()));
    let hash = stage::commit(&repo, &store, &author, &message)
        .await
        .map_err(CliError::from)?;
    println!("[{}] {message}", &hash.as_hex()[..7]);
    Ok(())
}

pub async fn status_cmd() -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let store = repo.store().await.map_err(CliError::from)?;
    let s = status(&repo, &store).await.map_err(CliError::from)?;
    print_status(&s);
    Ok(())
}

pub async fn log_cmd(limit: Option<usize>) -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let store = repo.store().await.map_err(CliError::from)?;
    let entries = log(&repo, &store, limit).await.map_err(CliError::from)?;
    print_log_entry(&entries);
    Ok(())
}

pub async fn clean(dry_run: bool) -> Result<(), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let report = crys_core::clean::clean(&repo, dry_run)
        .await
        .map_err(CliError::from)?;
    if report.removed.is_empty() {
        println!("nothing to clean");
    } else {
        let verb = if dry_run { "would remove" } else { "removed" };
        for path in &report.removed {
            println!("{verb} {path}");
        }
        println!();
        println!("{} file(s) {verb}", report.removed.len());
    }
    Ok(())
}
