use std::path::PathBuf;

use crys_core::log::log;
use crys_core::objects::Hash;
use crys_core::repo::Repo;
use crys_core::stage::{self, ResetMode};
use crys_core::status::status;

use crate::commands::cwd;
use crate::error::CliError;
use crate::output::{print_log, print_status, LogStyle};
use crate::progress::{print_progress_summary, ProgressBundle};

pub async fn add(paths: Vec<PathBuf>) -> Result<(), CliError> {
    tracing::info!(paths = ?paths, "add: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "add: repo opened");
    let store = repo.store().await.map_err(CliError::from)?;
    let progress = ProgressBundle::new();
    let mut total = 0usize;
    for path in paths {
        tracing::info!(path = %path.display(), "add: staging path");
        let staged = stage::add_with_progress(&repo, &store, &path, &progress.handle)
            .await
            .map_err(CliError::from)?;
        tracing::info!(path = %path.display(), staged = staged.len(), "add: path staged");
        total += staged.len();
    }
    print_progress_summary(&progress);
    println!("staged {total} file(s)");
    Ok(())
}

pub async fn commit(message: String, author: Option<String>) -> Result<(), CliError> {
    tracing::info!(message = %message, author = ?author, "commit: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "commit: repo opened");
    let store = repo.store().await.map_err(CliError::from)?;
    let author =
        author.unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()));
    tracing::info!(author = %author, "commit: writing commit");
    let hash = stage::commit(&repo, &store, &author, &message)
        .await
        .map_err(CliError::from)?;
    tracing::info!(hash = %hash.as_hex(), "commit: written");
    println!("[{}] {message}", &hash.as_hex()[..7]);
    Ok(())
}

pub async fn status_cmd() -> Result<(), CliError> {
    tracing::info!("status: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "status: repo opened");
    let store = repo.store().await.map_err(CliError::from)?;
    let s = status(&repo, &store).await.map_err(CliError::from)?;
    print_status(&s);
    Ok(())
}

pub async fn log_cmd(limit: Option<usize>, graph: bool, oneline: bool) -> Result<(), CliError> {
    tracing::info!(limit = ?limit, graph, oneline, "log: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "log: repo opened");
    let store = repo.store().await.map_err(CliError::from)?;
    let entries = log(&repo, &store, limit).await.map_err(CliError::from)?;
    tracing::info!(count = entries.len(), "log: entries gathered");
    let head = repo.head().await.map_err(CliError::from)?;
    let remote_head = repo.remote_head().await.map_err(CliError::from)?;
    // `--graph` implies one-line; otherwise honor `--oneline` as written.
    let style = match (graph, oneline) {
        (true, _) => LogStyle::Graph,
        (false, true) => LogStyle::Oneline,
        (false, false) => LogStyle::Default,
    };
    print_log(&entries, head.as_ref(), remote_head.as_ref(), style);
    Ok(())
}

pub async fn reset(commit: Option<String>, soft: bool, hard: bool) -> Result<(), CliError> {
    tracing::info!(commit = ?commit, soft, hard, "reset: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "reset: repo opened");
    let store = repo.store().await.map_err(CliError::from)?;

    let target = match commit.as_deref() {
        // `HEAD` as a literal acts the same as omitting the arg.
        None | Some("HEAD") => None,
        Some(hex) => Some(Hash::from_hex(hex.to_string()).map_err(CliError::from)?),
    };

    let mode = match (soft, hard) {
        (true, _) => ResetMode::Soft,
        (_, true) => ResetMode::Hard,
        _ => ResetMode::Mixed,
    };

    tracing::info!(target = ?target.as_ref().map(|h| h.as_hex()), mode = ?mode, "reset: applying");
    let new_head = stage::reset(&repo, &store, target.as_ref(), mode)
        .await
        .map_err(CliError::from)?;
    tracing::info!(new_head = ?new_head.as_ref().map(|h| h.as_hex()), "reset: applied");

    let label = match mode {
        ResetMode::Soft => "soft",
        ResetMode::Mixed => "mixed",
        ResetMode::Hard => "hard",
    };
    match new_head {
        Some(h) => println!("HEAD is now at {} ({label})", &h.as_hex()[..12]),
        None => println!("HEAD reset to no commit ({label})"),
    }
    Ok(())
}

pub async fn gc(dry_run: bool) -> Result<(), CliError> {
    tracing::info!(dry_run, "gc: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "gc: repo opened");
    let store = repo.store().await.map_err(CliError::from)?;
    let report = crys_core::gc::gc(&repo, &store, dry_run)
        .await
        .map_err(CliError::from)?;
    tracing::info!(
        removed = report.removed.len(),
        kept = report.kept,
        "gc: complete"
    );
    if report.removed.is_empty() {
        println!("nothing to collect ({} live)", report.kept);
    } else {
        let verb = if dry_run { "would remove" } else { "removed" };
        for hash in &report.removed {
            println!("{verb} {}", &hash.as_hex()[..12]);
        }
        println!();
        println!(
            "{} object(s) {verb}, {} kept",
            report.removed.len(),
            report.kept
        );
    }
    Ok(())
}

pub async fn clean(dry_run: bool) -> Result<(), CliError> {
    tracing::info!(dry_run, "clean: starting");
    let cwd = cwd()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "clean: repo opened");
    let report = crys_core::clean::clean(&repo, dry_run)
        .await
        .map_err(CliError::from)?;
    tracing::info!(removed = report.removed.len(), "clean: complete");
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
