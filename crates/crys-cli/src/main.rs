use std::process::ExitCode;

use clap::Parser;

mod cli;
mod commands;
mod config_keys;
mod error;
mod output;
mod progress;

use crate::cli::{Cli, Command};
use crate::error::CliError;

fn main() -> ExitCode {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let result = runtime.block_on(async_main());
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::User(msg)) => {
            eprintln!("error: {msg}");
            ExitCode::from(1)
        }
        Err(CliError::Network(msg)) => {
            eprintln!("error: {msg}");
            ExitCode::from(2)
        }
        Err(CliError::Corruption(msg)) => {
            eprintln!("error: {msg}");
            ExitCode::from(3)
        }
        Err(CliError::Other(msg)) => {
            eprintln!("error: {msg}");
            ExitCode::from(1)
        }
    }
}

fn init_tracing(verbose: bool) {
    let default_level = if verbose { "info" } else { "warn" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

async fn async_main() -> Result<(), CliError> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match cli.command {
        Command::Init {
            s3_uri,
            local_only,
            profile,
            region,
        } => commands::repo::init(s3_uri, local_only, profile, region).await,
        Command::Add { paths } => commands::index::add(paths).await,
        Command::Commit { message, author } => commands::index::commit(message, author).await,
        Command::Status {} => commands::index::status_cmd().await,
        Command::Log {
            limit,
            graph,
            oneline,
        } => commands::index::log_cmd(limit, graph, oneline).await,
        Command::Clean { dry_run } => commands::index::clean(dry_run).await,
        Command::Gc { dry_run } => commands::index::gc(dry_run).await,
        Command::Reset { commit, soft, hard } => commands::index::reset(commit, soft, hard).await,
        Command::Fetch {} => commands::sync::fetch().await,
        Command::Push {} => commands::sync::push().await,
        Command::Pull {} => commands::sync::pull().await,
        Command::Clone {
            s3_uri,
            dest,
            profile,
            region,
        } => commands::repo::clone(s3_uri, dest, profile, region).await,
        Command::Config { action } => commands::config::run(action).await,
        Command::Tree {
            s3_uri,
            profile,
            region,
        } => commands::repo::tree(s3_uri, profile, region).await,
    }
}
