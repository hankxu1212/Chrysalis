use std::path::PathBuf;

use clap::{Parser, Subcommand};
use crys_core::repo::init_remote;
use crys_core::{stage, Repo, S3Client, S3Uri};

#[derive(Debug, Parser)]
#[command(
    name = "crys",
    about = "Chrysalis: S3-backed file sharing with Git-like semantics.",
    version = crys_core::VERSION,
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a new Chrysalis repository in the current directory.
    ///
    /// Sets up the local `.crys/` layout, then bootstraps the remote on S3
    /// by conditionally creating `config.json` and an empty `HEAD`. If the
    /// remote already exists, the local `.crys/` is rolled back.
    Init {
        /// Remote S3 URI, e.g. `s3://my-bucket/path/to/repo`.
        s3_uri: String,

        /// Skip the S3 round-trip. Useful for offline tests; the remote will
        /// need to be initialized later.
        #[arg(long)]
        local_only: bool,
    },
    /// Stage paths into the index.
    Add {
        /// Files or directories to stage. Walks recursively, honoring `.crysignore`.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
    /// Record the index as a new commit.
    Commit {
        /// Commit message.
        #[arg(short, long)]
        message: String,
        /// Author string. Defaults to `$USER` or `unknown`.
        #[arg(long)]
        author: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Init { s3_uri, local_only } => {
            let uri = S3Uri::parse(&s3_uri)?;
            let cwd = std::env::current_dir()?;
            let repo = Repo::init(&cwd, &s3_uri).await?;
            if !local_only {
                let client = S3Client::from_env().await;
                if let Err(e) = init_remote(&client, &uri, repo.config().chunk_size).await {
                    // Roll back the local state so the user can retry cleanly.
                    let _ = std::fs::remove_dir_all(repo.crys_dir());
                    return Err(e.into());
                }
            }
            println!(
                "initialized empty Chrysalis repository in {} (remote {})",
                repo.crys_dir().display(),
                if local_only { "skipped" } else { &s3_uri }
            );
        }
        Command::Add { paths } => {
            let cwd = std::env::current_dir()?;
            let repo = Repo::open(&cwd).await?;
            let store = repo.store().await?;
            let mut total = 0usize;
            for path in paths {
                let staged = stage::add(&repo, &store, &path).await?;
                total += staged.len();
            }
            println!("staged {total} file(s)");
        }
        Command::Commit { message, author } => {
            let cwd = std::env::current_dir()?;
            let repo = Repo::open(&cwd).await?;
            let store = repo.store().await?;
            let author = author
                .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()));
            let hash = stage::commit(&repo, &store, &author, &message).await?;
            println!("[{}] {message}", &hash.as_hex()[..7]);
        }
    }
    Ok(())
}
