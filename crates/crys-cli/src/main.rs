use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use crys_core::repo::init_remote;
use crys_core::store::S3Store;
use crys_core::sync::{clone_repo, fetch, pull, push};
use crys_core::{stage, Error, Repo, S3Client, S3Uri};

#[derive(Debug, Parser)]
#[command(
    name = "crys",
    about = "Chrysalis: S3-backed file sharing with Git-like semantics.",
    version = crys_core::VERSION,
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Verbose tracing output. Equivalent to RUST_LOG=info.
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a new Chrysalis repository in the current directory.
    Init {
        /// Remote S3 URI, e.g. `s3://my-bucket/path/to/repo`.
        s3_uri: String,

        /// Skip the S3 round-trip. The remote will need to be initialized later.
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
    /// Refresh REMOTE_HEAD and download metadata for any new commits.
    Fetch {},
    /// Fetch then fast-forward the working tree to the remote tip.
    Pull {},
    /// Upload local commits to the remote (fast-forward only).
    Push {},
    /// Materialize a remote repository into a new local directory.
    Clone {
        /// Remote S3 URI to clone from.
        s3_uri: String,
        /// Destination directory. Defaults to the last path segment of the URI.
        dest: Option<PathBuf>,
    },
}

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

#[derive(Debug)]
enum CliError {
    User(String),
    Network(String),
    Corruption(String),
    Other(String),
}

impl From<Error> for CliError {
    fn from(value: Error) -> Self {
        match value {
            // User errors (design §10).
            Error::RepoExists(_)
            | Error::NothingToCommit
            | Error::DirtyWorkingTree
            | Error::NotFastForward
            | Error::NotARepo(_)
            | Error::InvalidS3Uri(_)
            | Error::InvalidHash(_) => CliError::User(value.to_string()),
            // Corruption.
            Error::CorruptObject { .. } => CliError::Corruption(value.to_string()),
            // Network / S3.
            Error::S3(_) | Error::NotFound { .. } | Error::PreconditionFailed { .. } => {
                CliError::Network(value.to_string())
            }
            // Local I/O / JSON: lump under "user" since they're typically
            // misuse (missing path, bad config).
            Error::Io(_) | Error::Json(_) => CliError::User(value.to_string()),
        }
    }
}

impl From<anyhow::Error> for CliError {
    fn from(value: anyhow::Error) -> Self {
        CliError::Other(value.to_string())
    }
}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        CliError::User(value.to_string())
    }
}

async fn async_main() -> Result<(), CliError> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match cli.command {
        Command::Init { s3_uri, local_only } => {
            let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
            let cwd = std::env::current_dir()?;
            let repo = Repo::init(&cwd, &s3_uri).await.map_err(CliError::from)?;
            if !local_only {
                let client = S3Client::from_env().await;
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
        }
        Command::Add { paths } => {
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
        }
        Command::Commit { message, author } => {
            let cwd = std::env::current_dir()?;
            let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
            let store = repo.store().await.map_err(CliError::from)?;
            let author = author
                .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()));
            let hash = stage::commit(&repo, &store, &author, &message)
                .await
                .map_err(CliError::from)?;
            println!("[{}] {message}", &hash.as_hex()[..7]);
        }
        Command::Fetch {} => {
            let (repo, remote) = open_repo_and_remote().await?;
            let head = fetch(&repo, &remote).await.map_err(CliError::from)?;
            match head {
                Some(h) => println!("fetched {}", &h.as_hex()[..12]),
                None => println!("remote is empty"),
            }
        }
        Command::Pull {} => {
            let (repo, remote) = open_repo_and_remote().await?;
            let head = pull(&repo, &remote).await.map_err(CliError::from)?;
            match head {
                Some(h) => println!("pulled to {}", &h.as_hex()[..12]),
                None => println!("nothing to pull"),
            }
        }
        Command::Push {} => {
            let (repo, remote) = open_repo_and_remote().await?;
            let head = push(&repo, &remote).await.map_err(CliError::from)?;
            match head {
                Some(h) => println!("pushed to {}", &h.as_hex()[..12]),
                None => println!("nothing to push"),
            }
        }
        Command::Clone { s3_uri, dest } => {
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
            let client = S3Client::from_env().await;
            let remote = S3Store::new(client, uri);
            let repo = clone_repo(&remote, &s3_uri, &dest)
                .await
                .map_err(CliError::from)?;
            println!("cloned to {}", repo.workdir().display());
        }
    }
    Ok(())
}

async fn open_repo_and_remote() -> Result<(Repo, S3Store), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let uri = S3Uri::parse(&repo.config().remote).map_err(CliError::from)?;
    let client = S3Client::from_env().await;
    let store = S3Store::new(client, uri);
    Ok((repo, store))
}

fn init_tracing(verbose: bool) {
    let default_level = if verbose { "info" } else { "warn" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
