use clap::{Parser, Subcommand};
use crys_core::{Repo, S3Uri};

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
    /// In Phase 2 this only sets up the local `.crys/` layout. The S3-side
    /// `config.json` and `HEAD` are written when the S3 backend lands in
    /// Phase 4.
    Init {
        /// Remote S3 URI, e.g. `s3://my-bucket/path/to/repo`.
        s3_uri: String,
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
        Command::Init { s3_uri } => {
            // Validate the URI shape early so users get a clear error before
            // we touch the filesystem.
            S3Uri::parse(&s3_uri)?;
            let cwd = std::env::current_dir()?;
            let repo = Repo::init(&cwd, &s3_uri).await?;
            println!(
                "initialized empty Chrysalis repository in {}",
                repo.crys_dir().display()
            );
        }
    }
    Ok(())
}
