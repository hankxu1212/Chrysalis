use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "crys",
    about = "Chrysalis: S3-backed file sharing with Git-like semantics.",
    version = env!("CARGO_PKG_VERSION"),
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize a new Chrysalis repository in the current directory.
    Init {
        /// Remote S3 URI, e.g. `s3://my-bucket/path/to/repo`.
        s3_uri: String,

        /// Skip the S3 round-trip. The remote will need to be initialized later.
        #[arg(long)]
        local_only: bool,

        /// Pin an AWS profile in `.crys/config` so future commands don't
        /// need `AWS_PROFILE`.
        #[arg(long)]
        profile: Option<String>,

        /// Pin an AWS region in `.crys/config`.
        #[arg(long)]
        region: Option<String>,
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
        /// Pin an AWS profile in the cloned repo's `.crys/config`.
        #[arg(long)]
        profile: Option<String>,
        /// Pin an AWS region in the cloned repo's `.crys/config`.
        #[arg(long)]
        region: Option<String>,
    },
    /// Show the working tree status.
    Status {},
    /// List commit history newest-first.
    Log {
        /// Cap output to N most-recent commits.
        #[arg(short = 'n', long)]
        limit: Option<usize>,
        /// Render a graph column alongside one-line entries.
        /// Chrysalis history is linear, so the column is always a single `*`,
        /// but ref decorations and short timestamps make this the practical
        /// browsing format.
        #[arg(long)]
        graph: bool,
        /// One-line per commit (short hash, decoration, message, age, author).
        /// Implied by `--graph`.
        #[arg(long)]
        oneline: bool,
    },
    /// Remove files in the working tree that aren't tracked in the index.
    /// Honors `.crysignore`.
    Clean {
        /// Show what would be removed without deleting anything.
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
    /// Read or write Chrysalis config (per-repo or global).
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// List the file tree of a remote Chrysalis repo at HEAD without cloning.
    Tree {
        /// Remote S3 URI, e.g. `s3://my-bucket/path/to/repo`.
        s3_uri: String,
        /// AWS profile override (otherwise resolved from global config / env).
        #[arg(long)]
        profile: Option<String>,
        /// AWS region override.
        #[arg(long)]
        region: Option<String>,
    },
    /// Sweep unreachable objects from the local `.crys/objects/` cache.
    /// Live set = HEAD ∪ REMOTE_HEAD ∪ everything currently in the index.
    /// Does not touch the remote.
    Gc {
        /// Show what would be removed without deleting.
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
    /// Move HEAD and (optionally) reset the index / working tree.
    /// Default mode rebuilds the index from the target commit's tree
    /// (i.e. unstages changes) without touching the working tree.
    Reset {
        /// Target commit hash. Defaults to current HEAD.
        commit: Option<String>,
        /// Move HEAD only; leave the index and working tree alone.
        #[arg(long, conflicts_with = "hard")]
        soft: bool,
        /// Move HEAD, rebuild the index, and overwrite the working tree.
        /// DESTROYS uncommitted local changes to tracked files.
        #[arg(long)]
        hard: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigAction {
    /// Show all values in scope (global merged with repo if inside one).
    Show {
        /// Show global config only.
        #[arg(long)]
        global: bool,
    },
    /// Get one value. Keys: `aws_profile`, `region`, `default_profile`,
    /// `default_region`, `remote`.
    Get {
        key: String,
        /// Read from global config instead of the per-repo config.
        #[arg(long)]
        global: bool,
    },
    /// Set one value. Same keys as `get`.
    Set {
        key: String,
        value: String,
        /// Write to global config instead of the per-repo config.
        #[arg(long)]
        global: bool,
    },
    /// Unset (clear) one value.
    Unset {
        key: String,
        #[arg(long)]
        global: bool,
    },
}
