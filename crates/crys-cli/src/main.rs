use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::{Parser, Subcommand};
use crys_core::clean::clean;
use crys_core::global_config::{self, resolve_aws, GlobalConfig};
use crys_core::log::{log, LogEntry};
use crys_core::repo::init_remote;
use crys_core::status::{status, Change, Status};
use crys_core::store::S3Store;
use crys_core::sync::{
    clone_with_progress, fetch_with_progress, pull_with_progress, push_with_progress, Progress,
    ProgressHandle,
};
use crys_core::{stage, Error, Repo, S3Client, S3Uri};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};

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
}

#[derive(Debug, Subcommand)]
enum ConfigAction {
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
        Command::Init {
            s3_uri,
            local_only,
            profile,
            region,
        } => {
            let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
            let cwd = std::env::current_dir()?;
            let repo = Repo::init_with(&cwd, &s3_uri, profile.clone(), region.clone())
                .await
                .map_err(CliError::from)?;
            if !local_only {
                let global = global_config::load().await.map_err(CliError::from)?;
                let resolved = resolve_aws(
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
            let progress = make_progress();
            let head = fetch_with_progress(&repo, &remote, &progress.handle)
                .await
                .map_err(CliError::from)?;
            print_progress_summary(&progress);
            match head {
                Some(h) => println!("fetched {}", &h.as_hex()[..12]),
                None => println!("remote is empty"),
            }
        }
        Command::Pull {} => {
            let (repo, remote) = open_repo_and_remote().await?;
            let progress = make_progress();
            let head = pull_with_progress(&repo, &remote, &progress.handle)
                .await
                .map_err(CliError::from)?;
            print_progress_summary(&progress);
            match head {
                Some(h) => println!("pulled to {}", &h.as_hex()[..12]),
                None => println!("nothing to pull"),
            }
        }
        Command::Push {} => {
            let (repo, remote) = open_repo_and_remote().await?;
            let progress = make_progress();
            let head = push_with_progress(&repo, &remote, &progress.handle)
                .await
                .map_err(CliError::from)?;
            print_progress_summary(&progress);
            match head {
                Some(h) => println!("pushed to {}", &h.as_hex()[..12]),
                None => println!("nothing to push"),
            }
        }
        Command::Status {} => {
            let cwd = std::env::current_dir()?;
            let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
            let store = repo.store().await.map_err(CliError::from)?;
            let s = status(&repo, &store).await.map_err(CliError::from)?;
            print_status(&s);
        }
        Command::Log { limit } => {
            let cwd = std::env::current_dir()?;
            let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
            let store = repo.store().await.map_err(CliError::from)?;
            let entries = log(&repo, &store, limit).await.map_err(CliError::from)?;
            print_log(&entries);
        }
        Command::Clone {
            s3_uri,
            dest,
            profile,
            region,
        } => {
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
            let resolved = resolve_aws(profile.as_deref(), region.as_deref(), None, None, &global);
            let client = S3Client::with_profile_and_region(
                resolved.profile.as_deref(),
                resolved.region.as_deref(),
            )
            .await;
            let remote = S3Store::new(client, uri);
            let progress = make_progress();
            let repo = clone_with_progress(&remote, &s3_uri, &dest, &progress.handle)
                .await
                .map_err(CliError::from)?;
            print_progress_summary(&progress);

            // Pin the AWS settings into the cloned repo so future commands
            // pick them up without env vars.
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
        }
        Command::Clean { dry_run } => {
            let cwd = std::env::current_dir()?;
            let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
            let report = clean(&repo, dry_run).await.map_err(CliError::from)?;
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
        }
        Command::Config { action } => run_config(action).await?,
    }
    Ok(())
}

async fn open_repo_and_remote() -> Result<(Repo, S3Store), CliError> {
    let cwd = std::env::current_dir()?;
    let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
    let uri = S3Uri::parse(&repo.config().remote).map_err(CliError::from)?;
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved = resolve_aws(
        None,
        None,
        repo.config().aws_profile.as_deref(),
        repo.config().region.as_deref(),
        &global,
    );
    let client =
        S3Client::with_profile_and_region(resolved.profile.as_deref(), resolved.region.as_deref())
            .await;
    let store = S3Store::new(client, uri);
    Ok((repo, store))
}

async fn run_config(action: ConfigAction) -> Result<(), CliError> {
    match action {
        ConfigAction::Show { global } => {
            let g = global_config::load().await.map_err(CliError::from)?;
            println!(
                "global ({}):",
                global_config::config_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<unknown>".into())
            );
            print_global(&g);
            if global {
                return Ok(());
            }
            // Try to also show per-repo, but don't error out if not in a repo.
            let cwd = std::env::current_dir()?;
            if let Ok(repo) = Repo::open(&cwd).await {
                println!();
                println!("repo ({}):", repo.crys_dir().display());
                print_repo(&repo);
            }
        }
        ConfigAction::Get { key, global } => {
            if global {
                let g = global_config::load().await.map_err(CliError::from)?;
                match get_global(&g, &key) {
                    Some(v) => println!("{v}"),
                    None => return Err(CliError::User(format!("not set: {key}"))),
                }
            } else {
                let cwd = std::env::current_dir()?;
                let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
                match get_repo(&repo, &key) {
                    Some(v) => println!("{v}"),
                    None => return Err(CliError::User(format!("not set: {key}"))),
                }
            }
        }
        ConfigAction::Set { key, value, global } => {
            if global {
                let mut g = global_config::load().await.map_err(CliError::from)?;
                set_global(&mut g, &key, Some(value))?;
                global_config::save(&g).await.map_err(CliError::from)?;
            } else {
                let cwd = std::env::current_dir()?;
                let mut repo = Repo::open(&cwd).await.map_err(CliError::from)?;
                let mut new_config = repo.config().clone();
                set_repo(&mut new_config, &key, Some(value))?;
                repo.write_config(new_config)
                    .await
                    .map_err(CliError::from)?;
            }
        }
        ConfigAction::Unset { key, global } => {
            if global {
                let mut g = global_config::load().await.map_err(CliError::from)?;
                set_global(&mut g, &key, None)?;
                global_config::save(&g).await.map_err(CliError::from)?;
            } else {
                let cwd = std::env::current_dir()?;
                let mut repo = Repo::open(&cwd).await.map_err(CliError::from)?;
                let mut new_config = repo.config().clone();
                set_repo(&mut new_config, &key, None)?;
                repo.write_config(new_config)
                    .await
                    .map_err(CliError::from)?;
            }
        }
    }
    Ok(())
}

fn print_global(g: &GlobalConfig) {
    println!(
        "  default_profile = {}",
        g.default_profile.as_deref().unwrap_or("<unset>")
    );
    println!(
        "  default_region  = {}",
        g.default_region.as_deref().unwrap_or("<unset>")
    );
}

fn print_repo(repo: &Repo) {
    let c = repo.config();
    println!("  remote      = {}", c.remote);
    println!(
        "  aws_profile = {}",
        c.aws_profile.as_deref().unwrap_or("<unset>")
    );
    println!(
        "  region      = {}",
        c.region.as_deref().unwrap_or("<unset>")
    );
    println!("  chunk_size  = {}", c.chunk_size);
}

fn get_global(g: &GlobalConfig, key: &str) -> Option<String> {
    match key {
        "default_profile" => g.default_profile.clone(),
        "default_region" => g.default_region.clone(),
        _ => None,
    }
}

fn set_global(g: &mut GlobalConfig, key: &str, value: Option<String>) -> Result<(), CliError> {
    match key {
        "default_profile" => g.default_profile = value,
        "default_region" => g.default_region = value,
        other => return Err(CliError::User(format!("unknown global key: {other}"))),
    }
    Ok(())
}

fn get_repo(repo: &Repo, key: &str) -> Option<String> {
    let c = repo.config();
    match key {
        "remote" => Some(c.remote.clone()),
        "aws_profile" => c.aws_profile.clone(),
        "region" => c.region.clone(),
        "chunk_size" => Some(c.chunk_size.to_string()),
        _ => None,
    }
}

fn set_repo(
    config: &mut crys_core::repo::Config,
    key: &str,
    value: Option<String>,
) -> Result<(), CliError> {
    match key {
        "aws_profile" => config.aws_profile = value,
        "region" => config.region = value,
        "remote" => match value {
            Some(v) => config.remote = v,
            None => return Err(CliError::User("cannot unset `remote`".into())),
        },
        other => return Err(CliError::User(format!("unknown repo key: {other}"))),
    }
    Ok(())
}

/// CLI progress reporter. Renders one progress bar per phase (chunks → files
/// → trees → commits) into a `MultiProgress` so they stack vertically and
/// don't clobber each other when phases overlap.
///
/// Tracks total bytes per phase so we can print a final summary.
struct IndicatifProgress {
    multi: MultiProgress,
    state: Mutex<IndicatifState>,
}

#[derive(Default)]
struct IndicatifState {
    /// Current bar per phase name.
    bars: std::collections::HashMap<String, ProgressBar>,
    /// Total bytes copied per phase.
    bytes: std::collections::HashMap<String, u64>,
    /// Total objects copied per phase.
    counts: std::collections::HashMap<String, u64>,
}

impl IndicatifProgress {
    fn new() -> Self {
        Self {
            multi: MultiProgress::new(),
            state: Mutex::new(IndicatifState::default()),
        }
    }

    fn summary(&self) -> (u64, u64) {
        let state = self.state.lock().unwrap();
        // Walking is the discovery phase; it doesn't transfer anything, so
        // exclude it from the "transferred" tally.
        let bytes: u64 = state
            .bytes
            .iter()
            .filter(|(k, _)| k.as_str() != "walking")
            .map(|(_, v)| *v)
            .sum();
        let count: u64 = state
            .counts
            .iter()
            .filter(|(k, _)| k.as_str() != "walking")
            .map(|(_, v)| *v)
            .sum();
        (count, bytes)
    }
}

fn phase_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:>8} [{bar:30.cyan/blue}] {pos}/{len} {msg}")
        .unwrap()
        .progress_chars("=> ")
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:>8} {spinner} {pos} objects {msg}")
        .unwrap()
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

impl Progress for IndicatifProgress {
    fn start_phase(&self, kind: &str, total: usize) {
        let pb = if total == 0 {
            // Indeterminate: discovery phase. Render as a spinner that ticks
            // every 100 ms so the bar stays alive even if we don't `inc`
            // for a while (e.g. waiting on a single GET).
            let pb = self.multi.add(ProgressBar::new_spinner());
            pb.set_style(spinner_style());
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            pb
        } else {
            let pb = self.multi.add(ProgressBar::new(total as u64));
            pb.set_style(phase_style());
            pb
        };
        pb.set_prefix(kind.to_string());
        let mut state = self.state.lock().unwrap();
        state.bars.insert(kind.to_string(), pb);
    }

    fn object_copied(&self, kind: &str, bytes: u64) {
        let mut state = self.state.lock().unwrap();
        *state.bytes.entry(kind.to_string()).or_insert(0) += bytes;
        *state.counts.entry(kind.to_string()).or_insert(0) += 1;
        let total_bytes = state.bytes[kind];
        if let Some(bar) = state.bars.get(kind) {
            bar.inc(1);
            // Suppress bytes message for the walking phase — we don't copy
            // anything there, so reporting bytes would be misleading.
            if kind != "walking" {
                bar.set_message(format!("{}", HumanBytes(total_bytes)));
            }
        }
    }

    fn finish_phase(&self, kind: &str) {
        let state = self.state.lock().unwrap();
        if let Some(bar) = state.bars.get(kind) {
            let bytes = state.bytes.get(kind).copied().unwrap_or(0);
            if kind == "walking" {
                let count = state.counts.get(kind).copied().unwrap_or(0);
                bar.finish_with_message(format!("({count} objects discovered)"));
            } else {
                bar.finish_with_message(format!("done • {}", HumanBytes(bytes)));
            }
        }
    }
}

/// Holds both a typed handle (for the post-run summary) and the trait-object
/// view (for sync.rs). Both point at the same `IndicatifProgress`.
struct ProgressBundle {
    inner: Arc<IndicatifProgress>,
    handle: ProgressHandle,
}

impl ProgressBundle {
    fn new() -> Self {
        let inner = Arc::new(IndicatifProgress::new());
        let handle: ProgressHandle = inner.clone();
        Self { inner, handle }
    }
}

fn make_progress() -> ProgressBundle {
    ProgressBundle::new()
}

/// After a transfer command, print "transferred N objects (X.X)" with
/// totals accumulated by the indicatif reporter.
fn print_progress_summary(bundle: &ProgressBundle) {
    let (count, bytes) = bundle.inner.summary();
    if count > 0 {
        println!("transferred {count} object(s) ({})", HumanBytes(bytes));
    }
}

fn print_log(entries: &[LogEntry]) {
    if entries.is_empty() {
        println!("(no commits yet)");
        return;
    }
    for (i, entry) in entries.iter().enumerate() {
        if i > 0 {
            println!();
        }
        let tag = match (entry.in_local, entry.in_remote) {
            (true, true) => "local, remote",
            (true, false) => "local",
            (false, true) => "remote",
            (false, false) => "?",
        };
        println!("commit {} ({tag})", entry.hash.as_hex());
        println!("Author: {}", entry.commit.author);
        println!("Date:   {}", entry.commit.timestamp);
        println!();
        for line in entry.commit.message.lines() {
            println!("    {line}");
        }
    }
}

fn print_status(s: &Status) {
    match &s.head {
        Some(h) => println!("On commit {}", &h.as_hex()[..12]),
        None => println!("No commits yet"),
    }

    if s.is_clean() {
        println!("nothing to commit, working tree clean");
        return;
    }

    if !s.staged.is_empty() {
        println!();
        println!("Changes to be committed:");
        println!("  (use `crys commit -m <msg>` to record)");
        for (path, change) in &s.staged {
            println!("\t{:<10} {}", label(change), path);
        }
    }

    if !s.unstaged.is_empty() {
        println!();
        println!("Changes not staged for commit:");
        println!("  (use `crys add <path>` to update what will be committed)");
        for (path, change) in &s.unstaged {
            println!("\t{:<10} {}", label(change), path);
        }
    }

    if !s.untracked.is_empty() {
        println!();
        println!("Untracked files:");
        println!("  (use `crys add <path>` to include)");
        for path in &s.untracked {
            println!("\t{path}");
        }
    }
}

fn label(change: &Change) -> &'static str {
    match change {
        Change::Added => "new file:",
        Change::Modified => "modified:",
        Change::Deleted => "deleted:",
    }
}

fn init_tracing(verbose: bool) {
    let default_level = if verbose { "info" } else { "warn" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
