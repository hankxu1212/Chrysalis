use std::path::PathBuf;

use console::style;
use crys_core::global_config;
use crys_core::objects::{CanonicalJson, CommitBody, EntryMode, Hash, TreeBody};
use crys_core::repo::{init_remote, Repo};
use crys_core::s3::{S3Client, S3Uri};
use crys_core::store::{ObjectStore, S3Store};
use crys_core::sync::clone_with_progress;

use crate::commands::cwd;
use crate::error::CliError;
use crate::progress::{print_progress_summary, ProgressBundle};

pub async fn init(
    s3_uri: String,
    local_only: bool,
    profile: Option<String>,
    region: Option<String>,
) -> Result<(), CliError> {
    tracing::info!(s3_uri = %s3_uri, "init: parsing s3 uri");
    let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
    tracing::info!("init: reading current_dir");
    let cwd = cwd()?;
    tracing::info!(cwd = %cwd.display(), "init: cwd resolved");
    tracing::info!("init: creating local repo via Repo::init_with");
    let repo = Repo::init_with(&cwd, &s3_uri, profile.clone(), region.clone())
        .await
        .map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "init: local repo created");
    if !local_only {
        tracing::info!("init: loading global config");
        let global = global_config::load().await.map_err(CliError::from)?;
        let resolved = global_config::resolve_aws(
            profile.as_deref(),
            region.as_deref(),
            repo.config().aws_profile.as_deref(),
            repo.config().region.as_deref(),
            &global,
        );
        tracing::info!(
            profile = ?resolved.profile,
            region = ?resolved.region,
            "init: resolved aws settings"
        );
        let client = S3Client::with_profile_and_region(
            resolved.profile.as_deref(),
            resolved.region.as_deref(),
        )
        .await;
        tracing::info!(bucket = %uri.bucket, key = %uri.key, "init: bootstrapping remote");
        if let Err(e) = init_remote(&client, &uri, repo.config().chunk_size).await {
            tracing::info!(error = %e, "init: init_remote failed; rolling back local .crys");
            let _ = std::fs::remove_dir_all(repo.crys_dir());
            return Err(e.into());
        }
        tracing::info!("init: remote bootstrap ok");
    }
    println!(
        "initialized empty Chrysalis repository in {} (remote {})",
        repo.crys_dir().display(),
        if local_only { "skipped" } else { &s3_uri }
    );
    Ok(())
}

pub async fn clone(
    s3_uri: String,
    dest: Option<PathBuf>,
    profile: Option<String>,
    region: Option<String>,
) -> Result<(), CliError> {
    tracing::info!(s3_uri = %s3_uri, "clone: parsing s3 uri");
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
    tracing::info!(dest = %dest.display(), "clone: ensuring dest dir");
    std::fs::create_dir_all(&dest)?;
    tracing::info!("clone: loading global config");
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved =
        global_config::resolve_aws(profile.as_deref(), region.as_deref(), None, None, &global);
    tracing::info!(
        profile = ?resolved.profile,
        region = ?resolved.region,
        "clone: building S3 client"
    );
    let client = S3Client::with_profile_and_region(
        resolved.profile.as_deref(),
        resolved.region.as_deref(),
    )
    .await;
    let remote = S3Store::new(client, uri);
    let progress = ProgressBundle::new();
    tracing::info!("clone: starting clone_with_progress");
    let repo = clone_with_progress(&remote, &s3_uri, &dest, &progress.handle)
        .await
        .map_err(CliError::from)?;
    tracing::info!(crys_dir = %repo.crys_dir().display(), "clone: clone complete");
    print_progress_summary(&progress);
    if profile.is_some() || region.is_some() {
        tracing::info!("clone: persisting profile/region overrides to repo config");
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
    Ok(())
}

pub async fn tree(
    s3_uri: String,
    profile: Option<String>,
    region: Option<String>,
) -> Result<(), CliError> {
    tracing::info!(s3_uri = %s3_uri, "tree: parsing s3 uri");
    let uri = S3Uri::parse(&s3_uri).map_err(CliError::from)?;
    let global = global_config::load().await.map_err(CliError::from)?;
    let resolved =
        global_config::resolve_aws(profile.as_deref(), region.as_deref(), None, None, &global);
    let client = S3Client::with_profile_and_region(
        resolved.profile.as_deref(),
        resolved.region.as_deref(),
    )
    .await;

    // Treat the absence of `config.json` as "this prefix isn't a crys repo".
    // `init_remote` writes config.json first, so its presence is the canonical
    // existence signal (see crys-core::repo::init_remote).
    let prefix = uri.key.trim_end_matches('/');
    let config_key = if prefix.is_empty() {
        "config.json".to_string()
    } else {
        format!("{prefix}/config.json")
    };
    if !client.head(&uri.bucket, &config_key).await.map_err(CliError::from)? {
        return Err(CliError::User(format!(
            "not a chrysalis repository: s3://{}/{}",
            uri.bucket, prefix
        )));
    }

    let store = S3Store::new(client, uri.clone());
    let head = store.get_head().await.map_err(CliError::from)?;
    let Some(head) = head else {
        println!("{s3_uri}  (empty repo, no commits yet)");
        return Ok(());
    };

    let commit_bytes = store.get(&head).await.map_err(CliError::from)?;
    let commit = CommitBody::from_storage_bytes(&commit_bytes).map_err(CliError::from)?;

    println!(
        "{}  {} {}",
        style(&s3_uri).cyan().bold(),
        style("HEAD").dim(),
        style(&head.as_hex()[..12]).yellow(),
    );
    print_tree(&store, &commit.tree).await?;
    Ok(())
}

/// Style a file name based on its extension. The use case Chrysalis is
/// built for (`README.md` motivation) is large binary blobs — highlight
/// those so they stand out from text/config noise.
fn style_file_name(name: &str) -> String {
    let ext = name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
    match ext.as_deref() {
        // Heavyweight binaries: model weights, datasets, archives, media.
        Some(
            "ckpt" | "safetensors" | "pt" | "pth" | "onnx" | "h5" | "npz" | "npy"
            | "parquet" | "arrow" | "feather"
            | "zip" | "tar" | "gz" | "tgz" | "bz2" | "xz" | "zst" | "7z"
            | "iso" | "img" | "bin" | "dat"
            | "mp4" | "mov" | "mkv" | "webm" | "avi"
            | "wav" | "flac" | "aif" | "aiff" | "psd" | "tif" | "tiff" | "exr" | "blend"
            | "fbx" | "obj" | "glb" | "gltf",
        ) => style(name).magenta().bold().to_string(),
        // Common images: visible but not screaming.
        Some("png" | "jpg" | "jpeg" | "gif" | "bmp" | "webp" | "svg") => {
            style(name).cyan().to_string()
        }
        // Plain text / source / config: leave default to keep noise low.
        _ => name.to_string(),
    }
}

/// Walk the tree on `store` rooted at `root` and print each entry with
/// ASCII tree connectors. Iterative to avoid the BoxFuture / extra-dep cost
/// of async recursion.
async fn print_tree(store: &S3Store, root: &Hash) -> Result<(), CliError> {
    // Each frame is the iterator over one tree's entries plus the prefix
    // string callers below it should inherit. We push a frame when descending
    // into a Dir entry and pop when its iterator is exhausted.
    struct Frame {
        entries: std::vec::IntoIter<crys_core::TreeEntry>,
        prefix: String,
    }

    async fn load(store: &S3Store, hash: &Hash) -> Result<Vec<crys_core::TreeEntry>, CliError> {
        let bytes = store.get(hash).await.map_err(CliError::from)?;
        let body = TreeBody::from_storage_bytes(&bytes).map_err(CliError::from)?;
        Ok(body.entries)
    }

    let mut stack: Vec<Frame> = vec![Frame {
        entries: load(store, root).await?.into_iter(),
        prefix: String::new(),
    }];

    while let Some(frame) = stack.last_mut() {
        let Some(entry) = frame.entries.next() else {
            stack.pop();
            continue;
        };
        let is_last = frame.entries.len() == 0;
        let prefix = frame.prefix.clone();
        let connector = style(if is_last { "└── " } else { "├── " }).dim();
        let styled_name = match entry.mode {
            EntryMode::Dir => style(format!("{}/", entry.name)).blue().bold().to_string(),
            EntryMode::File => style_file_name(&entry.name),
        };
        println!("{prefix}{connector}{styled_name}");

        if entry.mode == EntryMode::Dir {
            // Continuation glyph and pad are dimmed so directory/file names
            // remain the visual anchor at every depth.
            let cont = if is_last { "    " } else { "│   " };
            let child_prefix = format!("{prefix}{}", style(cont).dim());
            let entries = load(store, &entry.hash).await?;
            stack.push(Frame {
                entries: entries.into_iter(),
                prefix: child_prefix,
            });
        }
    }
    Ok(())
}


