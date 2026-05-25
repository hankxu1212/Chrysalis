use crys_core::global_config;
use crys_core::repo::Repo;

use crate::cli::ConfigAction;
use crate::commands::cwd;
use crate::config_keys::{get_global, get_repo, set_global, set_repo};
use crate::error::CliError;
use crate::output::{print_global, print_repo};

pub async fn run(action: ConfigAction) -> Result<(), CliError> {
    match action {
        ConfigAction::Show { global } => {
            tracing::info!(global, "config show: loading global config");
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
            let cwd = cwd()?;
            if let Ok(repo) = Repo::open(&cwd).await {
                println!();
                println!("repo ({}):", repo.crys_dir().display());
                print_repo(&repo);
            }
        }
        ConfigAction::Get { key, global } => {
            tracing::info!(key = %key, global, "config get: starting");
            if global {
                let g = global_config::load().await.map_err(CliError::from)?;
                match get_global(&g, &key) {
                    Some(v) => println!("{v}"),
                    None => return Err(CliError::User(format!("not set: {key}"))),
                }
            } else {
                let cwd = cwd()?;
                let repo = Repo::open(&cwd).await.map_err(CliError::from)?;
                tracing::info!(crys_dir = %repo.crys_dir().display(), "config get: repo opened");
                match get_repo(&repo, &key) {
                    Some(v) => println!("{v}"),
                    None => return Err(CliError::User(format!("not set: {key}"))),
                }
            }
        }
        ConfigAction::Set { key, value, global } => {
            tracing::info!(key = %key, value = %value, global, "config set: starting");
            if global {
                let mut g = global_config::load().await.map_err(CliError::from)?;
                set_global(&mut g, &key, Some(value))?;
                global_config::save(&g).await.map_err(CliError::from)?;
                tracing::info!("config set: global saved");
            } else {
                let cwd = cwd()?;
                let mut repo = Repo::open(&cwd).await.map_err(CliError::from)?;
                tracing::info!(crys_dir = %repo.crys_dir().display(), "config set: repo opened");
                let mut new_config = repo.config().clone();
                set_repo(&mut new_config, &key, Some(value))?;
                repo.write_config(new_config)
                    .await
                    .map_err(CliError::from)?;
                tracing::info!("config set: repo config written");
            }
        }
        ConfigAction::Unset { key, global } => {
            tracing::info!(key = %key, global, "config unset: starting");
            if global {
                let mut g = global_config::load().await.map_err(CliError::from)?;
                set_global(&mut g, &key, None)?;
                global_config::save(&g).await.map_err(CliError::from)?;
                tracing::info!("config unset: global saved");
            } else {
                let cwd = cwd()?;
                let mut repo = Repo::open(&cwd).await.map_err(CliError::from)?;
                tracing::info!(crys_dir = %repo.crys_dir().display(), "config unset: repo opened");
                let mut new_config = repo.config().clone();
                set_repo(&mut new_config, &key, None)?;
                repo.write_config(new_config)
                    .await
                    .map_err(CliError::from)?;
                tracing::info!("config unset: repo config written");
            }
        }
    }
    Ok(())
}
