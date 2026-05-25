use crys_core::global_config;
use crys_core::repo::Repo;

use crate::cli::ConfigAction;
use crate::config_keys::{get_global, get_repo, set_global, set_repo};
use crate::error::CliError;
use crate::output::{print_global, print_repo};

pub async fn run(action: ConfigAction) -> Result<(), CliError> {
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
