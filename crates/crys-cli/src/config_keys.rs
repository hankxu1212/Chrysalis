use crys_core::global_config::GlobalConfig;
use crys_core::repo::{Config, Repo};

use crate::error::CliError;

pub fn get_global(g: &GlobalConfig, key: &str) -> Option<String> {
    match key {
        "default_profile" => g.default_profile.clone(),
        "default_region" => g.default_region.clone(),
        _ => None,
    }
}

pub fn set_global(g: &mut GlobalConfig, key: &str, value: Option<String>) -> Result<(), CliError> {
    match key {
        "default_profile" => g.default_profile = value,
        "default_region" => g.default_region = value,
        other => return Err(CliError::User(format!("unknown global key: {other}"))),
    }
    Ok(())
}

pub fn get_repo(repo: &Repo, key: &str) -> Option<String> {
    let c = repo.config();
    match key {
        "remote" => Some(c.remote.clone()),
        "aws_profile" => c.aws_profile.clone(),
        "region" => c.region.clone(),
        "chunk_size" => Some(c.chunk_size.to_string()),
        _ => None,
    }
}

pub fn set_repo(config: &mut Config, key: &str, value: Option<String>) -> Result<(), CliError> {
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
