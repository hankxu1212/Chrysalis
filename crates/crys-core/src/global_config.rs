//! User-level Chrysalis config at `~/.config/chrysalis/config.json`.
//!
//! Holds AWS defaults (`default_profile`, `default_region`) used as a
//! fallback when a per-repo `.crys/config` doesn't pin them and no env
//! vars are set. Resolution order applied by [`resolve_aws`]:
//!
//! 1. Explicit CLI flag (passed in via `cli_*` args)
//! 2. Environment (`AWS_PROFILE` / `AWS_REGION`)
//! 3. Per-repo config (`Repo::config().aws_profile` / `.region`)
//! 4. This global config
//! 5. Whatever the AWS SDK default chain picks up
//!
//! This file is intentionally simple JSON — no TOML dependency, same shape
//! as the per-repo config, manually editable.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::Result;

const CONFIG_DIR: &str = "chrysalis";
const CONFIG_FILE: &str = "config.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default)]
    pub default_profile: Option<String>,
    #[serde(default)]
    pub default_region: Option<String>,
}

/// Path to the global config file. Honors `XDG_CONFIG_HOME`, falling back
/// to `~/.config/chrysalis/config.json`.
pub fn config_path() -> Option<PathBuf> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return Some(PathBuf::from(xdg).join(CONFIG_DIR).join(CONFIG_FILE));
        }
    }
    let home = std::env::var("HOME").ok().map(PathBuf::from)?;
    Some(home.join(".config").join(CONFIG_DIR).join(CONFIG_FILE))
}

/// Load the global config, returning a default if the file doesn't exist.
pub async fn load() -> Result<GlobalConfig> {
    let Some(path) = config_path() else {
        return Ok(GlobalConfig::default());
    };
    let bytes = match fs::read(&path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(GlobalConfig::default());
        }
        Err(e) => return Err(e.into()),
    };
    Ok(serde_json::from_slice(&bytes).unwrap_or_default())
}

/// Persist the global config, creating the parent directory if needed.
pub async fn save(config: &GlobalConfig) -> Result<()> {
    let path = config_path().ok_or_else(|| {
        std::io::Error::other("could not determine global config path: HOME unset")
    })?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let bytes = serde_json::to_vec_pretty(config)?;
    fs::write(&path, bytes).await?;
    Ok(())
}

/// Resolved AWS settings to feed the SDK. `None` fields fall through to the
/// AWS default credential chain.
#[derive(Debug, Clone, Default)]
pub struct ResolvedAws {
    pub profile: Option<String>,
    pub region: Option<String>,
}

/// Apply the documented resolution order. The CLI passes `cli_profile` /
/// `cli_region` from `--profile`/`--region` flags (None when unset).
/// `repo_profile` / `repo_region` come from `.crys/config`.
pub fn resolve_aws(
    cli_profile: Option<&str>,
    cli_region: Option<&str>,
    repo_profile: Option<&str>,
    repo_region: Option<&str>,
    global: &GlobalConfig,
) -> ResolvedAws {
    let env_profile = std::env::var("AWS_PROFILE").ok().filter(|s| !s.is_empty());
    let env_region = std::env::var("AWS_REGION")
        .ok()
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .filter(|s| !s.is_empty());

    let profile = cli_profile
        .map(str::to_string)
        .or(env_profile)
        .or_else(|| repo_profile.map(str::to_string))
        .or_else(|| global.default_profile.clone());

    let region = cli_region
        .map(str::to_string)
        .or(env_region)
        .or_else(|| repo_region.map(str::to_string))
        .or_else(|| global.default_region.clone());

    ResolvedAws { profile, region }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Tests in this module manipulate process-wide env vars; serialize them
    /// behind a single mutex so the parallel test harness doesn't race.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        unsafe {
            std::env::remove_var("AWS_PROFILE");
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
        }
    }

    fn set_env(k: &str, v: &str) {
        unsafe { std::env::set_var(k, v) }
    }

    #[test]
    fn cli_overrides_everything() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        set_env("AWS_PROFILE", "from-env");
        let global = GlobalConfig {
            default_profile: Some("from-global".into()),
            default_region: Some("eu-west-1".into()),
        };
        let r = resolve_aws(
            Some("from-cli"),
            Some("us-east-1"),
            Some("from-repo"),
            Some("us-west-2"),
            &global,
        );
        assert_eq!(r.profile.as_deref(), Some("from-cli"));
        assert_eq!(r.region.as_deref(), Some("us-east-1"));
        clear_env();
    }

    #[test]
    fn env_overrides_repo_and_global() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        set_env("AWS_PROFILE", "from-env");
        set_env("AWS_REGION", "from-env-region");
        let global = GlobalConfig {
            default_profile: Some("from-global".into()),
            default_region: Some("eu-west-1".into()),
        };
        let r = resolve_aws(None, None, Some("from-repo"), Some("us-west-2"), &global);
        assert_eq!(r.profile.as_deref(), Some("from-env"));
        assert_eq!(r.region.as_deref(), Some("from-env-region"));
        clear_env();
    }

    #[test]
    fn repo_overrides_global() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let global = GlobalConfig {
            default_profile: Some("from-global".into()),
            default_region: Some("eu-west-1".into()),
        };
        let r = resolve_aws(None, None, Some("from-repo"), Some("us-west-2"), &global);
        assert_eq!(r.profile.as_deref(), Some("from-repo"));
        assert_eq!(r.region.as_deref(), Some("us-west-2"));
        clear_env();
    }

    #[test]
    fn falls_through_to_global() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let global = GlobalConfig {
            default_profile: Some("from-global".into()),
            default_region: Some("eu-west-1".into()),
        };
        let r = resolve_aws(None, None, None, None, &global);
        assert_eq!(r.profile.as_deref(), Some("from-global"));
        assert_eq!(r.region.as_deref(), Some("eu-west-1"));
        clear_env();
    }

    // Note: tests in this module manipulate process-wide env vars, which is
    // inherently racy with parallel test execution. We tolerate this by
    // clearing env at the start of every test. If you need stricter
    // isolation, run with `--test-threads=1`.
}
