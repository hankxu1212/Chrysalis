use chrono::{DateTime, Utc};
use console::style;
use crys_core::global_config::GlobalConfig;
use crys_core::log::LogEntry;
use crys_core::objects::Hash;
use crys_core::repo::Repo;
use crys_core::status::{Change, Status};

/// How to render `crys log`. Controlled by `--graph`/`--oneline` flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStyle {
    /// Default git-log-style multi-line view.
    Default,
    /// One line per commit (no graph column).
    Oneline,
    /// One line per commit, prefixed with a `*` graph column.
    Graph,
}

pub fn print_global(g: &GlobalConfig) {
    println!(
        "  default_profile = {}",
        g.default_profile.as_deref().unwrap_or("<unset>")
    );
    println!(
        "  default_region  = {}",
        g.default_region.as_deref().unwrap_or("<unset>")
    );
}

pub fn print_repo(repo: &Repo) {
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

/// Render commit history per `style`. `head` and `remote_head` drive the
/// `(HEAD -> main, origin/main)`-style ref decoration on each line.
pub fn print_log(
    entries: &[LogEntry],
    head: Option<&Hash>,
    remote_head: Option<&Hash>,
    style: LogStyle,
) {
    if entries.is_empty() {
        println!("(no commits yet)");
        return;
    }
    let now = Utc::now();
    match style {
        LogStyle::Default => print_log_default(entries),
        LogStyle::Oneline => print_log_oneline(entries, head, remote_head, false, now),
        LogStyle::Graph => print_log_oneline(entries, head, remote_head, true, now),
    }
}

fn print_log_default(entries: &[LogEntry]) {
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

fn print_log_oneline(
    entries: &[LogEntry],
    head: Option<&Hash>,
    remote_head: Option<&Hash>,
    graph: bool,
    now: DateTime<Utc>,
) {
    for entry in entries {
        let short = &entry.hash.as_hex()[..7];
        let decoration = format_decoration(&entry.hash, head, remote_head);
        let age = format_age(&entry.commit.timestamp, now);
        let subject = entry
            .commit
            .message
            .lines()
            .next()
            .unwrap_or("");
        let prefix = if graph { "* " } else { "" };
        println!(
            "{prefix}{hash} -{deco} {subject} {age} {author}",
            hash = style(short).yellow(),
            deco = decoration,
            subject = subject,
            age = style(format!("({age})")).green(),
            author = style(format!("<{}>", entry.commit.author)).blue().bold(),
        );
    }
}

/// Build the `(HEAD -> main, origin/main)` decoration. Chrysalis only has one
/// branch (linear history) so the names are fixed: `main` for local HEAD and
/// `origin/main` for REMOTE_HEAD. Returns "" when no refs land on this commit.
fn format_decoration(
    hash: &Hash,
    head: Option<&Hash>,
    remote_head: Option<&Hash>,
) -> String {
    let is_head = head == Some(hash);
    let is_remote_head = remote_head == Some(hash);
    let mut parts: Vec<String> = Vec::new();
    if is_head {
        parts.push(format!("{} -> {}", style("HEAD").bold().cyan(), style("main").bold().green()));
    }
    if is_remote_head {
        parts.push(style("origin/main").bold().red().to_string());
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" {}{}{}", style("(").yellow(), parts.join(&style(", ").yellow().to_string()), style(")").yellow())
    }
}

/// Render an RFC3339 timestamp as a coarse "N units ago" string. Falls back
/// to the raw string if parsing fails — better to surface the original than
/// to lie about the age.
fn format_age(rfc3339: &str, now: DateTime<Utc>) -> String {
    let parsed = match DateTime::parse_from_rfc3339(rfc3339) {
        Ok(dt) => dt.with_timezone(&Utc),
        Err(_) => return rfc3339.to_string(),
    };
    let delta = now.signed_duration_since(parsed);
    let secs = delta.num_seconds();
    if secs < 0 {
        // Clock skew or post-dated commits — round to "just now" rather than
        // print a negative duration.
        return "just now".into();
    }
    let mins = delta.num_minutes();
    let hours = delta.num_hours();
    let days = delta.num_days();
    let weeks = days / 7;
    let months = days / 30;
    let years = days / 365;
    if secs < 60 {
        format!("{secs} second{} ago", plural(secs))
    } else if mins < 60 {
        format!("{mins} minute{} ago", plural(mins))
    } else if hours < 24 {
        format!("{hours} hour{} ago", plural(hours))
    } else if days < 14 {
        format!("{days} day{} ago", plural(days))
    } else if weeks < 8 {
        format!("{weeks} week{} ago", plural(weeks))
    } else if months < 12 {
        format!("{months} month{} ago", plural(months))
    } else {
        format!("{years} year{} ago", plural(years))
    }
}

fn plural(n: i64) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn age_buckets_match_git_style() {
        let now = Utc.with_ymd_and_hms(2026, 5, 24, 12, 0, 0).unwrap();
        assert_eq!(format_age("2026-05-24T11:59:50+00:00", now), "10 seconds ago");
        assert_eq!(format_age("2026-05-24T11:46:00+00:00", now), "14 minutes ago");
        assert_eq!(format_age("2026-05-23T22:00:00+00:00", now), "14 hours ago");
        assert_eq!(format_age("2026-05-22T12:00:00+00:00", now), "2 days ago");
        assert_eq!(format_age("2026-05-01T12:00:00+00:00", now), "3 weeks ago");
        assert_eq!(format_age("2025-09-01T12:00:00+00:00", now), "8 months ago");
        assert_eq!(format_age("2024-01-01T12:00:00+00:00", now), "2 years ago");
    }

    #[test]
    fn age_handles_post_dated_commits() {
        let now = Utc.with_ymd_and_hms(2026, 5, 24, 12, 0, 0).unwrap();
        assert_eq!(format_age("2026-05-25T00:00:00+00:00", now), "just now");
    }

    #[test]
    fn age_falls_back_to_raw_on_parse_failure() {
        let now = Utc.with_ymd_and_hms(2026, 5, 24, 12, 0, 0).unwrap();
        assert_eq!(format_age("not-a-date", now), "not-a-date");
    }

    #[test]
    fn age_singular_when_exactly_one_unit() {
        let now = at("2026-05-24T12:00:00+00:00");
        assert_eq!(format_age("2026-05-24T11:59:00+00:00", now), "1 minute ago");
        assert_eq!(format_age("2026-05-24T11:00:00+00:00", now), "1 hour ago");
    }
}

pub fn print_status(s: &Status) {
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
            println!("\t{}", style(format!("{:<10} {}", label(change), path)).green());
        }
    }

    if !s.unstaged.is_empty() {
        println!();
        println!("Changes not staged for commit:");
        println!("  (use `crys add <path>` to update what will be committed)");
        for (path, change) in &s.unstaged {
            println!("\t{}", style(format!("{:<10} {}", label(change), path)).red());
        }
    }

    if !s.untracked.is_empty() {
        println!();
        println!("Untracked files:");
        println!("  (use `crys add <path>` to include)");
        for path in &s.untracked {
            println!("\t{}", style(path).red());
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
