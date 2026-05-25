use crys_core::global_config::GlobalConfig;
use crys_core::log::LogEntry;
use crys_core::repo::Repo;
use crys_core::status::{Change, Status};

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

pub fn print_log_entry(entries: &[LogEntry]) {
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
