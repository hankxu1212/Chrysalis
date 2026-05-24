use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "crys",
    about = "Chrysalis: S3-backed file sharing with Git-like semantics.",
    version = crys_core::VERSION,
)]
struct Cli {}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let _ = Cli::parse();
    // Phase 0: no commands wired yet. `--version` and `--help` are handled by
    // clap before reaching this point.
    eprintln!("crys {}: no commands implemented yet", crys_core::VERSION);
    std::process::exit(2);
}
