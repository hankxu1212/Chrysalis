# Chrysalis

A platform-agnostic file-sharing CLI with Git-like semantics, backed entirely
by AWS S3. Built for large binary files: split into chunks, deduped across
versions, only changed chunks re-uploaded.

There is no Chrysalis server. A "remote" is an S3 prefix; commits, trees,
file content, and refs all live there as immutable objects plus one small
mutable pointer (`HEAD`).

## Motivation

Chrysalis is a small project aimed at one specific gap: simple version
control for directories full of multi-gigabyte binary blobs — datasets, ML
checkpoints, game assets, renders, audio stems, scanned documents.

Git assumes text. Git LFS bolts large-file storage onto Git but still needs
a server (or a paid host) and a separate workflow. Dedicated VCS-for-data
tools (DVC, lakeFS, etc.) bring their own services and concepts.

For a solo user or small team that just wants "git, but the remote is a
bucket I already pay for," all of those are heavy. **S3 is the cheapest
durable storage you can rent**, scales to terabytes without a thought, and
every cloud has an S3-compatible offering. Chrysalis treats an S3 prefix as
the entire remote — no server to run, no service to subscribe to. You pay
AWS for bytes-at-rest and bytes-out, and that's it.

The trade-off is intentional: linear history, fast-forward-only pushes, no
merges. If you want branches and rebases, use Git. If you want to share a
60 GB folder of binary blobs with someone else and only ship the bytes that
changed, this is for you.

## Install

```bash
cargo install --path crates/crys-cli
```

Pre-built binaries land on GitHub Releases when the next tag ships.

## Quickstart

```bash
# 1. Configure AWS credentials (any standard method works).
aws configure --profile chrysalis

# 2. Create a repo on S3 and locally.
mkdir my-art
cd my-art
AWS_PROFILE=chrysalis crys init s3://my-bucket/repos/my-art

# 3. Stage and commit files.
echo hello > note.txt
crys add .
crys commit -m "first commit"

# 4. Push to S3.
AWS_PROFILE=chrysalis crys push

# 5. Elsewhere, clone and pull.
AWS_PROFILE=chrysalis crys clone s3://my-bucket/repos/my-art
cd my-art
AWS_PROFILE=chrysalis crys pull
```

## Commands

| Command | Behavior |
|---|---|
| `crys init <s3-uri>` | Initialize `.crys/` and bootstrap the remote (`config.json` + empty `HEAD`). `--local-only` skips the S3 round-trip. |
| `crys clone <s3-uri> [dest]` | Materialize a remote repo into a new local directory. |
| `crys add <paths...>` | Stage files into the index. Walks recursively, honoring `.crysignore`. |
| `crys commit -m "<msg>"` | Record the index as a new commit. Refuses if the resulting tree matches `HEAD`. |
| `crys status` | Show staged / unstaged / untracked changes (git-style). |
| `crys log [-n N] [--graph] [--oneline]` | List commit history newest-first. `--graph` implies one-line entries with ref decorations. |
| `crys reset [<commit>] [--soft \| --hard]` | Move `HEAD`. Default rebuilds the index from the target tree (unstages); `--soft` moves only `HEAD`; `--hard` also overwrites the working tree. |
| `crys clean [-n]` | Remove untracked files. `-n` shows what would be removed. Honors `.crysignore`. |
| `crys gc [-n]` | Sweep unreachable objects from the local `.crys/objects/` cache. Live set = `HEAD` ∪ `REMOTE_HEAD` ∪ index. Does not touch the remote. |
| `crys fetch` | Refresh `REMOTE_HEAD` and download metadata for any new commits (no chunks). |
| `crys pull` | Fetch then fast-forward the working tree to the remote tip. Refuses if the working tree has uncommitted changes. |
| `crys push` | Upload local commits to the remote (fast-forward only). Serializes concurrent pushers via a HEAD compare-and-swap. |
| `crys tree <s3-uri>` | List the file tree of a remote repo at HEAD without cloning. |
| `crys config show \| get \| set \| unset [--global] <key> [<value>]` | Read/write per-repo or global config. |

## Authentication

Chrysalis uses the standard AWS credential chain via `aws-config`: env vars,
`~/.aws/credentials`, IAM role, SSO. Chrysalis never reads or stores
credentials directly.

To avoid setting `AWS_PROFILE` / `AWS_REGION` every time, configure once:

```bash
# Global default — applies everywhere unless overridden.
crys config set --global default_profile chrysalis-dev
crys config set --global default_region  us-west-2

# Per-repo override — pinned at init time, or set later.
crys init s3://bucket/path --profile chrysalis-dev --region us-west-2
crys config set aws_profile chrysalis-dev
crys config set region us-west-2

# Inspect what's in scope.
crys config show
```

Resolution order: `--profile`/`--region` flag → `AWS_PROFILE`/`AWS_REGION` env →
per-repo `.crys/config` → global `~/.config/chrysalis/config.json` → AWS SDK
default chain.

## Configuration

- `.crys/config` — local repo config: `remote`, `chunk_size`.
- `.crysignore` — gitignore-syntax patterns excluded from `crys add`.
- `RUST_LOG=info` — verbose tracing. `--verbose` on the CLI is equivalent.
- `CRYS_TEST_BUCKET` — used by integration tests; unset to skip them.

## Known limitations (v1)

- **No branches, merges, tags.** Linear history only. Diverged work has to
  be resolved by the user — typically `crys reset` to the common ancestor
  and re-stage.
- **No remote GC.** Push uses an S3 ETag compare-and-swap so concurrent
  pushers serialize cleanly, but objects from a losing push (or any
  abandoned history) remain in the bucket. `crys gc` only sweeps the local
  cache. Remote sweeping is on the v2 list.
- **No `diff` / `checkout` / `revert` commands.**
- **Fixed-size chunking.** Inserts in the middle of a large binary
  invalidate every downstream chunk. Content-defined chunking is on the v2
  list.
- **Encryption beyond default S3 SSE is out of scope.**

## Development

```bash
cargo build --workspace
cargo test --workspace        # unit tests + integration tests in skip mode
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

To run integration tests against real S3:

```bash
# .env.local example:
#   CRYS_TEST_BUCKET=my-test-bucket
#   AWS_PROFILE=chrysalis
#   AWS_REGION=us-west-2
set -a && . .env.local && set +a
cargo test --workspace
```

## License

Apache-2.0
