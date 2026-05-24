# Chrysalis

A platform-agnostic file-sharing CLI with Git-like semantics, backed entirely
by AWS S3. Built for large binary files: split into chunks, deduped across
versions, only changed chunks re-uploaded.

There is no Chrysalis server. A "remote" is an S3 prefix; commits, trees,
file content, and refs all live there as immutable objects plus one small
mutable pointer (`HEAD`).

See [docs/superpowers/specs/2026-05-24-chrysalis-design.md](docs/superpowers/specs/2026-05-24-chrysalis-design.md)
for the full design and [docs/superpowers/specs/2026-05-24-chrysalis-tasks.md](docs/superpowers/specs/2026-05-24-chrysalis-tasks.md)
for the implementation plan.

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
| `crys add <paths...>` | Stage files into the index. Walks recursively, honoring `.crysignore`. |
| `crys commit -m "<msg>"` | Record the index as a new commit. Refuses if the resulting tree matches `HEAD`. |
| `crys status` | Show staged / unstaged / untracked changes (git-style). |
| `crys log [-n N]` | List commit history newest-first. |
| `crys clean [-n]` | Remove untracked files. `-n` shows what would be removed. Honors `.crysignore`. |
| `crys config show \| get \| set \| unset [--global] <key> [<value>]` | Read/write per-repo or global config. |
| `crys fetch` | Refresh `REMOTE_HEAD` and download metadata for any new commits (no chunks). |
| `crys pull` | Fetch then fast-forward the working tree to the remote tip. Refuses if the working tree has uncommitted changes. |
| `crys push` | Upload local commits to the remote (fast-forward only). |
| `crys clone <s3-uri> [dest]` | Materialize a remote repo into a new local directory. |

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

- **No branches, merges, tags.** Linear history only.
- **Last-write-wins push.** Two clients pushing different descendants of the
  same `REMOTE_HEAD` race; the later writer's `HEAD` wins. Earlier client's
  commit becomes orphaned. Conditional-write push to detect this is on the
  v2 list.
- **No GC.** Orphaned objects (after a push race or aborted commit) remain
  in S3.
- **No `status`/`log`/`diff`/`checkout` commands.**
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
