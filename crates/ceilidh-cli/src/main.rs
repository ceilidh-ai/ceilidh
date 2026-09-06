use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "ceilidh",
    version,
    about = "Deterministic orchestration for your staff of agents"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the caller: HTTP API, embedded web UI, session state
    Serve(ServeArgs),
    /// Run a band runner that executes turns on this machine
    Runner(RunnerArgs),
    /// Run the caller and a local runner in one process (single box / dev)
    Up(UpArgs),
    /// Serve the sub-agent MCP over stdio (attached by runners to every harness)
    Mcp(McpArgs),
}

#[derive(clap::Args)]
struct McpArgs {
    /// Base URL of the caller
    #[arg(long, env = "CEILIDH_SERVER", default_value = "http://127.0.0.1:8080")]
    server: String,
    /// Bearer token (env: CEILIDH_TOKEN)
    #[arg(long, env = "CEILIDH_TOKEN")]
    token: Option<String>,
    /// The session this MCP serves; spawned children hang off it
    #[arg(long, env = "CEILIDH_PARENT_SESSION")]
    parent: uuid::Uuid,
}

/// Where the harness CLIs live and how the sub-agent MCP reaches the caller.
#[derive(clap::Args, Clone)]
struct HarnessArgs {
    /// Claude Code binary
    #[arg(long, env = "CEILIDH_CLAUDE_BIN", default_value = "claude")]
    claude_bin: String,
    /// Codex binary
    #[arg(long, env = "CEILIDH_CODEX_BIN", default_value = "codex")]
    codex_bin: String,
    /// Cursor agent binary
    #[arg(long, env = "CEILIDH_CURSOR_BIN", default_value = "cursor-agent")]
    cursor_bin: String,
    /// The seat's Codex login file, symlinked into each session's CODEX_HOME
    #[arg(long, env = "CEILIDH_CODEX_AUTH")]
    codex_auth: Option<PathBuf>,
    /// The ceilidh binary spawned as `ceilidh mcp`; defaults to this executable
    #[arg(long, env = "CEILIDH_BIN")]
    ceilidh_bin: Option<PathBuf>,
    /// Turns this runner plays at once
    #[arg(long, env = "CEILIDH_MAX_TURNS", default_value_t = 3)]
    max_turns: usize,
}

impl HarnessArgs {
    fn into_config(self, server_url: &str, token: Option<&String>) -> Result<ceilidh_runner::HarnessConfig> {
        let ceilidh_bin = match self.ceilidh_bin {
            Some(path) => path,
            None => std::env::current_exe().context("locate the ceilidh binary")?,
        };
        let codex_auth = match self.codex_auth {
            Some(path) => path,
            None => {
                let home = std::env::var("HOME").context("HOME is not set")?;
                PathBuf::from(home).join(".codex").join("auth.json")
            }
        };
        Ok(ceilidh_runner::HarnessConfig {
            claude_bin: self.claude_bin,
            codex_bin: self.codex_bin,
            cursor_bin: self.cursor_bin,
            ceilidh_bin,
            codex_auth,
            server_url: server_url.to_string(),
            token: token.cloned(),
        })
    }
}

#[derive(clap::Args)]
struct ServeArgs {
    /// Address to bind
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    /// SQLite database file
    #[arg(long, default_value = "ceilidh.db")]
    db: PathBuf,
    /// Bearer token required on every request (env: CEILIDH_TOKEN)
    #[arg(long, env = "CEILIDH_TOKEN")]
    token: Option<String>,
    /// Directory of built web assets; defaults to web/dist when present
    #[arg(long)]
    web_dir: Option<PathBuf>,
    /// Repository prefilled into the new-session form (env: CEILIDH_DEFAULT_REPO)
    #[arg(long, env = "CEILIDH_DEFAULT_REPO")]
    default_repo: Option<String>,
}

#[derive(clap::Args)]
struct RunnerArgs {
    /// Base URL of the caller
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    server: String,
    /// Bearer token (env: CEILIDH_TOKEN)
    #[arg(long, env = "CEILIDH_TOKEN")]
    token: Option<String>,
    /// Stable runner identity; defaults to <host>:<user>
    #[arg(long)]
    runner_id: Option<String>,
    /// Where session workspaces live
    #[arg(long, default_value = ".ceilidh/runner")]
    data_dir: PathBuf,
    /// Harnesses this runner offers (repeatable)
    #[arg(long = "harness", value_enum, default_values_t = vec![HarnessArg::ClaudeCode])]
    harnesses: Vec<HarnessArg>,
    #[command(flatten)]
    harness_args: HarnessArgs,
}

#[derive(clap::Args)]
struct UpArgs {
    #[command(flatten)]
    serve: ServeArgs,
    /// Where the local runner keeps session workspaces
    #[arg(long, default_value = ".ceilidh/runner")]
    data_dir: PathBuf,
    /// Harnesses the local runner offers (repeatable)
    #[arg(long = "harness", value_enum, default_values_t = vec![HarnessArg::ClaudeCode])]
    harnesses: Vec<HarnessArg>,
    /// Offer the mock harness too (used by scripts/smoke.sh)
    #[arg(long)]
    mock: bool,
    #[command(flatten)]
    harness_args: HarnessArgs,
}

#[derive(Clone, Copy, ValueEnum)]
enum HarnessArg {
    ClaudeCode,
    Codex,
    Cursor,
    Mock,
}

impl From<HarnessArg> for ceilidh_protocol::Harness {
    fn from(h: HarnessArg) -> Self {
        match h {
            HarnessArg::ClaudeCode => ceilidh_protocol::Harness::ClaudeCode,
            HarnessArg::Codex => ceilidh_protocol::Harness::Codex,
            HarnessArg::Cursor => ceilidh_protocol::Harness::Cursor,
            HarnessArg::Mock => ceilidh_protocol::Harness::Mock,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr everywhere: `ceilidh mcp` owns stdout for JSON-RPC.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().cmd {
        Cmd::Serve(a) => {
            guard_exposed_bind(a.bind, a.token.as_ref())?;
            let web_dir = resolve_web_dir(a.web_dir);
            ceilidh_server::serve_with_web_dir(
                ceilidh_server::ServeOptions {
                    bind: a.bind,
                    db_path: a.db,
                    token: a.token,
                    default_repo_url: a.default_repo,
                },
                web_dir,
            )
            .await
        }
        Cmd::Runner(a) => {
            let harness = a.harness_args.clone().into_config(&a.server, a.token.as_ref())?;
            ceilidh_runner::run(ceilidh_runner::RunnerOptions {
                server_url: a.server,
                token: a.token,
                runner_id: a.runner_id,
                data_dir: a.data_dir,
                harnesses: a.harnesses.into_iter().map(Into::into).collect(),
                max_turns: a.harness_args.max_turns,
                harness,
            })
            .await
        }
        Cmd::Mcp(a) => {
            ceilidh_mcp::run(ceilidh_mcp::McpOptions {
                server_url: a.server,
                token: a.token,
                parent: a.parent,
            })
            .await
        }
        Cmd::Up(a) => {
            guard_exposed_bind(a.serve.bind, a.serve.token.as_ref())?;
            let web_dir = resolve_web_dir(a.serve.web_dir.clone());
            let serve_opts = ceilidh_server::ServeOptions {
                bind: a.serve.bind,
                db_path: a.serve.db,
                token: a.serve.token.clone(),
                default_repo_url: a.serve.default_repo.clone(),
            };

            let mut harnesses: Vec<ceilidh_protocol::Harness> =
                a.harnesses.into_iter().map(Into::into).collect();
            if a.mock && !harnesses.contains(&ceilidh_protocol::Harness::Mock) {
                harnesses.push(ceilidh_protocol::Harness::Mock);
            }

            let host = if a.serve.bind.ip().is_unspecified() {
                "127.0.0.1".to_string()
            } else {
                a.serve.bind.ip().to_string()
            };
            let server_url = format!("http://{}:{}", host, a.serve.bind.port());
            let harness = a
                .harness_args
                .clone()
                .into_config(&server_url, a.serve.token.as_ref())?;
            let runner_opts = ceilidh_runner::RunnerOptions {
                server_url,
                token: a.serve.token,
                runner_id: None,
                data_dir: a.data_dir,
                harnesses,
                max_turns: a.harness_args.max_turns,
                harness,
            };

            let server = tokio::spawn(ceilidh_server::serve_with_web_dir(serve_opts, web_dir));
            let runner = tokio::spawn(async move {
                // The runner's claim loop retries with backoff, so it only
                // needs a beat for the listener to bind.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                ceilidh_runner::run(runner_opts).await
            });

            tokio::select! {
                r = server => r?,
                r = runner => r?,
            }
        }
    }
}

/// Binding beyond loopback with no token would publish session creation, the
/// runner protocol, and every transcript to the whole network.
fn guard_exposed_bind(bind: SocketAddr, token: Option<&String>) -> Result<()> {
    if bind.ip().is_loopback() || token.is_some_and(|t| !t.is_empty()) {
        return Ok(());
    }

    anyhow::bail!(
        "refusing to bind {bind} without a token: set CEILIDH_TOKEN (or --token), or bind to 127.0.0.1"
    )
}

/// The explicit flag wins; otherwise web/dist is picked up when it exists.
fn resolve_web_dir(flag: Option<PathBuf>) -> Option<PathBuf> {
    flag.or_else(|| {
        let default = PathBuf::from("web/dist");
        default.is_dir().then_some(default)
    })
}
