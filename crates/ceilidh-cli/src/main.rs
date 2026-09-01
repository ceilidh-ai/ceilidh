use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
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
}

#[derive(Clone, Copy, ValueEnum)]
enum HarnessArg {
    ClaudeCode,
    Codex,
    Mock,
}

impl From<HarnessArg> for ceilidh_protocol::Harness {
    fn from(h: HarnessArg) -> Self {
        match h {
            HarnessArg::ClaudeCode => ceilidh_protocol::Harness::ClaudeCode,
            HarnessArg::Codex => ceilidh_protocol::Harness::Codex,
            HarnessArg::Mock => ceilidh_protocol::Harness::Mock,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().cmd {
        Cmd::Serve(a) => {
            let web_dir = resolve_web_dir(a.web_dir);
            ceilidh_server::serve_with_web_dir(
                ceilidh_server::ServeOptions {
                    bind: a.bind,
                    db_path: a.db,
                    token: a.token,
                },
                web_dir,
            )
            .await
        }
        Cmd::Runner(a) => {
            ceilidh_runner::run(ceilidh_runner::RunnerOptions {
                server_url: a.server,
                token: a.token,
                runner_id: a.runner_id,
                data_dir: a.data_dir,
                harnesses: a.harnesses.into_iter().map(Into::into).collect(),
            })
            .await
        }
        Cmd::Up(a) => {
            let web_dir = resolve_web_dir(a.serve.web_dir.clone());
            let serve_opts = ceilidh_server::ServeOptions {
                bind: a.serve.bind,
                db_path: a.serve.db,
                token: a.serve.token.clone(),
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
            let runner_opts = ceilidh_runner::RunnerOptions {
                server_url: format!("http://{}:{}", host, a.serve.bind.port()),
                token: a.serve.token,
                runner_id: None,
                data_dir: a.data_dir,
                harnesses,
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

/// The explicit flag wins; otherwise web/dist is picked up when it exists.
fn resolve_web_dir(flag: Option<PathBuf>) -> Option<PathBuf> {
    flag.or_else(|| {
        let default = PathBuf::from("web/dist");
        default.is_dir().then_some(default)
    })
}
