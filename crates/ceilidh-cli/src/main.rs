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
            ceilidh_server::serve(ceilidh_server::ServeOptions {
                bind: a.bind,
                db_path: a.db,
                token: a.token,
            })
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
            // Integration lands in wave 2: serve + a local runner in one
            // process, with the runner joining once the server is listening.
            let _ = a;
            anyhow::bail!("ceilidh up is not wired yet (wave 2 integration)")
        }
    }
}
