//! ceilidh-runner: a band runner.
//!
//! Owned by lane/runner. The stub below fixes the entry-point signature the
//! CLI links against; fill in the implementation without changing it.

use std::path::PathBuf;

use ceilidh_protocol::Harness;

#[derive(Debug, Clone)]
pub struct RunnerOptions {
    /// Base URL of the caller, e.g. http://127.0.0.1:8080
    pub server_url: String,
    pub token: Option<String>,
    /// Stable identity; defaults to `<host>:<user>` when None.
    pub runner_id: Option<String>,
    /// Where session workspaces live.
    pub data_dir: PathBuf,
    /// Which harnesses this runner offers.
    pub harnesses: Vec<Harness>,
}

pub async fn run(_opts: RunnerOptions) -> anyhow::Result<()> {
    anyhow::bail!("ceilidh-runner is not implemented yet (lane/runner)")
}
