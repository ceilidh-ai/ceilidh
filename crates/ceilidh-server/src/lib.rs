//! ceilidh-server: the caller's HTTP surface.
//!
//! Owned by lane/caller-server. The stub below fixes the entry-point
//! signature the CLI links against; fill in the implementation without
//! changing it.

use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub bind: SocketAddr,
    /// SQLite database file; created if absent.
    pub db_path: PathBuf,
    /// Bearer token required on every API request when set.
    pub token: Option<String>,
}

pub async fn serve(_opts: ServeOptions) -> anyhow::Result<()> {
    anyhow::bail!("ceilidh-server is not implemented yet (lane/caller-server)")
}
