use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    /// Environment variables a harness child inherits anyway. ANTHROPIC_API_KEY
    /// and OPENAI_API_KEY are stripped by default, so a key in your shell
    /// cannot silently move a subscription lane (Claude Max, ChatGPT) onto
    /// metered billing; name one here to pass it through. CURSOR_API_KEY is
    /// never stripped. Repeatable
    /// (env: CEILIDH_PASS_ENV, comma separated)
    #[arg(
        long = "pass-env",
        value_name = "NAME",
        env = "CEILIDH_PASS_ENV",
        value_delimiter = ','
    )]
    pass_env: Vec<String>,
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
            pass_env: self.pass_env,
        })
    }
}

#[derive(clap::Args)]
struct ServeArgs {
    /// Address to bind
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    /// SQLite database file. Defaults to <ceilidh home>/ceilidh.db, where the
    /// home is $CEILIDH_HOME or ~/.ceilidh (env: CEILIDH_DB)
    #[arg(long, env = "CEILIDH_DB")]
    db: Option<PathBuf>,
    /// Bearer token required on every request. With neither this flag nor the
    /// env set, ceilidh reuses <ceilidh home>/token, or mints one there and
    /// prints it (env: CEILIDH_TOKEN)
    #[arg(long, env = "CEILIDH_TOKEN")]
    token: Option<String>,
    /// Directory of built web assets; defaults to web/dist when present
    #[arg(long)]
    web_dir: Option<PathBuf>,
    /// Repository prefilled into the new-session form (env: CEILIDH_DEFAULT_REPO)
    #[arg(long, env = "CEILIDH_DEFAULT_REPO")]
    default_repo: Option<String>,
    /// Read-only GitHub token, so the new-session form can offer a repository
    /// picker instead of a free-text field (env: CEILIDH_GITHUB_TOKEN)
    #[arg(long, env = "CEILIDH_GITHUB_TOKEN")]
    github_token: Option<String>,
    /// Google OAuth client id; with the secret, the public URL and an
    /// allowlist, the browser signs in with Google instead of the token
    #[arg(long, env = "CEILIDH_GOOGLE_CLIENT_ID")]
    google_client_id: Option<String>,
    #[arg(long, env = "CEILIDH_GOOGLE_CLIENT_SECRET", hide_env_values = true)]
    google_client_secret: Option<String>,
    /// Where browsers reach this caller, e.g. https://ceilidh.example
    #[arg(long, env = "CEILIDH_PUBLIC_URL")]
    public_url: Option<String>,
    /// Comma-separated emails allowed to sign in
    #[arg(long, env = "CEILIDH_ALLOWED_EMAILS")]
    allowed_emails: Option<String>,
    /// Key for the login cookies; defaults to the bearer token
    #[arg(long, env = "CEILIDH_COOKIE_SECRET", hide_env_values = true)]
    cookie_secret: Option<String>,
}

impl ServeArgs {
    fn google(&self) -> Option<ceilidh_server::GoogleAuth> {
        ceilidh_server::GoogleAuth::from_env_values(
            self.google_client_id.clone(),
            self.google_client_secret.clone(),
            self.public_url.clone(),
            self.allowed_emails.clone(),
        )
    }
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
    /// Where session workspaces live. Defaults to <ceilidh home>/runner, where
    /// the home is $CEILIDH_HOME or ~/.ceilidh (env: CEILIDH_DATA_DIR)
    #[arg(long, env = "CEILIDH_DATA_DIR")]
    data_dir: Option<PathBuf>,
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
    /// Where the local runner keeps session workspaces. Defaults to
    /// <ceilidh home>/runner, where the home is $CEILIDH_HOME or ~/.ceilidh
    /// (env: CEILIDH_DATA_DIR)
    #[arg(long, env = "CEILIDH_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Harnesses the local runner offers (repeatable). Defaults to whichever
    /// of the configured harness binaries are installed on this machine
    #[arg(long = "harness", value_enum)]
    harnesses: Vec<HarnessArg>,
    /// Offer the mock harness too (used by scripts/smoke.sh)
    #[arg(long)]
    mock: bool,
    /// Do not open the workbench in a browser
    #[arg(long)]
    no_open: bool,
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
            // The guard sees only what the operator supplied: a token minted
            // into the home directory is not a decision to publish this box.
            guard_exposed_bind(a.bind, a.token.as_ref())?;
            let mut home = LocalHome::default();
            let google = a.google();
            let token = resolve_token(a.token, &mut home)?;
            let db_path = match a.db {
                Some(db) => db,
                None => home.path()?.join("ceilidh.db"),
            };
            let cookie_secret = a.cookie_secret;
            let web_dir = resolve_web_dir(a.web_dir);
            announce(a.bind, &token);
            ceilidh_server::serve_with_web_dir(
                ceilidh_server::ServeOptions {
                    bind: a.bind,
                    db_path,
                    google,
                    cookie_secret,
                    token: Some(token.value),
                    default_repo_url: a.default_repo,
                    github_token: a.github_token,
                },
                web_dir,
            )
            .await
        }
        Cmd::Runner(a) => {
            let data_dir = match a.data_dir {
                Some(dir) => dir,
                None => LocalHome::default().path()?.join("runner"),
            };
            std::fs::create_dir_all(&data_dir)
                .with_context(|| format!("create {}", data_dir.display()))?;
            let harness = a.harness_args.clone().into_config(&a.server, a.token.as_ref())?;
            ceilidh_runner::run(ceilidh_runner::RunnerOptions {
                server_url: a.server,
                token: a.token,
                runner_id: a.runner_id,
                data_dir,
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
            let bind = a.serve.bind;
            // The guard sees only what the operator supplied: a token minted
            // into the home directory is not a decision to publish this box.
            guard_exposed_bind(bind, a.serve.token.as_ref())?;
            let mut home = LocalHome::default();
            let google = a.serve.google();
            let token = resolve_token(a.serve.token, &mut home)?;
            let db_path = match a.serve.db {
                Some(db) => db,
                None => home.path()?.join("ceilidh.db"),
            };
            let data_dir = match a.data_dir {
                Some(dir) => dir,
                None => home.path()?.join("runner"),
            };
            std::fs::create_dir_all(&data_dir)
                .with_context(|| format!("create {}", data_dir.display()))?;
            let web_dir = resolve_web_dir(a.serve.web_dir);
            let serve_opts = ceilidh_server::ServeOptions {
                bind,
                db_path,
                token: Some(token.value.clone()),
                default_repo_url: a.serve.default_repo.clone(),
                github_token: a.serve.github_token.clone(),
                google,
                cookie_secret: a.serve.cookie_secret.clone(),
            };

            // No --harness: offer the harnesses this machine actually has.
            let mut harnesses: Vec<ceilidh_protocol::Harness> = if a.harnesses.is_empty() {
                let installed = installed_harnesses(&a.harness_args);
                if installed.is_empty() {
                    if !a.mock {
                        print_no_harness_warning();
                    }
                    vec![ceilidh_protocol::Harness::Mock]
                } else {
                    installed
                }
            } else {
                a.harnesses.into_iter().map(Into::into).collect()
            };
            if a.mock && !harnesses.contains(&ceilidh_protocol::Harness::Mock) {
                harnesses.push(ceilidh_protocol::Harness::Mock);
            }
            tracing::info!(
                "harnesses offered: {}",
                harnesses
                    .iter()
                    .map(|harness| harness_label(*harness))
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            let server_url = local_url(bind);
            let harness = a
                .harness_args
                .clone()
                .into_config(&server_url, Some(&token.value))?;
            let runner_opts = ceilidh_runner::RunnerOptions {
                server_url,
                token: Some(token.value.clone()),
                runner_id: None,
                data_dir,
                harnesses,
                max_turns: a.harness_args.max_turns,
                harness,
            };

            let sign_in = announce(bind, &token);
            // A local `up` is a desktop app in all but name. A script (no TTY)
            // and an operator who said --no-open get neither browser nor blame.
            let open_when_ready =
                !a.no_open && std::io::stdout().is_terminal();

            let server = tokio::spawn(ceilidh_server::serve_with_web_dir(serve_opts, web_dir));
            let runner = tokio::spawn(async move {
                // The runner's claim loop retries with backoff, so it only
                // needs a beat for the listener to bind.
                tokio::time::sleep(Duration::from_millis(500)).await;
                ceilidh_runner::run(runner_opts).await
            });
            if open_when_ready {
                tokio::spawn(async move {
                    if wait_for_listener(bind, Duration::from_secs(15)).await {
                        open_browser(&sign_in);
                    }
                });
            }

            tokio::select! {
                r = server => r?,
                r = runner => r?,
            }
        }
    }
}

/// Binding beyond loopback with no token would publish session creation, the
/// runner protocol, and every transcript to the whole network. The token
/// ceilidh mints for itself does not count here: it is saved on this machine
/// and never typed, which is right for 127.0.0.1 and wrong for an address the
/// network can reach, so an exposed bind still takes a token the operator
/// chose to hand over.
fn guard_exposed_bind(bind: SocketAddr, token: Option<&String>) -> Result<()> {
    if bind.ip().is_loopback() || token.is_some_and(|t| !t.trim().is_empty()) {
        return Ok(());
    }

    anyhow::bail!(
        "refusing to bind {bind} without a token: a token ceilidh mints for itself \
         guards a loopback bind, not a network one. Set CEILIDH_TOKEN (or --token) \
         explicitly, or bind to 127.0.0.1"
    )
}

/// `$CEILIDH_HOME`, else `~/.ceilidh`. It holds the database, the runner's
/// session workspaces, and the bearer token.
fn ceilidh_home() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("CEILIDH_HOME").filter(|home| !home.is_empty()) {
        return Ok(PathBuf::from(home));
    }
    let home = std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .context("neither CEILIDH_HOME nor HOME is set")?;
    Ok(PathBuf::from(home).join(".ceilidh"))
}

/// The home, resolved and created the first time something actually defaults
/// into it. A caller that is given its database and its token (the container
/// image is) never touches a home directory, and so never needs a HOME.
#[derive(Default)]
struct LocalHome(Option<PathBuf>);

impl LocalHome {
    fn path(&mut self) -> Result<PathBuf> {
        if let Some(home) = &self.0 {
            return Ok(home.clone());
        }
        let home = ceilidh_home()?;
        ensure_home(&home)?;
        self.0 = Some(home.clone());
        Ok(home)
    }
}

/// The home holds a bearer token and every session's working copy, so it
/// belongs to this user alone.
fn ensure_home(home: &Path) -> Result<()> {
    std::fs::create_dir_all(home).with_context(|| format!("create {}", home.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(home, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("restrict {} to this user", home.display()))?;
    }
    Ok(())
}

/// Where the caller's token came from, which is what the startup lines report.
enum TokenSource {
    Supplied,
    Saved(PathBuf),
    Minted(PathBuf),
}

struct ResolvedToken {
    value: String,
    source: TokenSource,
}

/// A local caller should not make the operator invent a credential. The flag
/// or env wins; otherwise the one saved in the home directory is reused, and
/// otherwise a fresh one is minted and saved.
fn resolve_token(supplied: Option<String>, home: &mut LocalHome) -> Result<ResolvedToken> {
    if let Some(value) = supplied.filter(|token| !token.trim().is_empty()) {
        return Ok(ResolvedToken {
            value,
            source: TokenSource::Supplied,
        });
    }

    let path = home.path()?.join("token");
    let saved = std::fs::read_to_string(&path)
        .map(|saved| saved.trim().to_string())
        .unwrap_or_default();
    if !saved.is_empty() {
        return Ok(ResolvedToken {
            value: saved,
            source: TokenSource::Saved(path),
        });
    }

    let value = mint_token();
    write_secret(&path, &value)?;
    Ok(ResolvedToken {
        value,
        source: TokenSource::Minted(path),
    })
}

/// 32 hex characters: 128 bits of randomness, and nothing in it needs escaping
/// in the URL that carries it to the browser.
fn mint_token() -> String {
    let mut bytes = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// 0600 from the moment the file exists: it is a bearer credential.
fn write_secret(path: &Path, value: &str) -> Result<()> {
    let contents = format!("{value}\n");
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("write {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("write {}", path.display()))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restrict {}", path.display()))?;
    }
    #[cfg(not(unix))]
    std::fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

/// The URL a browser on this machine uses. A wildcard bind is reachable on
/// loopback too, and loopback is what the operator is sitting at.
fn local_url(bind: SocketAddr) -> String {
    let host = if bind.ip().is_unspecified() {
        "127.0.0.1".to_string()
    } else if bind.is_ipv6() {
        format!("[{}]", bind.ip())
    } else {
        bind.ip().to_string()
    };
    format!("http://{host}:{}", bind.port())
}

/// Says on stdout where the caller is, what the token is, and where it lives.
/// Returns the sign-in URL, which carries the token so the first load of the
/// browser needs nothing pasted.
fn announce(bind: SocketAddr, token: &ResolvedToken) -> String {
    let url = local_url(bind);
    let sign_in = format!("{url}/?token={}", token.value);
    let where_from = match &token.source {
        TokenSource::Supplied => " (from --token or CEILIDH_TOKEN)".to_string(),
        TokenSource::Saved(path) => format!(" (saved in {})", path.display()),
        TokenSource::Minted(path) => format!(" (new, saved in {})", path.display()),
    };
    println!("ceilidh caller on {url}");
    println!("token {}{where_from}", token.value);
    println!("open {sign_in}");
    sign_in
}

/// Which of the configured harness binaries this machine actually has: an
/// absolute path that exists, or a name that resolves on PATH.
fn installed_harnesses(args: &HarnessArgs) -> Vec<ceilidh_protocol::Harness> {
    [
        (ceilidh_protocol::Harness::ClaudeCode, &args.claude_bin),
        (ceilidh_protocol::Harness::Codex, &args.codex_bin),
        (ceilidh_protocol::Harness::Cursor, &args.cursor_bin),
    ]
    .into_iter()
    .filter(|(_, bin)| binary_resolves(bin))
    .map(|(harness, _)| harness)
    .collect()
}

fn binary_resolves(bin: &str) -> bool {
    let path = Path::new(bin);
    if path.is_absolute() || bin.contains('/') {
        return is_executable(path);
    }
    let Some(search) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&search).any(|dir| is_executable(&dir.join(bin)))
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    path.is_file()
}

fn harness_label(harness: ceilidh_protocol::Harness) -> &'static str {
    match harness {
        ceilidh_protocol::Harness::ClaudeCode => "claude-code",
        ceilidh_protocol::Harness::Codex => "codex",
        ceilidh_protocol::Harness::Cursor => "cursor",
        ceilidh_protocol::Harness::Mock => "mock",
    }
}

/// The mock harness echoes; it is not a coding agent. Say so, and say what to
/// install, rather than letting the first turn answer nonsense.
fn print_no_harness_warning() {
    println!(
        "warning: no harness CLI found on PATH, so only the mock harness is offered. \
         The mock echoes your message back and writes no code."
    );
    println!("  claude-code  npm i -g @anthropic-ai/claude-code, then run: claude");
    println!("  codex        npm i -g @openai/codex, then run: codex");
    println!("  cursor       curl https://cursor.com/install -fsS | bash, then: cursor-agent login");
    println!(
        "Each one signs in on its own; ceilidh runs them as you. Point ceilidh at a binary \
         elsewhere with --claude-bin, --codex-bin or --cursor-bin."
    );
}

/// Waits for the caller's listener so the browser does not open on a refused
/// connection. Returns false if it never came up in time.
async fn wait_for_listener(bind: SocketAddr, within: Duration) -> bool {
    let target = if bind.ip().is_unspecified() {
        SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), bind.port())
    } else {
        bind
    };
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(target).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Best effort: a headless box or a missing opener is not a reason to fail.
fn open_browser(url: &str) {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    let _ = std::process::Command::new(opener)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// The explicit flag wins; otherwise web/dist is picked up when it exists.
fn resolve_web_dir(flag: Option<PathBuf>) -> Option<PathBuf> {
    flag.or_else(|| {
        let default = PathBuf::from("web/dist");
        default.is_dir().then_some(default)
    })
}
