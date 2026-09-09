//! Stages the built web client where `rust-embed` can bake it into the binary.
//!
//! `web/dist` is a build artifact, so a fresh clone with no node toolchain has
//! none. That must still compile: when the client is missing we stage a short
//! placeholder page instead, and the binary says so at `/`.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const PLACEHOLDER: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ceilidh</title>
  </head>
  <body>
    <main>
      <h1>ceilidh caller is running</h1>
      <p>The web assets were not built into this binary. Two fixes:</p>
      <ul>
        <li>Build the client (<code>cd web &amp;&amp; npm install &amp;&amp; npm run build</code>), then rebuild ceilidh.</li>
        <li>Or start the caller with <code>--web-dir /path/to/web/dist</code> to serve the client from disk.</li>
      </ul>
    </main>
  </body>
</html>
"#;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let dist = Path::new(&manifest_dir)
        .join("..")
        .join("..")
        .join("web")
        .join("dist");
    let staged = Path::new(&out_dir).join("web");

    // With no dist yet this names a path that does not exist, so cargo reruns
    // this script on every build and the first build after `npm run build`
    // picks the client up. Cargo compares timestamps, so a dist restored with
    // older ones (moving an earlier build back into place) wants a
    // `touch web/dist` to be seen.
    println!("cargo:rerun-if-changed={}", dist.display());

    fs::create_dir_all(&staged).expect("create the staged web client");
    let mut wanted = HashSet::new();
    if dist.join("index.html").is_file() {
        stage_dir(&dist, &staged, Path::new(""), &mut wanted);
    } else {
        stage_file(&staged.join("index.html"), PLACEHOLDER.as_bytes());
        wanted.insert(PathBuf::from("index.html"));
    }
    prune(&staged, Path::new(""), &wanted);
}

fn stage_dir(from: &Path, to: &Path, rel: &Path, wanted: &mut HashSet<PathBuf>) {
    for entry in fs::read_dir(from).expect("read the built web client") {
        let entry = entry.expect("read a web client entry");
        let name = entry.file_name();
        let entry_rel = rel.join(&name);
        let target = to.join(&name);
        if entry.file_type().expect("stat a web client entry").is_dir() {
            fs::create_dir_all(&target).expect("create a web client directory");
            stage_dir(&entry.path(), &target, &entry_rel, wanted);
        } else {
            let bytes = fs::read(entry.path()).expect("read a web client file");
            stage_file(&target, &bytes);
            wanted.insert(entry_rel);
        }
    }
}

/// Writes only what changed. rust-embed pulls the staged files in with
/// `include_bytes!`, so rewriting an identical file would recompile the crate
/// on every build.
fn stage_file(path: &Path, bytes: &[u8]) {
    if fs::read(path).is_ok_and(|current| current == bytes) {
        return;
    }
    fs::write(path, bytes).expect("stage a web client file");
}

/// Drops files a previous build staged and this one did not, so a stale asset
/// never ships inside the binary.
fn prune(dir: &Path, rel: &Path, wanted: &HashSet<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let entry_rel = rel.join(entry.file_name());
        let path = entry.path();
        if path.is_dir() {
            prune(&path, &entry_rel, wanted);
        } else if !wanted.contains(&entry_rel) {
            let _ = fs::remove_file(path);
        }
    }
}
