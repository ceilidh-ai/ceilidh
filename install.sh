#!/usr/bin/env bash
set -euo pipefail

REPO="${CEILIDH_REPO:-ceilidh-ai/ceilidh}"
API_URL="${GITHUB_API_URL:-https://api.github.com}"

fail() {
  echo "error: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

build_from_source() {
  echo "No release asset is available for this system yet."
  echo "Build from source with:"
  echo "  cargo install --path crates/ceilidh-cli"
}

# Verifies the tarball against the release SHA256SUMS. A release without that
# asset, or a system with no sha256 tool, installs exactly as it did before.
verify_checksum() {
  archive_path="$1"
  name="$2"
  sums_url="$3"

  [ -n "$sums_url" ] || return 0

  if command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$archive_path" | awk '{ print $1 }')"
  elif command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$archive_path" | awk '{ print $1 }')"
  else
    return 0
  fi

  sums="$(curl -fsSL "$sums_url" 2>/dev/null || true)"
  expected="$(printf '%s\n' "$sums" | awk -v name="$name" '$2 == name || $2 == "*" name { print $1; exit }')"
  [ -n "$expected" ] || return 0

  if [ "$expected" != "$actual" ]; then
    fail "checksum mismatch for $name: expected $expected, got $actual"
  fi
  echo "Verified $name against SHA256SUMS."
}

detect_target() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Darwin)
      os_part="apple-darwin"
      ;;
    Linux)
      os_part="unknown-linux-gnu"
      ;;
    *)
      fail "unsupported OS: $os"
      ;;
  esac

  case "$arch" in
    arm64|aarch64)
      arch_part="aarch64"
      ;;
    x86_64|amd64)
      arch_part="x86_64"
      ;;
    *)
      fail "unsupported architecture: $arch"
      ;;
  esac

  printf '%s-%s\n' "$arch_part" "$os_part"
}

install_dir() {
  if [[ -n "${INSTALL_DIR:-}" ]]; then
    printf '%s\n' "$INSTALL_DIR"
  elif [[ -d /usr/local/bin && -w /usr/local/bin ]]; then
    echo "Installing to /usr/local/bin because it is writable." >&2
    printf '%s\n' "/usr/local/bin"
  else
    printf '%s\n' "$HOME/.local/bin"
  fi
}

extract_field() {
  field="$1"
  awk -F'"' -v field="$field" '$0 ~ "\"" field "\":" { print $4; exit }'
}

find_asset_url() {
  asset_name="$1"
  awk -v asset_name="$asset_name" '
    /"browser_download_url":/ && index($0, asset_name) {
      sub(/^[[:space:]]*"browser_download_url":[[:space:]]*"/, "")
      sub(/",?[[:space:]]*$/, "")
      print
      exit
    }
  '
}

need curl
need tar
need uname
need mktemp

target="$(detect_target)"
release_json="$(curl -fsSL "$API_URL/repos/$REPO/releases/latest" 2>/dev/null || true)"
if [[ -z "$release_json" ]]; then
  build_from_source
  exit 0
fi

tag_name="$(printf '%s\n' "$release_json" | extract_field tag_name)"
version="${tag_name#v}"
asset_name="ceilidh-${version}-${target}.tar.gz"
asset_url="$(printf '%s\n' "$release_json" | find_asset_url "$asset_name")"
sums_url="$(printf '%s\n' "$release_json" | find_asset_url "SHA256SUMS")"

if [[ -z "$tag_name" || -z "$asset_url" ]]; then
  build_from_source
  exit 0
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

archive="$tmp_dir/$asset_name"
extract_dir="$tmp_dir/extract"
mkdir -p "$extract_dir"

curl -fsSL "$asset_url" -o "$archive"
verify_checksum "$archive" "$asset_name" "$sums_url"
tar -xzf "$archive" -C "$extract_dir"

binary="$(find "$extract_dir" -type f -name ceilidh -print -quit)"
if [[ -z "$binary" ]]; then
  fail "release asset did not contain a ceilidh binary"
fi

dir="$(install_dir)"
mkdir -p "$dir"
if [[ ! -w "$dir" ]]; then
  fail "$dir is not writable. Set INSTALL_DIR to a writable directory."
fi

install -m 0755 "$binary" "$dir/ceilidh"
echo "ceilidh $tag_name installed to $dir/ceilidh"
