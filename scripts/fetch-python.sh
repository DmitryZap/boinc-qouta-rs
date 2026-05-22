#!/usr/bin/env bash
# Fetch a self-contained python-build-standalone interpreter into vendor/python.
# The app links PyO3 against this interpreter and ships it next to the binary,
# so no system Python is required at runtime.
#
# Usage: scripts/fetch-python.sh [release-tag]
# Detects the host platform; override the PBS release tag as the first arg.
set -euo pipefail

PY_VERSION="3.12"
TAG="${1:-20260510}"
REPO="astral-sh/python-build-standalone"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  TRIPLE="aarch64-apple-darwin" ;;
  Darwin-x86_64) TRIPLE="x86_64-apple-darwin" ;;
  Linux-x86_64)  TRIPLE="x86_64-unknown-linux-gnu" ;;
  Linux-aarch64) TRIPLE="aarch64-unknown-linux-gnu" ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENDOR="$ROOT/vendor"
mkdir -p "$VENDOR"

# Resolve the full cpython-<ver>+<tag>-<triple>-install_only asset name.
ASSET="$(curl -sL "https://api.github.com/repos/$REPO/releases/tags/$TAG" \
  | grep -oE "https://[^\"]*cpython-${PY_VERSION}[^\"]*-${TRIPLE}-install_only\.tar\.gz" \
  | head -1)"
[ -n "$ASSET" ] || { echo "no install_only asset for $PY_VERSION/$TRIPLE in $TAG" >&2; exit 1; }

echo "downloading $ASSET"
curl -sL "$ASSET" -o "$VENDOR/pbs.tar.gz"
rm -rf "$VENDOR/python"
tar xzf "$VENDOR/pbs.tar.gz" -C "$VENDOR"   # extracts to vendor/python
rm -f "$VENDOR/pbs.tar.gz"

echo "bundled: $("$VENDOR/python/bin/python3" --version) at $VENDOR/python"
