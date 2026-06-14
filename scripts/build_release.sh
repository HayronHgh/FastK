#!/usr/bin/env bash
set -euo pipefail

TARGET=""

usage() {
  cat >&2 <<'EOF'
usage: scripts/build_release.sh [--target <triple>]

Builds release examples and packages fastk_bridge/fastk_admin into dist/.
For Linux static-style builds, pass --target x86_64-unknown-linux-musl after
installing that Rust target. macOS builds are portable, not fully static.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target)
      if [ "$#" -lt 2 ]; then
        echo "missing value for --target" >&2
        exit 2
      fi
      TARGET="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)"
if [ -z "$VERSION" ]; then
  echo "unable to read package version from Cargo.toml" >&2
  exit 1
fi

HOST_TARGET="$(rustc -vV | awk '/^host:/{print $2}')"
if [ -z "$HOST_TARGET" ]; then
  echo "unable to determine rustc host target" >&2
  exit 1
fi

EFFECTIVE_TARGET="${TARGET:-$HOST_TARGET}"

if [ "$TARGET" = "x86_64-unknown-linux-musl" ]; then
  if ! command -v rustup >/dev/null 2>&1; then
    echo "musl target requested, but rustup is unavailable; install x86_64-unknown-linux-musl first" >&2
    exit 2
  fi
  if ! rustup target list --installed | grep -qx "$TARGET"; then
    echo "musl target requested, but $TARGET is not installed" >&2
    echo "run: rustup target add $TARGET" >&2
    exit 2
  fi
fi

if [ "$(uname -s)" = "Darwin" ] && [ -z "$TARGET" ]; then
  echo "macOS release package is portable; this script does not claim a fully static binary." >&2
fi

BUILD_ARGS=(build --release --examples)
if [ -n "$TARGET" ]; then
  BUILD_ARGS+=(--target "$TARGET")
fi
cargo "${BUILD_ARGS[@]}"
cargo package --allow-dirty --no-verify

EXE_EXT=""
case "$EFFECTIVE_TARGET" in
  *windows*) EXE_EXT=".exe" ;;
esac

if [ -n "$TARGET" ]; then
  RELEASE_DIR="$REPO_ROOT/target/$TARGET/release/examples"
else
  RELEASE_DIR="$REPO_ROOT/target/release/examples"
fi

DIST_ROOT="$REPO_ROOT/dist"
PACKAGE_DIR="$DIST_ROOT/fastk-$VERSION-$EFFECTIVE_TARGET"
rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR/bin" "$PACKAGE_DIR/crate" "$PACKAGE_DIR/docs" "$PACKAGE_DIR/schemas"

BRIDGE_SOURCE="$RELEASE_DIR/fastk_bridge$EXE_EXT"
ADMIN_SOURCE="$RELEASE_DIR/fastk_admin$EXE_EXT"
if [ ! -f "$BRIDGE_SOURCE" ]; then
  echo "missing built fastk_bridge binary: $BRIDGE_SOURCE" >&2
  exit 1
fi
if [ ! -f "$ADMIN_SOURCE" ]; then
  echo "missing built fastk_admin binary: $ADMIN_SOURCE" >&2
  exit 1
fi

cp "$BRIDGE_SOURCE" "$PACKAGE_DIR/bin/fastk_bridge$EXE_EXT"
cp "$ADMIN_SOURCE" "$PACKAGE_DIR/bin/fastk_admin$EXE_EXT"

CRATE_SOURCE="$REPO_ROOT/target/package/fastk-$VERSION.crate"
if [ ! -f "$CRATE_SOURCE" ]; then
  echo "missing packaged crate: $CRATE_SOURCE" >&2
  exit 1
fi
cp "$CRATE_SOURCE" "$PACKAGE_DIR/crate/fastk-$VERSION.crate"

DOCS=(
  README.md
  docs/ARCHITECTURE_BOUNDARY.md
  docs/STORE_LIFECYCLE.md
  docs/BACKTEST_INTEGRATION.md
  docs/REPLAY_AND_TAIL.md
  docs/RELEASE_CHECKLIST.md
  docs/RELEASE_NOTES.md
  docs/BACKEND_INTEGRATION.md
  docs/BRIDGE_CONTRACT.md
  docs/KLINE_STORAGE_COMPARISON.md
  docs/PROJECT_STRUCTURE.md
  docs/SIGNAL_SCALAR_STORAGE.md
)
for doc in "${DOCS[@]}"; do
  if [ ! -f "$doc" ]; then
    echo "missing release doc: $doc" >&2
    exit 1
  fi
  cp "$doc" "$PACKAGE_DIR/docs/$(basename "$doc")"
done

cp schemas/*.json "$PACKAGE_DIR/schemas/"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "sha256sum or shasum is required" >&2
    exit 1
  fi
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

GIT_COMMIT="unknown"
if git rev-parse --short=12 HEAD >/dev/null 2>&1; then
  GIT_COMMIT="$(git rev-parse --short=12 HEAD)"
fi

RUSTC_VERSION="$(rustc -vV)"
CARGO_VERSION="$(cargo --version)"
BUILD_TIME_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
BRIDGE_SHA="$(sha256_file "$PACKAGE_DIR/bin/fastk_bridge$EXE_EXT")"
ADMIN_SHA="$(sha256_file "$PACKAGE_DIR/bin/fastk_admin$EXE_EXT")"
CRATE_SHA="$(sha256_file "$PACKAGE_DIR/crate/fastk-$VERSION.crate")"

cat > "$PACKAGE_DIR/release_manifest.json" <<EOF
{
  "name": "fastk",
  "version": "$(json_escape "$VERSION")",
  "target": "$(json_escape "$EFFECTIVE_TARGET")",
  "git_commit": "$(json_escape "$GIT_COMMIT")",
  "rustc": "$(json_escape "$RUSTC_VERSION")",
  "cargo": "$(json_escape "$CARGO_VERSION")",
  "build_time_utc": "$(json_escape "$BUILD_TIME_UTC")",
  "artifacts": [
    {
      "path": "bin/fastk_bridge$EXE_EXT",
      "kind": "binary",
      "sha256": "$BRIDGE_SHA"
    },
    {
      "path": "bin/fastk_admin$EXE_EXT",
      "kind": "binary",
      "sha256": "$ADMIN_SHA"
    },
    {
      "path": "crate/fastk-$VERSION.crate",
      "kind": "crate",
      "sha256": "$CRATE_SHA"
    }
  ],
  "stable_surfaces": [
    "FastKStore",
    "BacktestStoreView",
    "KlineRecord",
    "ScalarRecord",
    "DatasetRegistry",
    "DatasetRef",
    "fastk_bridge JSON contract"
  ],
  "experimental_surfaces": [
    "TradeRecord",
    "BboRecord",
    "BookDeltaRecord",
    "ReplayCursor",
    "SequenceScanReport",
    "day/hour partition internals"
  ]
}
EOF

(
  cd "$PACKAGE_DIR"
  find . -type f ! -name SHA256SUMS -print | sort | while IFS= read -r file; do
    rel="${file#./}"
    printf '%s  %s\n' "$(sha256_file "$file")" "$rel"
  done
) > "$PACKAGE_DIR/SHA256SUMS"

echo "release package: $PACKAGE_DIR"
echo "manifest: $PACKAGE_DIR/release_manifest.json"
echo "checksums: $PACKAGE_DIR/SHA256SUMS"
