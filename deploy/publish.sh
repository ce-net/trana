#!/usr/bin/env bash
# publish.sh — build, content-address, and publish the trana-node backend as a ce-appmgr (`ce app`)
# package, so it deploys to any node (or your whole fleet) with ONE `ce app install` command.
#
# What it does, end to end:
#   1. builds the trana-node release binary for the host target (and, with --with-linux, linux-amd64
#      on the relay via tools/ce-build so the public mesh node can run it);
#   2. uploads each binary to the local CE node's content-addressed blob store (POST /blobs) — durable,
#      128 MB, and self-replicating across the mesh, so any installing node can fetch it by hash;
#   3. rewrites the [native].artifacts block in deploy/trana-node.ceapp.toml with the real sha256s;
#   4. signs + publishes the manifest to the registry with `ce app publish`;
#   5. verifies each artifact is fetchable from the registry, and prints the install commands.
#
# Re-runnable and idempotent: re-publishing identical bytes is a no-op (same content address).
#
# Usage:
#   ./deploy/publish.sh [--with-linux] [--registry <origin>] [--node <url>]
#
# Env (override defaults):
#   TRANA_REGISTRY   manifest + blob origin to publish to   (default: https://ce-net.com)
#   CE_NODE_URL      local CE node HTTP API                 (default: http://127.0.0.1:8844)
#   CE_BIN           ce binary                              (default: ce on PATH, else ~/.local/bin/ce)
#   CE_BUILD_NODE    relay ssh target for --with-linux      (default: root@178.105.145.170)
set -euo pipefail

REGISTRY="${TRANA_REGISTRY:-https://ce-net.com}"
NODE_URL="${CE_NODE_URL:-http://127.0.0.1:8844}"
CE_BIN="${CE_BIN:-$(command -v ce || echo "$HOME/.local/bin/ce")}"
BUILD_NODE="${CE_BUILD_NODE:-root@178.105.145.170}"
WITH_LINUX=0

while [ $# -gt 0 ]; do
  case "$1" in
    --with-linux) WITH_LINUX=1 ;;
    --registry) REGISTRY="${2:?--registry needs a value}"; shift ;;
    --node) NODE_URL="${2:?--node needs a value}"; shift ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "publish.sh: unknown arg '$1'" >&2; exit 2 ;;
  esac
  shift
done
REGISTRY="${REGISTRY%/}"
NODE_URL="${NODE_URL%/}"

# Resolve paths: this script lives in <trana>/deploy; the workspace root is the nearest ancestor
# holding CLAUDE.md (so tools/ce-build resolves the same path-dep layout the remote build expects).
DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$DEPLOY_DIR/.." && pwd)"
MANIFEST="$DEPLOY_DIR/trana-node.ceapp.toml"
WS_ROOT="$REPO"
while [ "$WS_ROOT" != "/" ] && [ ! -f "$WS_ROOT/CLAUDE.md" ]; do WS_ROOT="$(dirname "$WS_ROOT")"; done
[ -f "$WS_ROOT/CLAUDE.md" ] || WS_ROOT="$(dirname "$REPO")"

[ -x "$CE_BIN" ] || { echo "publish.sh: ce binary not found at '$CE_BIN' (set CE_BIN or install ce)" >&2; exit 1; }
command -v curl >/dev/null || { echo "publish.sh: curl is required" >&2; exit 1; }

sha256_hex() { # $1 = file -> lowercase hex sha256
  if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# Upload a binary to the local node's blob store; verify the returned hash matches; echo the hash.
upload_blob() { # $1 = file -> prints sha256 hex on success
  local file="$1" want got resp
  want="$(sha256_hex "$file")"
  resp="$(curl -fsS -X POST --data-binary @"$file" \
            -H 'content-type: application/octet-stream' "$NODE_URL/blobs")" \
    || { echo "publish.sh: blob upload to $NODE_URL/blobs failed (is \`ce start\` running?)" >&2; return 1; }
  got="$(printf '%s' "$resp" | sed -E 's/.*"hash" *: *"([0-9a-f]+)".*/\1/')"
  if [ "$got" != "$want" ]; then
    echo "publish.sh: blob hash mismatch (local $want vs node $got) for $file" >&2; return 1
  fi
  printf '%s' "$want"
}

declare -a ART_LINES=()
add_artifact() { # $1 = target, $2 = file
  local target="$1" file="$2" hash
  [ -f "$file" ] || { echo "publish.sh: missing binary for $target: $file" >&2; return 1; }
  echo "  $target: $(du -h "$file" | awk '{print $1}')  $file"
  hash="$(upload_blob "$file")"
  ART_LINES+=("artifacts.\"$target\" = \"sha256:$hash\"")
  echo "    -> sha256:$hash (uploaded, mesh-replicated)"
}

# --- 1. build the host target (os-arch keys match ce-appmgr's host_target()) ---
case "$(uname -s)" in Darwin) OS=darwin ;; Linux) OS=linux ;; *) OS="$(uname -s | tr '[:upper:]' '[:lower:]')" ;; esac
case "$(uname -m)" in arm64|aarch64) ARCH=arm64 ;; x86_64|amd64) ARCH=amd64 ;; *) ARCH="$(uname -m)" ;; esac
HOST_TARGET="$OS-$ARCH"

echo "==> building trana-node (release) for $HOST_TARGET"
( cd "$REPO" && cargo build --release -p trana-node )
HOST_BIN="$REPO/target/release/trana-node"

echo "==> uploading artifacts to $NODE_URL"
add_artifact "$HOST_TARGET" "$HOST_BIN"

# --- optional: linux-amd64 built on the relay so the public mesh node can run it ---
if [ "$WITH_LINUX" -eq 1 ] && [ "$HOST_TARGET" != "linux-amd64" ]; then
  echo "==> building trana-node (release) for linux-amd64 on $BUILD_NODE (tools/ce-build)"
  "$WS_ROOT/tools/ce-build" "$REPO" build --release -p trana-node
  LINUX_BIN="$(mktemp)"
  # ce-build keeps a per-repo target dir on the relay at /root/ce-build/target-<basename>.
  scp -q "$BUILD_NODE:/root/ce-build/target-$(basename "$REPO")/release/trana-node" "$LINUX_BIN" \
    || { echo "publish.sh: could not fetch linux-amd64 binary from $BUILD_NODE" >&2; exit 1; }
  chmod +x "$LINUX_BIN"
  add_artifact "linux-amd64" "$LINUX_BIN"
fi

# --- 3. rewrite the managed artifacts block in the manifest ---
echo "==> writing artifacts into $(basename "$MANIFEST")"
BLOCK="$(mktemp)"; printf '%s\n' "${ART_LINES[@]}" > "$BLOCK"
TMP="$(mktemp)"
awk -v blockfile="$BLOCK" '
  /AUTO-ARTIFACTS-START/ { print; while ((getline l < blockfile) > 0) print l; skip=1; next }
  /AUTO-ARTIFACTS-END/   { skip=0; print; next }
  skip { next }
  { print }
' "$MANIFEST" > "$TMP" && mv "$TMP" "$MANIFEST"
rm -f "$BLOCK"

# --- 4. sign + publish the manifest ---
echo "==> publishing manifest to $REGISTRY"
"$CE_BIN" app publish "$MANIFEST" --registry "$REGISTRY"

# --- 5. verify each artifact is reachable from the registry origin ---
echo "==> verifying artifacts resolve from $REGISTRY/blobs"
ok=1
for line in "${ART_LINES[@]}"; do
  hash="${line##*sha256:}"; hash="${hash%\"}"
  if curl -fsI "$REGISTRY/blobs/$hash" >/dev/null 2>&1 || curl -fso /dev/null "$REGISTRY/blobs/$hash"; then
    echo "  ok: $REGISTRY/blobs/$hash"
  else
    ok=0
    echo "  WARN: $REGISTRY/blobs/$hash not directly served by this registry yet." >&2
    echo "        It is on the mesh (uploaded to $NODE_URL); a node-backed /blobs origin will resolve it." >&2
  fi
done

echo
echo "published trana-node $("$CE_BIN" app info trana-node --registry "$REGISTRY" 2>/dev/null | sed -n 's/.*version[^0-9]*\([0-9][0-9.]*\).*/\1/p' | head -1)"
echo "install it anywhere with ONE command:"
echo "  ce app install trana-node --registry $REGISTRY --on fleet=mine --yes"
echo "  ce app daemon enable trana-node          # supervise the backend (serves trana/* forever)"
echo
echo "on a single node instead of the fleet:"
echo "  ce app install trana-node --registry $REGISTRY --on node=<node-id> --yes"
[ "$ok" -eq 1 ] || echo "(note: some artifacts warned above — install with a node-backed --registry to resolve via the mesh)"
