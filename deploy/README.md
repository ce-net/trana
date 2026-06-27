# Deploying trana

The trana **backend** runs natively under **ce-appmgr** (`ce app`) — CE's one app substrate. You
publish the backend once, then install it on any node, or your whole fleet, with a single command.
Browser **frontends** never run the backend; they reach a live trana-node over the mesh with the
`@ce-net/trana` TS SDK. Two halves, one mesh.

## Backend (native, via ce-appmgr) — the easy path

```bash
# 1. Publish: build + content-address + sign + upload. Run on the publisher node (holds the key).
./deploy/publish.sh                 # host target only
./deploy/publish.sh --with-linux    # also build linux-amd64 on the relay (so the public node can run it)

# 2. Install — ONE command, every paired device:
ce app install trana-node --on fleet=mine --yes
ce app daemon enable trana-node     # supervise it: serves trana/* RPCs, gossip-replicates, stays discoverable

# ...or a single node (e.g. the public relay so browsers can reach it):
ce app install trana-node --on node=<node-id> --yes
```

That is the whole deploy. `ce app`:

- resolves [`trana-node.ceapp.toml`](trana-node.ceapp.toml), verifies the publisher signature;
- fetches the right per-host binary by content hash from the mesh blob store and verifies it;
- supervises it (restart on failure) under the single `ce` daemon, and registers the live instance in
  ce-hub (see it with `ce app ps --app trana-node`).

`trana-node` only needs the local CE node (`ce start`) on loopback `:8844`; it talks to the mesh
through that node, so the sandbox is `net = loopback`.

### What publish.sh does

1. builds `trana-node` (release) for the host target — and, with `--with-linux`, for `linux-amd64` on
   the relay via `tools/ce-build`;
2. uploads each binary to the local node's content-addressed `/blobs` store (durable, mesh-replicated,
   128 MB), so any installing node can fetch it by hash from whoever holds a replica;
3. rewrites the `[native].artifacts` block in the manifest with the real sha256s (idempotent);
4. signs + publishes the manifest with `ce app publish`;
5. verifies each artifact resolves from the registry and prints the install commands.

Defaults: registry `https://ce-net.com`, node `http://127.0.0.1:8844`. Override with `--registry` /
`--node` or the `TRANA_REGISTRY` / `CE_NODE_URL` env vars. Local-only test: point both at a node you
control (e.g. `--registry http://127.0.0.1:8844`) and install with that `--registry`.

## Frontend (browser) — reach the backend over the mesh

A web app is a separate app; it does not bundle the backend. It discovers a live trana-node and calls
it over the mesh bridge, with discovery, load-spreading, and failover handled by the SDK:

```ts
import { Trana, profile, detectPlatform } from "@ce-net/trana";
const trana = new Trana({ node: "/ce" });                 // the page's mesh bridge (ce-serve / in-browser node)
const { threads } = await trana.feed.hot("ce-dev", profile(detectPlatform()).feedPageSize);
```

`/ce` is the mesh bridge injected by `ce-serve` (or an in-browser CE node). The frontend names
*content*, never a server — see [`../FRONTEND.md`](../FRONTEND.md).

## Alternative: ce-gke (a replicated OCI fleet)

For a self-healing pool of N containerized replicas (rather than a supervised host daemon), trana also
ships a ce-gke Deployment — see [`trana.gke.yaml`](trana.gke.yaml) and the project README. The
ce-appmgr path above is the recommended default; reach for ce-gke when you want declarative replica
counts and rolling updates across docker hosts.
