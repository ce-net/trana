# trana

A **distributed social/content backend** built on CE primitives — and a **reusable trust layer**.
trana is not one app: it is the shared backend many different frontends can build on, all referencing
the same profiles and the same trust scores to decide *whom to entrust with critical tasks*.

It serves every kind of content, 100% distributed and replicated across the mesh:

- **Threads** — Reddit-like boards, threads, and comment trees, with up/down votes (karma).
- **Media** — images, video, audio, podcasts, documents — content-addressed in the CE object store.
- **Live streaming** — a growing, content-addressed segment playlist (HLS-like) over the mesh, video or audio.
- **Profiles** — a person and the devices they own, with their compute capacity, uptime, and price exposed via API.
- **Karma + compute trust** — social reputation fused with on-chain compute reputation into one transparent score.

There is **no frontend here** — this is the backend and its API. Frontends are separate apps.

## Why it builds trust

The point of trana is trust. A profile that has proven itself — good posts and comments (Reddit-style
karma), *and* delivered compute with real uptime — earns a higher trust score, so others are more
likely to hand it critical tasks. The trust formula
([`trana_core::karma`](crate/trana-core/src/karma.rs)) is transparent and tunable; every input and
component is exposed, never a black box. The compute half is read straight from CE's on-chain facts
(`/history`, `/atlas`), so it is economically costly to fake.

## Community governance (no moderators)

trana has **no mods and no owners with removal powers**. Governance is the community: trust-weighted
voting, trust, and respect. The defining inversion of Reddit:

- **Controversy is given visibility, not buried.** Every new post gets a **grace window** (per-board
  `grace_secs`, default 6h) during which the default feed ranks it by *engagement* (ups + downs,
  sign-agnostic) — so a dissenting or unpopular take gets *seen* first, with a real chance to persuade
  people or find the ones who already agree. Only **after** the window does net reception take over,
  so sustained approval persists and sustained "booing" fades.
- **Banning is a community vote.** A `BanVote` is one-person-one-vote; a ban takes effect only when it
  clears the board's **trust-weighted support fraction** (`ban_support`, default 0.66) *and* a
  distinct-voter **quorum** (`ban_quorum`, default 10). Reversible if the community changes its mind.
  A banned user's content is hidden and they can't post/comment/vote in that board.
- **Trust gates participation.** Boards can require a minimum fused trust to post or vote
  (`min_trust_to_post` / `min_trust_to_vote`, default 0 = open) — the sybil-resistance lever, using
  the costly-to-fake on-chain compute trust. Ban votes are weighted by social standing ("respect").
- **Policies are voted forward by people** (`PolicyProposal` / `PolicyVote`) — the substrate a future
  AI policy layer will enforce. People decide the rules; AI applies them.

## The feed algorithm

One enum, seven rankings (all in `trana_core::state`):

| Sort | What it surfaces |
|---|---|
| `hot` (default) | the grace-aware community feed above — controversy gets a chance, then reception decides |
| `new` | newest first |
| `top` | highest net score (pure reception) |
| `best` | Wilson lower-bound confidence — *how confidently liked*, robust to sample size |
| `trending` | velocity: engagement decayed steeply by age — what's blowing up now |
| `rising` | young posts gaining traction fast (score + replies per early hour) |
| `controversial` | high engagement that's split — the fights |

Available per-board, cross-board (`all`), or as a personalized `home` feed (thread roots from accounts
you follow).

## Architecture

Four crates, mirroring the CE mesh-app pattern (mesh-native RPC, content-addressed blobs, `locate`
for discovery):

| Crate | Role |
|---|---|
| **`trana-core`** | The pure, **WASM-clean** heart: content model, content-addressed signed records, the materialized read-model fold, and the trust math. No tokio/reqwest/libp2p — a phone or browser tab compiles it to `wasm32` and *contributes* (verifies records, folds the model, chunks media), not just consumes. |
| **`trana-node`** | The backend daemon: serves every `trana/*` mesh RPC, persists an append-only record log, **replicates** via gossip, **places media on nearby nodes**, and aggregates compute trust. Optional HTTP gateway (`--features gateway`, off by default). |
| **`trana-sdk`** | Typed client: `locate` a live trana instance over the mesh and call its RPCs, with failover. |
| **`trana-cli`** | `trana` command — seed and manage profiles, posts, media, and live streams. |

### How distribution works

- **Records** (the social graph) are tiny, signed, content-addressed events. Every write is gossiped
  on `trana/events/v1`; every node subscribes and folds them in. The fold is idempotent and
  order-tolerant, so all nodes converge — it is a replicated read-model, not a single source of truth.
- **Media bytes** live in CE's chunked, content-addressed object store. After a write, the origin
  asks a few **nearby** trana instances (chosen by `ce-rs::locate` — ranked by trust + capacity +
  recency, spread across fault domains) to pull and pin the object CIDs, so bytes live near where
  they're served. "Auto-spawning on closeby nodes" *is* this placement step.
- **Mobile/WASM** nodes participate because `trana-core` is runtime-free and clients upload media
  chunks themselves (`put_object`) — they contribute the bytes and can verify everything locally.

## API

Canonical API is **mesh-native** request/reply (reach a node by NodeId, never a stored ip:port). Topics
and payloads are defined once in [`trana_core::proto`](crate/trana-core/src/proto.rs) and shared by
the node and SDK. Highlights:

```
trana/profile/put|get      trana/post/create|get     trana/threads        trana/comments
trana/media/put|get        trana/vote                trana/follow         trana/karma
trana/stream/start|append|end|get   trana/streams/live
trana/events/v1 (gossip)   trana/replicate (internal placement)
```

Each reply is a JSON `Envelope { ok, error?, data }`. A `ProfileResp`/`KarmaResp` carries the profile,
social karma, **compute trust** (devices, jobs hosted, earned, cpu/mem, uptime, inferred avg price),
and the fused `TrustScore`.

The optional gateway exposes the same operations as REST (`GET /profile/:id`, `POST /post`,
`GET /threads/:board`, `GET /karma/:id`, `POST /stream/append`, ...) for plain-HTTP frontends.

## Run it

```bash
# Needs a local CE node running (`ce start`).
cargo run -p trana-node --release                     # join the mesh, serve trana
cargo run -p trana-node --release --features gateway -- --gateway-port 8975   # + REST facade

# Seed content with the CLI:
trana profile-set --display-name "Leif" --bio "building ce-net" --device <other-node-id>
trana post --board ce-dev --title "trana is live" --body "100% distributed."
trana media ./clip.mp4 --kind video --title "demo"     # uploads bytes + registers media
trana stream-start --title "dev stream" --kind video    # prints a stream id
trana stream-append <stream-id> 0 ./seg0.ts --duration-ms 2000
trana karma <node-id>                                   # social + compute trust
```

## Deploy with ce-appmgr (`ce app`) — the easy path

The backend deploys as a ce-appmgr package: publish once, then install on any node — or your whole
fleet — with one command. `ce app` fetches the right per-host binary by content hash from the mesh,
verifies it, supervises it (restart on failure), and registers the live instance.

```bash
./deploy/publish.sh                                  # build + content-address + sign + publish
ce app install trana-node --on fleet=mine --yes      # 1 command, every paired device
ce app daemon enable trana-node                       # supervise it (serves trana/* forever)
```

Browser frontends do not run the backend; they reach a live trana-node over the mesh with the
`@ce-net/trana` TS SDK (`new Trana({ node: "/ce" })`). Full guide: [`deploy/README.md`](deploy/README.md).

### Alternative: orchestrate a replicated fleet with ce-gke

For a self-healing pool of N containerized replicas, trana also ships a ce-gke Deployment manifest
([`deploy/trana.gke.yaml`](deploy/trana.gke.yaml)): ce-gke places N replicas across docker-capable
hosts, keeps that many Running, replaces any that die, and advertises the healthy set as
`ce-gke/social/trana-api` for `ce_rs::locate`.

```bash
ce-gke --grant <cap-token> apply -f trana/deploy/trana.gke.yaml
ce-gke run --every 10        # reconcile/heal forever
```

## Tests

- `cargo test --workspace` — 24 unit tests (records, read-model fold, karma/trust, store).
- `ce-gke/tests/trana_e2e.rs` — deterministic e2e: ce-gke deploys trana and self-heals under random
  replica/node failure, scale, and rolling update (4/4 green; `cargo test -p ce-gke --test trana_e2e`).
- `~/ce-net/e2e/e2e-trana.sh` — hermetic live mesh: two trana nodes on a real CE mesh; proves
  cross-node gossip replication, object replication, the trust API, and fault tolerance (content
  survives a trana node + a CE node being killed). Run: `CE_BIN=~/.local/bin/ce e2e/e2e-trana.sh`.
- `~/ce-net/e2e/e2e-trana-gke.sh` — live ce-gke deploy + heal against real docker hosts.

## Status

v0.1 foundation: full content model, content-addressed signed records, the replicated read-model,
gossip + nearby-placement replication, compute-trust aggregation, the mesh RPC service, the SDK, the
CLI, and the optional gateway — all unit-tested. Authorship is established by authenticated mesh
ingest; records also carry an optional detached ed25519 signature (`trana-core` verifies it) for
fully self-authenticating replication once the node exposes raw signing. Transcoding, paid
pinning/SLAs, and a public ingress are layered on top later.
