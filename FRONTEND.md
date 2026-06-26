# trana — frontend & client architecture

How any frontend connects to trana efficiently while the backend stays distributed, fault-tolerant,
and scalable to a very large number of clients. The rule: **the frontend never deals with load
balancers, replicas, or content distribution** — the SDK does. You subscribe to *content*, not to
servers.

## One transport, three clients

The same wire protocol (mesh request/reply by NodeId, content addressed by hash) serves all three;
only the per-client **budgets** differ ([`trana_sdk::platform`] in Rust, `platform.ts` in TS):

| Client | Node | Transport | Budget |
|---|---|---|---|
| **Native** | full in-process CE node | direct mesh | largest — 100-item pages, 64 MiB media autoload, autoplay, 16 fetches |
| **Desktop browser** | WASM node (or local-node proxy) | mesh / `/ce` | medium — 50-item pages, 16 MiB autoload, 8 fetches |
| **Mobile browser** | WASM-only | relay `wss` | tightest — 20-item pages, thumbnails-only, tap-to-play, 4 fetches |

```ts
import { Trana, profile, detectPlatform } from "@ce-net/trana";
const p = profile(detectPlatform());                  // budgets for this device
const trana = new Trana({ node: "/ce" });
const { threads } = await trana.feed.hot("ce-dev", p.feedPageSize); // bounded page
```

```rust
use trana_sdk::{Trana, Platform};
let p = Platform::MobileBrowser.profile();             // tighter budgets on a phone
```

## What makes it scale

- **Bounded per-client cost.** A client pulls *pages* (feed page size) and loads media lazily
  (thumbnails first on mobile, full bytes on demand), so cost is O(what you look at), not
  O(population). That is why a million clients is affordable.
- **Discovery + failover are invisible.** Every SDK call resolves live `trana` instances, round-robins
  to spread load, and fails over to the next instance on a transport error — surfacing only real
  application errors. You never name a server.
- **Content addressing, not URLs.** Posts/documents/media/streams are addressed by `trana://<kind>/<id>`
  (content hash / NodeId), resolvable from any node — there is no origin to balance or cache-bust.
  Media + stream segments are chunked and content-addressed, so the same bytes are fetchable from any
  node that holds a replica, and dedupe is automatic.
- **Local-first reads.** A native or desktop-WASM client *is* a node: it can serve and verify content
  locally, gossip-replicate what it sees, and fall back to the relay only when needed.

## Fault tolerance (transparent to the frontend)

- **Replicated reads.** The social graph gossips to every trana node; killing one node loses nothing
  (proven: `e2e/e2e-trana.sh` kills a trana node and a CE node mid-flight and content keeps serving).
- **Nearby placement.** Media/segment bytes are pinned onto nearby instances; reads come from whoever
  is closest and alive.
- **Reconnect backoff.** On a dropped connection the client retries with capped exponential backoff
  (`Platform::reconnect_backoff_ms` / `reconnectBackoffMs`), tuned per platform.

## Live streaming on the client

Start a stream, push content-addressed segments, and any client polls the growing playlist
(`streams.get`) and fetches segments by CID — an HLS-like flow with no media server, fully
distributed. Mobile clients fetch lower-frequency / on-demand; native clients prefetch.

## Versioned documents & files

Documents and files (PDFs, datasets, binaries) are versioned (`documents.history` / `diff` /
`latest`) as a content-addressed chain — git-like, immutable, verifiable. A reader resolves
`trana://document/<id>` to a specific version or asks for `latest`; an editor publishes a new version
with `documents.update(series, prev, ...)`.

## Connection lifecycle (pseudocode)

```
trana = new Trana({ node, token })          // discovery is lazy
budget = profile(detectPlatform())
loop:
  page = trana.feed.hot(board, budget.feedPageSize)   // bounded; auto-failover underneath
  render(page); prefetch(budget.prefetchPages)
  for media in visible: load up to budget.maxMediaAutoloadBytes, else thumbnail
on disconnect: sleep reconnectBackoffMs(platform, attempt++); rediscover; resume
```

The native-mobile client (a real in-process node on the phone, not WASM-over-wss) is the next step;
the budgets and wire are already in place for it.
