# @ce-net/trana — TypeScript SDK

One easy, **type-safe** handle over the whole trana distributed backend. You call
`trana.posts.create(...)`, `trana.feed.hot(board)`, `trana.media.upload(bytes)` and never think about
*which* node serves you, how many replicas exist, or how content is chunked and addressed — the SDK
does discovery, **failover**, and content addressing underneath.

It talks to a local CE node's HTTP API, which routes your calls to live `trana` instances over the
mesh. In the browser, point it at the node exposed by `ce-serve`'s mesh-bridge; in Node, at your
local `ce` node (`http://127.0.0.1:8844`).

## Install

```bash
npm install @ce-net/trana
```

## Quick start

```ts
import { Trana } from "@ce-net/trana";

const trana = new Trana({ node: "http://127.0.0.1:8844", token: process.env.CE_API_TOKEN });

// Threads / posts / comments
const { id } = await trana.posts.create({ board: "ce-dev", title: "hello", body: "first post" });
await trana.posts.reply("ce-dev", id, "nice");
await trana.vote(id, 1);

// Feeds — the feed algorithm: hot | new | top | best | trending | rising | controversial
const { threads } = await trana.feed.trending("ce-dev");
const home = await trana.home(myNodeId, "hot");

// Media — upload bytes (chunked + content-addressed for you) and register a descriptor
const file = new Uint8Array(await (await fetch("/clip.mp4")).arrayBuffer());
const mediaId = await trana.media.add("video", "video/mp4", file, "demo");
const bytes = await trana.media.download(mediaId);

// Live streaming
const { id: sid } = await trana.streams.start({ title: "live", kind: "video" });
await trana.streams.pushSegment(sid, 0, segBytes, 2000);
const { stream } = await trana.streams.get(sid); // growing playlist
await trana.streams.end(sid);

// Profiles + trust
await trana.profile.set({ displayName: "Leif", bio: "building ce-net", devices: [otherNodeId] });
const { trust } = await trana.profile.get(myNodeId); // social + on-chain compute trust, fused

// Community governance (no mods)
await trana.boards.create({ board: "town", policy: { ban_quorum: 10, min_trust_to_vote: 0.2 } });
await trana.banVote("town", someNodeId, true);           // community ban vote
const { banned } = await trana.banStanding("town", someNodeId);
await trana.policy.propose({ title: "No spam", body: "links need context", board: "town" });
```

## How distribution is hidden

- **Discovery + failover.** Every call resolves live `trana` instances (via the node's DHT
  discovery), round-robins across them to spread load, and fails over to the next instance on a
  transport error — surfacing only *application* errors. Set `node`/`token`; that's it.
- **Content addressing.** `media.upload` / `streams.pushSegment` chunk bytes (1 MiB), hash each
  chunk with Web Crypto, store them, and return an object CID; downloads verify every chunk. Works in
  the browser and Node, so a phone genuinely contributes the bytes.
- **Type safety.** Every request and response is typed (`src/types.ts` mirrors `trana_core::proto`),
  so the compiler catches mistakes before they ship.

## Build

```bash
npm run typecheck   # tsc --noEmit (strict)
npm run build       # emit dist/ (ESM + .d.ts)
```

There is a matching **Rust SDK** (`trana-sdk`) with the same model and the same locate+failover
behavior, for native apps and agents.
