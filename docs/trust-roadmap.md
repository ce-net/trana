# Trust roadmap: web-of-trust, trust-weighted voting, decay

Status: **P0b, P1, P2, P3 implemented** (2026-06-26). P0a (device-binding) and P5 (personalized
PPR) remain. This extends the existing trust system (`trana-core/src/karma.rs`,
`trana-core/src/state.rs`, `trana-node/src/engine.rs`) without breaking the append-only log or the
convergence guarantee.

## What shipped

- **Reddit-grade karma reliability.** Karma is no longer a raw vote count. `State::social_weighted`
  computes an `effective_score` where every received vote is multiplied by the voter's web-of-trust
  rank and time-decayed. An upvote from a zero-trust account moves karma by ~0; you can only build
  karma when already-trusted members approve you. `trust_score` reads `effective_score`, not the raw
  count. (Raw counts are still reported for display.)
- **Decay.** `karma::decay(age, half_life)` = `2^(-age/half_life)`; votes now retain their
  `created_ms` (`State::target_votes`) so age can be applied. Default half-life 90d, in `Weights`.
- **Web of trust.** `State::trust_graph` is a personalized PageRank over the follow graph, restarting
  to a seed set (configured roots `TRANA_TRUST_ROOTS` + in-log board creators). A sybil ring with no
  inbound follow from the seed-connected component stays at ~0 rank regardless of cross-following.
  The node caches it (`Engine::rank_snapshot`, recomputed when the store grows) and uses it both as
  each voter's weight and as a third `graph_term` in the fused `TrustScore`.
- Ban voting now weights voters by the same web-of-trust rank (`Engine::voter_weight`).

The append-only log is unchanged — no new record types, no migration. These are all reinterpretations
of the existing follow/vote records, which already carry author + timestamp.

## Where we are today

- **Social karma** (`State::social`, state.rs:844) sums *raw* ups/downs across a user's
  posts, comments and documents. One-vote-one-weight. Cumulative for all time — no decay.
- **TrustScore** (`karma::trust_score`, karma.rs:158) is a logistic blend of a social term
  and a compute term (CE `/history` + `/atlas`). Default weights 0.45 / 0.55.
- The **only** trust-weighted path is community ban voting: `Engine::social_weight`
  (engine.rs:117) maps a voter's karma to a 0.1–3.0 weight, applied in `ban_standing`
  (engine.rs:340). Ordinary content votes are unweighted.
- The **follow graph** (`follows`, state.rs:223) is stored but only powers the home feed.
  It does not propagate trust.
- Votes are stored as `votes: (voter,target)->i8` + `tally: target->(ups,downs)`
  (state.rs:218-220). **The vote timestamp is discarded** at apply time
  (`apply_vote`, state.rs:318) — so nothing can currently be decayed by vote age.

## Design constraints (do not violate)

1. **Convergence.** `State` is folded from records; a server, phone and browser that have
   seen the same records must produce identical views (state.rs:1-8). Any global trust value
   must be a deterministic function of the folded log + a passed-in `now_ms` — never of
   wall-clock read order or per-node randomness. (`threads`/`comments` already take `now_ms`;
   follow that pattern.)
2. **WASM-clean core.** `trana-core` has no async/network. Pure math (decay, graph power
   iteration, social-only weighting) lives in core. Anything needing a live CE lookup
   (full compute trust) lives in `trana-node`, like `ban_standing` already does.
3. **Additive, no migration.** Follows and votes already carry the graph and the timestamps
   (records have `created_ms`). These three features are *interpretation* of the existing
   log — **no new `Body` variants required for v1**, no rewrite of `log.jsonl`.

## Prerequisite finding — the self-declared-devices hole

`device_set` (engine.rs:443) rolls up compute trust over `Profile.devices`, which the
profile author declares freely. Nothing proves the author controls those node keys, so any
identity can list a high-reputation node as its "device" and inherit its compute trust. Since
the plan below uses **compute trust as the hard-to-fake seed** for web-of-trust, this hole
must be closed first or the seed is forgeable.

**Fix (P0a):** require a bidirectional binding before counting a device — a `DeviceClaim`
record signed by the *device* key asserting "I belong to U", or a ce-cap chain rooted at the
device. Only count compute for devices that have reciprocated. Small new `Body` variant +
fold rule + a filter in `device_set`.

---

## Feature 1 — Decay (time-weighted reputation)

Recent reputation should count more; old one-time pumps and abandoned accounts should fade.

**Data-model change (the one real change):** retain vote timestamps and a reverse index.

```
// state.rs — replace the two maps with one that keeps who + when:
votes_by_target: HashMap<String, HashMap<String, (i8, u64)>>  // target -> voter -> (value, created_ms)
// keep `tally: target->(ups,downs)` as a cached raw fast-path (unchanged semantics)
```

`apply_vote` (state.rs:318) already receives the record; thread `created_ms` in and store it.

**Core API (pure, deterministic):**

```
// karma.rs / state.rs
fn decay(age_secs: u64, half_life_secs: u64) -> f64   // 2^(-age/half_life)
State::social_at(&self, node, now_ms, half_life_secs) -> SocialKarma  // decayed sums
```

Each vote contributes `value * decay(now - vote.created_ms, τ)` instead of `±1`. Post/comment
age can also taper. Add `half_life_secs` to `Weights` (karma.rs:114); a sensible default is
~90d for social, tunable per board. Compute decay (recent vs old jobs) is lower priority since
CE `/history` is already point-in-time.

Plumb `now_ms` through `profile_get`/`karma` (engine.rs:138,233) the way `threads` already does.

## Feature 2 — Trust-weighted voting

Mirror the ban-voting model for ordinary content votes: a score is a *weighted* sum of voter
weights, not a raw count.

**Core API:**

```
State::weighted_score(&self, target, weight_fn: &impl Fn(&str) -> f64) -> f64
```

Iterate the target's `voter -> (value, created_ms)` map, sum `value * decay(...) * weight_fn(voter)`.
The reverse index from Feature 1 makes this O(voters-on-target).

**Weight source — critical ordering to avoid the sybil cycle.** Do **not** weight votes by
karma: a sybil ring cross-votes to inflate each other's karma, which would then inflate their
vote weight. Instead weight votes by **graph rank** (Feature 3), which a ring cannot raise
without honest follows. So the dependency is: graph rank → vote weight → (weighted) karma —
never the reverse. Until Feature 3 lands, ship weighted voting using `social_weight` only for
the ban path (status quo) and keep content votes raw.

Expose weighted score as an alternate ranking input in `rank_views` (state.rs:884) and as a
`social` aggregate variant feeding `trust_score`. Keep the raw tally as the cheap default so a
phone with no rank table still renders feeds.

## Feature 3 — Web-of-trust (transitive propagation)

Turn the follow graph (optionally plus strong positive votes) into a trust rank.

**Choice: global EigenTrust-style propagation, seeded by compute trust.** A per-viewer
personalized PageRank is more sybil-resistant but is viewer-relative, which breaks the single
convergent ban/gate verdict every node must agree on. So:

- **Global rank for trust *scores* / gates / bans** — deterministic eigenvector over the
  folded graph → every node computes the same value from the same log.
- **Personalized PPR offered as a separate query** for feed personalization only (P5, below),
  where viewer-relative is fine.

**Core API (pure power iteration — no network, fully deterministic):**

```
State::trust_graph(&self, seeds: &[(String, f64)], damping: f64, iters: usize)
    -> HashMap<String, f64>     // node -> rank in 0..1
```

Edges = active follows (and, optionally, sustained upvote relationships) weighted by the
*source's own rank* (inherent in the eigenvector — a follow from a no-rank sybil carries no
weight). `seeds` = the top-N compute-trust nodes (hard to fake once the device-binding hole is
closed), so trust is anchored to an economic cost, not just the social graph.

**Node side (`trana-node`):** recompute the global rank periodically (every N seconds or every
M new follow/vote records), cache it behind an `RwLock` in `Engine`, and reuse the same pass to
build the `node -> trust` map that Feature 2 needs for weighting — so no per-vote network calls.

**Scoring change:** add a third term to `TrustScore`:

```
Weights { social, compute, graph, social_scale, compute_scale, graph_scale, half_life_secs }
combined = (w.social*social_term + w.compute*compute_term + w.graph*graph_term) / Σw
```

Update `karma::trust_score` (karma.rs:158) to take `graph_rank: f64`, and `KarmaResp` /
`ProfileResp` (proto) to surface `graph_term` so the UI can show *why* someone is trusted.

**Residual attacks to document, not hide:** an honest user who follows a sybil leaks a bounded
share of their own rank to it (bounded by their rank × out-degree share); a sybil that farms
real follows still climbs. Mitigations: cap per-source out-edge contribution, decay follow
edges by age (reuse Feature 1's `decay`), keep the compute seed weighted heavily.

---

## Rollout (each phase independently shippable, additive, tested)

| Phase | Status | Scope | Touches |
|-------|--------|-------|---------|
| **P0a** | TODO | Device-binding fix (prereq for trusting compute as a *seed*) | new `DeviceClaim` body + fold + `device_set` filter |
| **P0b** | DONE | Vote timestamps + reverse index | `target_votes`, `apply_vote`, tests |
| **P1** | DONE | Decay: `decay`, `Weights.half_life_secs`, `now_ms` plumbed | core + engine |
| **P2** | DONE | `weighted_score` + `social_weighted` effective karma | core + engine |
| **P3** | DONE | Web-of-trust: `trust_graph`, node recompute+cache, 3rd `TrustScore` term, seeded by roots + board creators; vote/ban weight = graph rank | core + engine |
| **P5** | TODO | Personalized PPR `trust/graph?from=` query for feed personalization | engine + proto + SDK |

## Operational notes

- Set `TRANA_TRUST_ROOTS` (comma-separated node ids) on each node to a shared anchor set so global
  ranks converge and sybil resistance is real; with no roots the graph falls back to a uniform
  restart over board creators (weaker, still useful for ranking).
- Seeds today are **board creators + configured roots**, not compute-trust nodes — so P0a is *not*
  blocking this release. Switching/adding compute-trust seeding is the P0a follow-up (it requires the
  device-binding fix first, or self-declared `Profile.devices` would forge the seed).
- Decay uses each node's wall-clock `now_ms`, so two nodes' *trust scores* can differ by seconds of
  decay. The convergent content-hiding decision still uses the raw `banned_raw` tally, so this drift
  never causes nodes to disagree on what is hidden.

Convergence, idempotence and the existing 24 unit tests must stay green at every phase; add
fold tests for decayed scores, weighted tallies, and a sybil-ring scenario (ring gets near-zero
graph rank despite heavy cross-voting) for P3.
