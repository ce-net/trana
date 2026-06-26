//! The engine: every trana operation as one `author`-parameterised async method.
//!
//! Both front ends drive this same engine, so they can never diverge in behaviour:
//! - the mesh [`crate::service`] passes the authenticated mesh sender as `author`;
//! - the optional [`crate::gateway`] passes the local node's own id as `author`.
//!
//! Writes become content-addressed [`Record`]s (author + timestamp + body), are ingested into the
//! [`Store`], then broadcast for replication. Reads are served straight from the store, with the
//! compute-trust half fetched live from CE.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use trana_core::karma::{trust_score, ComputeTrust, SocialKarma, Weights};
use trana_core::model::{
    BanVote, BoardCreate, Body, Media, PolicyProposal, PolicyVote, Post, StreamEnd, StreamSegment,
    StreamStart,
};
use trana_core::proto::*;
use trana_core::record::Record;
use trana_core::state::SortBy;

use crate::compute::ComputeProbe;
use crate::replicate::{Replicator, DEFAULT_REPLICAS};
use crate::store::Store;

/// Unix milliseconds now.
fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Shared application logic behind every front end.
pub struct Engine {
    store: Arc<Store>,
    ce: ce_rs::CeClient,
    compute: ComputeProbe,
    replicator: Replicator,
    weights: Weights,
    /// Pre-trusted seed identities for the web of trust (operator-configured roots). Combined with
    /// the in-log board creators when the graph is computed. Read from `TRANA_TRUST_ROOTS`
    /// (comma-separated node ids).
    roots: Vec<String>,
    /// Cached web-of-trust ranks: `(record_count_at_compute, node -> rank)`. Recomputed lazily when
    /// the store has grown, so reads don't pay a power iteration on an unchanged graph.
    rank: RwLock<(usize, HashMap<String, f64>)>,
}

impl Engine {
    pub fn new(store: Arc<Store>, ce: ce_rs::CeClient, compute: ComputeProbe, replicator: Replicator) -> Self {
        let roots = std::env::var("TRANA_TRUST_ROOTS")
            .ok()
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default();
        Engine {
            store,
            ce,
            compute,
            replicator,
            weights: Weights::default(),
            roots,
            // usize::MAX sentinel forces a compute on first trust read.
            rank: RwLock::new((usize::MAX, HashMap::new())),
        }
    }

    /// The current web-of-trust ranks, recomputing if the store grew since the cache was built.
    /// Seeds = configured roots (weight 2.0) + every board creator (weight 1.0) + CE compute-trust
    /// nodes that have delivered real work (weight `ln(1+delivered)`, the hard-to-fake anchor — see
    /// [`ComputeProbe::seed_nodes`]). Recompute is gated on `store.len()` growth, so the bounded CE
    /// lookups happen only when new content arrives, not on every read.
    async fn rank_snapshot(&self) -> HashMap<String, f64> {
        let len = self.store.len();
        {
            let g = self.rank.read().unwrap();
            if g.0 == len {
                return g.1.clone();
            }
        }
        let mut seeds: Vec<(String, f64)> = self.roots.iter().map(|r| (r.clone(), 2.0)).collect();
        for c in self.store.board_creators() {
            seeds.push((c, 1.0));
        }
        // Compute-trust anchor (P0a makes the device→owner roll-up unforgeable, so this is sound).
        for (n, w) in self.compute.seed_nodes(32).await {
            seeds.push((n, w));
        }
        let map = self.store.trust_graph(&seeds, 0.85, 20);
        let mut g = self.rank.write().unwrap();
        *g = (len, map.clone());
        map
    }

    /// The trust-weighted, time-decayed social aggregate for a node, using the web-of-trust rank as
    /// each voter's weight. This is the karma view every trust decision reads.
    fn weighted_social(&self, node_id: &str, rank: &HashMap<String, f64>) -> SocialKarma {
        self.store.social_weighted(node_id, now_ms(), self.weights.half_life_secs, &|voter| {
            rank.get(voter).copied().unwrap_or(0.0)
        })
    }

    /// The node's own graph rank to feed the trust score: `Some(rank)` (0 if absent) when a graph was
    /// computed so the graph term applies to everyone — an unconnected account is correctly penalized
    /// — or `None` to drop the graph half when no graph exists at all (degraded).
    fn graph_rank_of(node_id: &str, rank: &HashMap<String, f64>) -> Option<f64> {
        if rank.is_empty() {
            None
        } else {
            Some(rank.get(node_id).copied().unwrap_or(0.0))
        }
    }

    /// The device set whose compute rolls up for a profile: the node itself, plus each *declared*
    /// device (`Profile.devices`) that has **also** published a matching `DeviceLink` back. The
    /// two-signature, mutual binding is what stops a profile inheriting a high-reputation node's
    /// compute trust by merely naming it — the device must consent (and can revoke).
    fn device_set(&self, node_id: &str, declared: Option<&Vec<String>>) -> Vec<String> {
        let mut set = vec![node_id.to_string()];
        if let Some(devs) = declared {
            for d in devs {
                if d != node_id && !set.contains(d) && self.store.is_device_linked(node_id, d) {
                    set.push(d.clone());
                }
            }
        }
        set
    }

    /// A voter's weight for community ban tallying. Uses the web-of-trust rank (with a small floor so
    /// engaged-but-unranked members still count toward a community verdict); falls back to the cheap
    /// karma-based curve only when no graph exists at all.
    fn voter_weight(&self, rank: &HashMap<String, f64>, node_id: &str) -> f64 {
        if !rank.is_empty() {
            return 0.05 + 0.95 * rank.get(node_id).copied().unwrap_or(0.0);
        }
        let karma = self.store.social(node_id).karma() as f64;
        0.1 + 2.9 / (1.0 + (-karma / 50.0).exp())
    }

    /// Resolve the effective author of a write: normally the authenticated sender `from`, but if the
    /// request carries `_as` (a claimed author) + `_cap` (a ce-cap capability), verify the delegation
    /// and return the claimed author. A claimed author without a valid capability is rejected.
    pub async fn resolve_author(&self, from: &str, payload: &[u8]) -> Result<String> {
        let v: serde_json::Value = serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null);
        let as_author = v.get("_as").and_then(|x| x.as_str());
        let cap = v.get("_cap").and_then(|x| x.as_str());
        match (as_author, cap) {
            (Some(u), Some(token)) => {
                crate::auth::verify_act_as(&self.ce, u, from, token).await?;
                Ok(u.to_string())
            }
            (Some(_), None) => {
                anyhow::bail!("acting as another identity requires a capability (_cap)")
            }
            _ => Ok(from.to_string()),
        }
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    pub fn replicator(&self) -> &Replicator {
        &self.replicator
    }

    /// Build a record, ingest it locally, then replicate it (gossip + nearby pin of `object_cids`).
    async fn write(
        &self,
        author: &str,
        body: Body,
        object_cids: Vec<String>,
        replicas: usize,
    ) -> Result<String> {
        let rec = Record::new(author, now_ms(), body)
            .map_err(|e| anyhow::anyhow!("build record: {e}"))?;
        self.store.ingest(&rec)?;
        let id = rec.id.clone();
        self.replicator.broadcast(rec, object_cids, replicas).await;
        Ok(id)
    }

    fn replica_count(requested: u32) -> usize {
        if requested == 0 { DEFAULT_REPLICAS } else { requested as usize }
    }

    /// A user's fused trust (0.0–1.0): trust-weighted social karma + on-chain compute reputation
    /// across their devices + web-of-trust rank.
    async fn trust_of(&self, node_id: &str) -> f64 {
        let rank = self.rank_snapshot().await;
        let social = self.weighted_social(node_id, &rank);
        let devices = self.device_set(node_id, self.store.profile(node_id).as_ref().map(|p| &p.profile.devices));
        let compute = self.compute.aggregate(&devices).await;
        trust_score(&social, &compute, Self::graph_rank_of(node_id, &rank), &self.weights).combined
    }

    /// Enforce a board's minimum-trust gate (sybil resistance). No-op when `min <= 0` (the common,
    /// fast path — we only pay the compute-trust lookup when a board actually requires trust).
    async fn require_trust(&self, who: &str, min: f64, action: &str) -> Result<()> {
        if min <= 0.0 {
            return Ok(());
        }
        let t = self.trust_of(who).await;
        if t < min {
            anyhow::bail!("{action} in this board requires trust >= {min:.2}; {who} has {t:.2}");
        }
        Ok(())
    }

    /// Reject a writer who has been community-banned from `board`.
    fn deny_if_banned(&self, board: &str, who: &str) -> Result<()> {
        if self.store.ban_standing(board, who).banned_raw {
            anyhow::bail!("the community has banned {who} from board '{board}'");
        }
        Ok(())
    }

    // ----- profile -----

    pub async fn profile_put(&self, author: &str, req: ProfilePutReq) -> Result<IdResp> {
        let id = self.write(author, profile_body(req), vec![], 0).await?;
        Ok(IdResp { id })
    }

    /// A full profile response: stored profile + trust-weighted social karma + compute trust + the
    /// fused score (with the web-of-trust term).
    pub async fn profile_get(&self, node_id: &str) -> Result<ProfileResp> {
        let profile = self.store.profile(node_id);
        let rank = self.rank_snapshot().await;
        let social = self.weighted_social(node_id, &rank);
        let devices = self.device_set(node_id, profile.as_ref().map(|p| &p.profile.devices));
        let compute = self.compute.aggregate(&devices).await;
        let trust = trust_score(&social, &compute, Self::graph_rank_of(node_id, &rank), &self.weights);
        Ok(ProfileResp { profile, social, compute, trust })
    }

    // ----- media -----

    pub async fn media_put(&self, author: &str, req: MediaPutReq) -> Result<IdResp> {
        let object_cid = req.object_cid.clone();
        let replicas = Self::replica_count(req.replicas);
        let body = Body::Media(Media {
            kind: req.kind,
            object_cid: req.object_cid,
            mime: req.mime,
            size: req.size,
            title: req.title,
            duration_ms: req.duration_ms,
            width: req.width,
            height: req.height,
            thumbnail: req.thumbnail,
            extra: req.extra,
        });
        let id = self.write(author, body, vec![object_cid], replicas).await?;
        Ok(IdResp { id })
    }

    pub fn media_get(&self, media_id: &str) -> MediaResp {
        MediaResp { media: self.store.media(media_id) }
    }

    // ----- posts / threads / comments -----

    pub async fn post_create(&self, author: &str, req: PostCreateReq) -> Result<IdResp> {
        // Community gates: banned users can't post; boards may require a trust floor (anti-spam).
        self.deny_if_banned(&req.board, author)?;
        self.require_trust(author, self.store.board_policy(&req.board).min_trust_to_post, "posting")
            .await?;
        let body = Body::Post(Post {
            board: req.board,
            parent: req.parent,
            title: req.title,
            body: req.body,
            media: req.media,
        });
        let id = self.write(author, body, vec![], 0).await?;
        Ok(IdResp { id })
    }

    pub fn post_get(&self, id: &str) -> PostResp {
        PostResp { post: self.store.post(id) }
    }

    pub fn threads(&self, req: ThreadsReq) -> ThreadsResp {
        let threads =
            self.store.threads(&req.board, SortBy::parse(&req.sort), req.limit.min(500), now_ms());
        ThreadsResp { threads }
    }

    pub fn comments(&self, req: CommentsReq) -> CommentsResp {
        let comments = self.store.comments(&req.root, SortBy::parse(&req.sort), now_ms());
        CommentsResp { comments }
    }

    // ----- vote / follow -----

    pub async fn vote(&self, author: &str, req: VoteReq) -> Result<OkResp> {
        // If the target is a post in a board with a vote-trust floor (or the voter is banned there),
        // enforce it. Targets that aren't posts (or open boards) vote freely.
        if let Some(post) = self.store.post(&req.target) {
            self.deny_if_banned(&post.board, author)?;
            self.require_trust(author, self.store.board_policy(&post.board).min_trust_to_vote, "voting")
                .await?;
        }
        self.write(author, Body::Vote(trana_core::model::Vote { target: req.target, value: req.value }), vec![], 0)
            .await?;
        Ok(OkResp { ok: true })
    }

    pub async fn follow(&self, author: &str, req: FollowReq) -> Result<OkResp> {
        self.write(
            author,
            Body::Follow(trana_core::model::Follow { followee: req.followee, active: req.active }),
            vec![],
            0,
        )
        .await?;
        Ok(OkResp { ok: true })
    }

    /// Publish this device's consent to belong to `owner` (the device's half of the mutual binding).
    /// The author is the device itself; only when `owner` also lists this device in their profile does
    /// the device's compute roll into `owner`'s trust.
    pub async fn device_link(&self, author: &str, req: DeviceLinkReq) -> Result<OkResp> {
        self.write(
            author,
            Body::DeviceLink(trana_core::model::DeviceLink { owner: req.owner, active: req.active }),
            vec![],
            0,
        )
        .await?;
        Ok(OkResp { ok: true })
    }

    /// Personalized web-of-trust: how much `viewer` trusts each requested node (or their top-ranked
    /// nodes), from the viewer's own vantage point — a PageRank restarting to the viewer. This is
    /// viewer-relative and for feed personalization; it does NOT touch the global gate/ban ranks.
    pub fn personal_trust(&self, req: PersonalTrustReq) -> PersonalTrustResp {
        let ranks = self.store.trust_graph(&[(req.viewer.clone(), 1.0)], 0.85, 20);
        let mut v: Vec<(String, f64)> = if req.nodes.is_empty() {
            ranks.into_iter().filter(|(n, _)| n != &req.viewer).collect()
        } else {
            req.nodes.iter().map(|n| (n.clone(), ranks.get(n).copied().unwrap_or(0.0))).collect()
        };
        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        v.truncate(req.limit.min(500));
        PersonalTrustResp { ranks: v }
    }

    // ----- karma -----

    pub async fn karma(&self, node_id: &str) -> Result<KarmaResp> {
        let rank = self.rank_snapshot().await;
        let social = self.weighted_social(node_id, &rank);
        let devices = self.device_set(node_id, self.store.profile(node_id).as_ref().map(|p| &p.profile.devices));
        let compute = self.compute.aggregate(&devices).await;
        let trust = trust_score(&social, &compute, Self::graph_rank_of(node_id, &rank), &self.weights);
        Ok(KarmaResp { social, compute, trust })
    }

    // ----- streams -----

    pub async fn stream_start(&self, author: &str, req: StreamStartReq) -> Result<IdResp> {
        let body = Body::StreamStart(StreamStart {
            title: req.title,
            kind: req.kind,
            board: req.board,
            thumbnail: req.thumbnail,
            extra: req.extra,
        });
        let id = self.write(author, body, vec![], 0).await?;
        Ok(IdResp { id })
    }

    pub async fn stream_append(&self, author: &str, req: StreamAppendReq) -> Result<IdResp> {
        let object_cid = req.object_cid.clone();
        let replicas = Self::replica_count(req.replicas);
        let body = Body::StreamSegment(StreamSegment {
            stream: req.stream,
            seq: req.seq,
            object_cid: req.object_cid,
            duration_ms: req.duration_ms,
        });
        let id = self.write(author, body, vec![object_cid], replicas).await?;
        Ok(IdResp { id })
    }

    pub async fn stream_end(&self, author: &str, req: StreamEndReq) -> Result<OkResp> {
        let cids = req.recording_cid.clone().into_iter().collect();
        let body = Body::StreamEnd(StreamEnd { stream: req.stream, recording_cid: req.recording_cid });
        self.write(author, body, cids, DEFAULT_REPLICAS).await?;
        Ok(OkResp { ok: true })
    }

    pub fn stream_get(&self, id: &str) -> StreamResp {
        StreamResp { stream: self.store.stream(id) }
    }

    pub fn streams_live(&self) -> StreamsLiveResp {
        StreamsLiveResp { streams: self.store.live_streams() }
    }

    // ----- community governance -----

    pub async fn board_put(&self, author: &str, req: BoardPutReq) -> Result<IdResp> {
        let body = Body::BoardCreate(BoardCreate {
            board: req.board,
            title: req.title,
            description: req.description,
            policy: req.policy,
        });
        let id = self.write(author, body, vec![], 0).await?;
        Ok(IdResp { id })
    }

    pub fn board_get(&self, board: &str) -> BoardResp {
        BoardResp { board: self.store.board(board) }
    }

    pub fn boards(&self) -> BoardsResp {
        BoardsResp { boards: self.store.boards() }
    }

    /// A board / cross-board / home feed, ranked by any feed algorithm.
    pub fn feed(&self, req: FeedReq) -> ThreadsResp {
        let sort = SortBy::parse(&req.sort);
        let now = now_ms();
        let limit = req.limit.min(500);
        let threads = match req.scope.as_str() {
            "board" => self.store.threads(req.board.as_deref().unwrap_or(""), sort, limit, now),
            "home" => self.store.home_feed(req.viewer.as_deref().unwrap_or(""), sort, limit, now),
            _ => self.store.all_feed(sort, limit, now),
        };
        ThreadsResp { threads }
    }

    pub async fn ban_vote(&self, author: &str, req: BanVoteReq) -> Result<OkResp> {
        // Ban voting itself is gated by the board's vote-trust floor (so a sybil swarm can't brigade
        // a ban). The author cannot vote while themselves banned.
        self.deny_if_banned(&req.board, author)?;
        self.require_trust(author, self.store.board_policy(&req.board).min_trust_to_vote, "ban voting")
            .await?;
        self.write(
            author,
            Body::BanVote(BanVote {
                board: req.board,
                target: req.target,
                support: req.support,
                reason: req.reason,
            }),
            vec![],
            0,
        )
        .await?;
        Ok(OkResp { ok: true })
    }

    /// A user's community ban standing: the raw one-person-one-vote tally plus the node's
    /// trust-weighted ("respect"-weighted) verdict.
    pub async fn ban_standing(&self, board: &str, target: &str) -> BanStandingResp {
        let standing = self.store.ban_standing(board, target);
        let policy = self.store.board_policy(board);
        let rank = self.rank_snapshot().await;
        let mut w_support = 0.0;
        let mut w_total = 0.0;
        for (voter, support) in self.store.ban_votes_raw(board, target) {
            let w = self.voter_weight(&rank, &voter);
            w_total += w;
            if support {
                w_support += w;
            }
        }
        let weighted_support = if w_total > 0.0 { w_support / w_total } else { 0.0 };
        let banned = (standing.support + standing.oppose) >= policy.ban_quorum as u64
            && weighted_support >= policy.ban_support;
        BanStandingResp { standing, weighted_support, banned }
    }

    pub async fn policy_propose(&self, author: &str, req: PolicyProposeReq) -> Result<IdResp> {
        let body = Body::PolicyProposal(PolicyProposal {
            board: req.board,
            title: req.title,
            body: req.body,
        });
        let id = self.write(author, body, vec![], 0).await?;
        Ok(IdResp { id })
    }

    pub async fn policy_vote(&self, author: &str, req: PolicyVoteReq) -> Result<OkResp> {
        self.write(
            author,
            Body::PolicyVote(PolicyVote { proposal: req.proposal, support: req.support }),
            vec![],
            0,
        )
        .await?;
        Ok(OkResp { ok: true })
    }

    pub fn proposals(&self, board: Option<&str>) -> ProposalsResp {
        ProposalsResp { proposals: self.store.proposals(board) }
    }

    pub fn proposal_get(&self, id: &str) -> ProposalResp {
        ProposalResp { proposal: self.store.proposal(id) }
    }

    // ----- documents + versioning -----

    /// Create a document or publish a new version of one. If the document declares a board, the
    /// board's post-trust gate and ban list apply (same community rules as posts). The bytes of a
    /// file payload are replicated to nearby nodes.
    pub async fn document_put(&self, author: &str, req: DocPutReq) -> Result<IdResp> {
        if let Some(board) = &req.board {
            self.deny_if_banned(board, author)?;
            self.require_trust(author, self.store.board_policy(board).min_trust_to_post, "publishing")
                .await?;
        }
        let object_cids: Vec<String> = req.file.as_ref().map(|f| vec![f.object_cid.clone()]).unwrap_or_default();
        let replicas = if object_cids.is_empty() { 0 } else { DEFAULT_REPLICAS };
        let body = Body::Document(trana_core::model::Document {
            title: req.title,
            body: req.body,
            file: req.file,
            refs: req.refs,
            board: req.board,
            series: req.series,
            prev: req.prev,
        });
        let id = self.write(author, body, object_cids, replicas).await?;
        Ok(IdResp { id })
    }

    pub fn document_get(&self, id: &str) -> DocResp {
        DocResp { document: self.store.document(id) }
    }
    pub fn document_history(&self, key: &str) -> DocsResp {
        DocsResp { documents: self.store.document_history(key) }
    }
    pub fn document_latest(&self, key: &str) -> DocResp {
        DocResp { document: self.store.document_latest(key) }
    }
    pub fn document_diff(&self, from: &str, to: &str) -> DocDiffResp {
        DocDiffResp { diff: self.store.document_diff(from, to) }
    }
    pub fn documents_by(&self, author: &str) -> DocsResp {
        DocsResp { documents: self.store.documents_by(author) }
    }
    pub fn backlinks(&self, id: &str) -> BacklinksResp {
        BacklinksResp { uris: self.store.backlinks(id) }
    }

    // ----- replication (internal) -----

    /// Handle a directed replication push: ingest the record and pull its objects.
    pub async fn replicate(&self, payload: &[u8]) -> OkResp {
        let ok = crate::replicate::ingest_replication(&self.store, &self.replicator, payload).await;
        OkResp { ok }
    }
}

/// Build a [`ComputeTrust`] for a single node directly (used by tests / simple callers).
#[allow(dead_code)]
pub async fn compute_for(probe: &ComputeProbe, node_id: &str) -> ComputeTrust {
    probe.aggregate(&[node_id.to_string()]).await
}
