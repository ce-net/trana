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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use trana_core::karma::{trust_score, ComputeTrust, Weights};
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
    compute: ComputeProbe,
    replicator: Replicator,
    weights: Weights,
}

impl Engine {
    pub fn new(store: Arc<Store>, compute: ComputeProbe, replicator: Replicator) -> Self {
        Engine { store, compute, replicator, weights: Weights::default() }
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

    /// A user's fused trust (0.0–1.0): social karma + on-chain compute reputation across their devices.
    async fn trust_of(&self, node_id: &str) -> f64 {
        let social = self.store.social(node_id);
        let devices = device_set(node_id, self.store.profile(node_id).as_ref().map(|p| &p.profile.devices));
        let compute = self.compute.aggregate(&devices).await;
        trust_score(&social, &compute, &self.weights).combined
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

    /// A cheap, in-store trust weight for community ban voting (respect): maps a voter's social
    /// karma to a 0.1–3.0 weight so well-regarded members count more, sybils barely at all — without
    /// a per-voter network lookup.
    fn social_weight(&self, node_id: &str) -> f64 {
        let karma = self.store.social(node_id).karma() as f64;
        0.1 + 2.9 / (1.0 + (-karma / 50.0).exp())
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

    /// A full profile response: stored profile + social karma + compute trust + the fused score.
    pub async fn profile_get(&self, node_id: &str) -> Result<ProfileResp> {
        let profile = self.store.profile(node_id);
        let social = self.store.social(node_id);
        let devices = device_set(node_id, profile.as_ref().map(|p| &p.profile.devices));
        let compute = self.compute.aggregate(&devices).await;
        let trust = trust_score(&social, &compute, &self.weights);
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

    // ----- karma -----

    pub async fn karma(&self, node_id: &str) -> Result<KarmaResp> {
        let social = self.store.social(node_id);
        let devices = device_set(node_id, self.store.profile(node_id).as_ref().map(|p| &p.profile.devices));
        let compute = self.compute.aggregate(&devices).await;
        let trust = trust_score(&social, &compute, &self.weights);
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
    pub fn ban_standing(&self, board: &str, target: &str) -> BanStandingResp {
        let standing = self.store.ban_standing(board, target);
        let policy = self.store.board_policy(board);
        let mut w_support = 0.0;
        let mut w_total = 0.0;
        for (voter, support) in self.store.ban_votes_raw(board, target) {
            let w = self.social_weight(&voter);
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

/// The device set whose compute we roll up for a profile: the node itself plus any extra devices it
/// declares. Deduplicated, with the primary node always included.
fn device_set(node_id: &str, declared: Option<&Vec<String>>) -> Vec<String> {
    let mut set = vec![node_id.to_string()];
    if let Some(devs) = declared {
        for d in devs {
            if d != node_id && !set.contains(d) {
                set.push(d.clone());
            }
        }
    }
    set
}

/// Build a [`ComputeTrust`] for a single node directly (used by tests / simple callers).
#[allow(dead_code)]
pub async fn compute_for(probe: &ComputeProbe, node_id: &str) -> ComputeTrust {
    probe.aggregate(&[node_id.to_string()]).await
}
