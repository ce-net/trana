//! # trana-sdk — typed client for the trana backend
//!
//! Build profiles, post to threads, vote, upload media, and run live streams against trana from any
//! app. The SDK is deliberately thin: it locates a live `trana` instance over the mesh (ranked by
//! trust + capacity, with failover) and makes the typed RPC call. Because the serving instance
//! attributes every write to the **authenticated caller**, your content is authored by *your* CE
//! node no matter which trana host serves the request.
//!
//! trana is a reusable component: many different social/content frontends can build on this one
//! backend and share the same trust profiles. Media bytes are uploaded straight to the CE object
//! store with [`TranaClient::upload`] (works from native, mobile, or browser nodes — they contribute
//! the bytes), then registered with [`TranaClient::media_put`].
//!
//! ```no_run
//! # async fn demo() -> anyhow::Result<()> {
//! use trana_sdk::TranaClient;
//! use trana_core::proto::PostCreateReq;
//!
//! let t = TranaClient::local();
//! let post = t.post_create(PostCreateReq {
//!     board: "ce-dev".into(), parent: None,
//!     title: Some("hello trana".into()), body: "first post".into(), media: vec![],
//! }).await?;
//! println!("created {}", post.id);
//! # Ok(()) }
//! ```

pub mod platform;
pub use platform::{Platform, Profile};

use anyhow::{anyhow, Result};
use ce_rs::locate::LocateOpts;
use ce_rs::CeClient;
use trana_core::proto::{self, Envelope};

/// Default per-call timeout.
const TIMEOUT_MS: u64 = 10_000;

/// A client for the trana backend, talking to whichever live instance the mesh selects.
#[derive(Clone)]
pub struct TranaClient {
    ce: CeClient,
    timeout_ms: u64,
    /// If set, always call this specific trana node instead of locating one.
    pinned: Option<String>,
    /// Delegated identity: `(claimed_author, capability_token)`. When set, every write is attributed
    /// to `claimed_author` and carries the ce-cap capability proving this device may act as them.
    act_as: Option<(String, String)>,
}

impl TranaClient {
    /// Client against the local CE node, auto-locating a trana instance per call.
    pub fn local() -> Self {
        Self::new(CeClient::local())
    }

    /// Client against a given [`CeClient`].
    pub fn new(ce: CeClient) -> Self {
        TranaClient { ce, timeout_ms: TIMEOUT_MS, pinned: None, act_as: None }
    }

    /// Pin every call to one specific trana node id (skip discovery). Useful for tests and for
    /// talking to your own co-located node.
    pub fn pinned(ce: CeClient, node_id: impl Into<String>) -> Self {
        TranaClient { ce, timeout_ms: TIMEOUT_MS, pinned: Some(node_id.into()), act_as: None }
    }

    /// Override the per-call timeout.
    pub fn with_timeout(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Act as another identity via a ce-cap capability: every write is attributed to `author` and
    /// carries `cap_token` (a `ce grant <this-device> --can trana:act` token signed by `author`). The
    /// serving node verifies the delegation and rejects a forged one.
    pub fn with_act_as(mut self, author: impl Into<String>, cap_token: impl Into<String>) -> Self {
        self.act_as = Some((author.into(), cap_token.into()));
        self
    }

    /// The underlying CE client (for uploading media bytes, checking status, etc.).
    pub fn ce(&self) -> &CeClient {
        &self.ce
    }

    /// Upload media bytes to the CE object store and return the object CID. Chunked + content
    /// addressed client-side, so a phone or browser tab can do this and genuinely contribute the
    /// bytes. Pass the returned CID to [`media_put`](Self::media_put) / stream append.
    pub async fn upload(&self, bytes: &[u8]) -> Result<String> {
        self.ce.put_object(bytes).await
    }

    /// One typed RPC: encode `req`, send to a trana instance, decode the [`Envelope`] reply as `R`.
    async fn call<Q: serde::Serialize, R: for<'de> serde::Deserialize<'de>>(
        &self,
        topic: &str,
        req: &Q,
    ) -> Result<R> {
        // When acting as a delegated identity, merge `_as`/`_cap` into the (object) request body.
        let payload = match &self.act_as {
            Some((author, cap)) => {
                let mut v = serde_json::to_value(req)?;
                if let serde_json::Value::Object(m) = &mut v {
                    m.insert("_as".into(), serde_json::Value::String(author.clone()));
                    m.insert("_cap".into(), serde_json::Value::String(cap.clone()));
                }
                serde_json::to_vec(&v)?
            }
            None => serde_json::to_vec(req)?,
        };
        let reply = match &self.pinned {
            Some(node) => self.ce.request(node, topic, &payload, self.timeout_ms).await?,
            None => {
                ce_rs::locate::call(
                    &self.ce,
                    proto::SERVICE,
                    topic,
                    &payload,
                    &LocateOpts::default(),
                    self.timeout_ms,
                )
                .await?
            }
        };
        Envelope::decode(&reply).map_err(|e| anyhow!("trana {topic}: {e}"))
    }

    // ----- profile -----

    pub async fn profile_put(&self, req: proto::ProfilePutReq) -> Result<proto::IdResp> {
        self.call(proto::T_PROFILE_PUT, &req).await
    }

    pub async fn profile_get(&self, node_id: &str) -> Result<proto::ProfileResp> {
        self.call(proto::T_PROFILE_GET, &proto::ProfileGetReq { node_id: node_id.into() }).await
    }

    // ----- media -----

    pub async fn media_put(&self, req: proto::MediaPutReq) -> Result<proto::IdResp> {
        self.call(proto::T_MEDIA_PUT, &req).await
    }

    pub async fn media_get(&self, media_id: &str) -> Result<proto::MediaResp> {
        self.call(proto::T_MEDIA_GET, &proto::MediaGetReq { media_id: media_id.into() }).await
    }

    /// Fetch the actual media bytes for a media id (resolves the descriptor, then the object).
    pub async fn download(&self, media_id: &str) -> Result<Vec<u8>> {
        let m = self
            .media_get(media_id)
            .await?
            .media
            .ok_or_else(|| anyhow!("no such media: {media_id}"))?;
        self.ce.get_object(&m.object_cid).await
    }

    // ----- posts / threads / comments -----

    pub async fn post_create(&self, req: proto::PostCreateReq) -> Result<proto::IdResp> {
        self.call(proto::T_POST_CREATE, &req).await
    }

    pub async fn post_get(&self, id: &str) -> Result<proto::PostResp> {
        self.call(proto::T_POST_GET, &proto::PostGetReq { id: id.into() }).await
    }

    pub async fn threads(&self, req: proto::ThreadsReq) -> Result<proto::ThreadsResp> {
        self.call(proto::T_THREADS, &req).await
    }

    pub async fn comments(&self, req: proto::CommentsReq) -> Result<proto::CommentsResp> {
        self.call(proto::T_COMMENTS, &req).await
    }

    // ----- vote / follow -----

    pub async fn vote(&self, target: &str, value: i8) -> Result<proto::OkResp> {
        self.call(proto::T_VOTE, &proto::VoteReq { target: target.into(), value }).await
    }

    /// Upvote a post/comment/document.
    pub async fn upvote(&self, target: &str) -> Result<proto::OkResp> {
        self.vote(target, 1).await
    }
    /// Downvote.
    pub async fn downvote(&self, target: &str) -> Result<proto::OkResp> {
        self.vote(target, -1).await
    }
    /// Clear your vote.
    pub async fn unvote(&self, target: &str) -> Result<proto::OkResp> {
        self.vote(target, 0).await
    }

    pub async fn follow(&self, followee: &str, active: bool) -> Result<proto::OkResp> {
        self.call(proto::T_FOLLOW, &proto::FollowReq { followee: followee.into(), active }).await
    }
    /// Stop following.
    pub async fn unfollow(&self, followee: &str) -> Result<proto::OkResp> {
        self.follow(followee, false).await
    }

    /// Publish this device's consent to belong to `owner` (the device half of the mutual binding
    /// that lets the owner's profile roll up this device's compute trust). Call from the device.
    pub async fn link_device(&self, owner: &str, active: bool) -> Result<proto::OkResp> {
        self.call(proto::T_DEVICE_LINK, &proto::DeviceLinkReq { owner: owner.into(), active }).await
    }
    /// Revoke this device's link to `owner`.
    pub async fn unlink_device(&self, owner: &str) -> Result<proto::OkResp> {
        self.link_device(owner, false).await
    }

    // ----- karma / trust -----

    /// Personalized web-of-trust ranks from `viewer`'s perspective (feed personalization). Pass an
    /// explicit `nodes` list to score those, or empty to get the viewer's top-ranked nodes.
    pub async fn personal_trust(
        &self,
        viewer: &str,
        nodes: Vec<String>,
        limit: usize,
    ) -> Result<proto::PersonalTrustResp> {
        self.call(proto::T_TRUST_GRAPH, &proto::PersonalTrustReq { viewer: viewer.into(), nodes, limit })
            .await
    }

    pub async fn karma(&self, node_id: &str) -> Result<proto::KarmaResp> {
        self.call(proto::T_KARMA, &proto::KarmaReq { node_id: node_id.into() }).await
    }

    /// The full fused [`trana_core::karma::TrustScore`] for a node (social + compute + web-of-trust).
    pub async fn trust_score(&self, node_id: &str) -> Result<trana_core::karma::TrustScore> {
        Ok(self.karma(node_id).await?.trust)
    }

    /// Just the fused trust in 0.0–1.0 — "how much should I trust this node?".
    pub async fn trust(&self, node_id: &str) -> Result<f64> {
        Ok(self.karma(node_id).await?.trust.combined)
    }

    // ----- streams -----

    pub async fn stream_start(&self, req: proto::StreamStartReq) -> Result<proto::IdResp> {
        self.call(proto::T_STREAM_START, &req).await
    }

    pub async fn stream_append(&self, req: proto::StreamAppendReq) -> Result<proto::IdResp> {
        self.call(proto::T_STREAM_APPEND, &req).await
    }

    pub async fn stream_end(&self, req: proto::StreamEndReq) -> Result<proto::OkResp> {
        self.call(proto::T_STREAM_END, &req).await
    }

    pub async fn stream_get(&self, id: &str) -> Result<proto::StreamResp> {
        self.call(proto::T_STREAM_GET, &proto::StreamGetReq { id: id.into() }).await
    }

    pub async fn streams_live(&self) -> Result<proto::StreamsLiveResp> {
        self.call::<(), _>(proto::T_STREAMS_LIVE, &()).await
    }

    // ----- community governance -----

    pub async fn board_put(&self, req: proto::BoardPutReq) -> Result<proto::IdResp> {
        self.call(proto::T_BOARD_PUT, &req).await
    }

    pub async fn board_get(&self, board: &str) -> Result<proto::BoardResp> {
        self.call(proto::T_BOARD_GET, &proto::BoardGetReq { board: board.into() }).await
    }

    pub async fn boards(&self) -> Result<proto::BoardsResp> {
        self.call::<(), _>(proto::T_BOARDS, &()).await
    }

    /// A feed ranked by any algorithm (hot, top, new, best, trending, rising, controversial).
    pub async fn feed(&self, req: proto::FeedReq) -> Result<proto::ThreadsResp> {
        self.call(proto::T_FEED, &req).await
    }

    /// Cast a community ban vote (`support = true` to ban, `false` to keep).
    pub async fn ban_vote(&self, board: &str, target: &str, support: bool, reason: &str) -> Result<proto::OkResp> {
        self.call(
            proto::T_BANVOTE,
            &proto::BanVoteReq { board: board.into(), target: target.into(), support, reason: reason.into() },
        )
        .await
    }

    pub async fn ban_standing(&self, board: &str, target: &str) -> Result<proto::BanStandingResp> {
        self.call(proto::T_BANSTANDING, &proto::BanStandingReq { board: board.into(), target: target.into() }).await
    }

    pub async fn policy_propose(&self, req: proto::PolicyProposeReq) -> Result<proto::IdResp> {
        self.call(proto::T_POLICY_PROPOSE, &req).await
    }

    pub async fn policy_vote(&self, proposal: &str, support: bool) -> Result<proto::OkResp> {
        self.call(proto::T_POLICY_VOTE, &proto::PolicyVoteReq { proposal: proposal.into(), support }).await
    }

    pub async fn proposals(&self, board: Option<&str>) -> Result<proto::ProposalsResp> {
        self.call(proto::T_PROPOSALS, &proto::ProposalsReq { board: board.map(|s| s.to_string()) }).await
    }

    pub async fn proposal(&self, id: &str) -> Result<proto::ProposalResp> {
        self.call(proto::T_PROPOSAL_GET, &proto::ProposalGetReq { id: id.into() }).await
    }

    // ----- documents + versioning -----

    /// Create a document or publish a new version (set `series` + `prev`). For a file/PDF/binary,
    /// upload the bytes with [`upload`](Self::upload) first and pass a `file`.
    pub async fn document_put(&self, req: proto::DocPutReq) -> Result<proto::IdResp> {
        self.call(proto::T_DOC_PUT, &req).await
    }

    pub async fn document_get(&self, id: &str) -> Result<proto::DocResp> {
        self.call(proto::T_DOC_GET, &proto::DocGetReq { id: id.into() }).await
    }

    /// Full version history of a document (pass any version id or the series id), oldest -> newest.
    pub async fn document_history(&self, key: &str) -> Result<proto::DocsResp> {
        self.call(proto::T_DOC_HISTORY, &proto::DocKeyReq { key: key.into() }).await
    }

    pub async fn document_latest(&self, key: &str) -> Result<proto::DocResp> {
        self.call(proto::T_DOC_LATEST, &proto::DocKeyReq { key: key.into() }).await
    }

    /// A unified line diff between two document versions.
    pub async fn document_diff(&self, from: &str, to: &str) -> Result<proto::DocDiffResp> {
        self.call(proto::T_DOC_DIFF, &proto::DocDiffReq { from: from.into(), to: to.into() }).await
    }

    pub async fn documents_by(&self, author: &str) -> Result<proto::DocsResp> {
        self.call(proto::T_DOCS_BY, &proto::DocsByReq { author: author.into() }).await
    }

    /// `trana://...` URIs that reference content `id` (backlinks).
    pub async fn backlinks(&self, id: &str) -> Result<proto::BacklinksResp> {
        self.call(proto::T_BACKLINKS, &proto::BacklinksReq { id: id.into() }).await
    }

    /// Resolve a `trana://<kind>/<id>` reference to its content (the typed get for that kind).
    /// Returns the raw JSON value so a caller can handle any kind uniformly.
    pub async fn resolve(&self, uri: &str) -> Result<serde_json::Value> {
        use trana_core::model::RefKind;
        let r = trana_core::Ref::parse(uri).ok_or_else(|| anyhow!("not a trana:// ref: {uri}"))?;
        match r.kind {
            RefKind::Post => Ok(serde_json::to_value(self.post_get(&r.id).await?)?),
            RefKind::Document => Ok(serde_json::to_value(self.document_get(&r.id).await?)?),
            RefKind::Media => Ok(serde_json::to_value(self.media_get(&r.id).await?)?),
            RefKind::Stream => Ok(serde_json::to_value(self.stream_get(&r.id).await?)?),
            RefKind::Profile => Ok(serde_json::to_value(self.profile_get(&r.id).await?)?),
            RefKind::Board => Ok(serde_json::to_value(self.board_get(&r.id).await?)?),
            RefKind::Blob => Ok(serde_json::json!({ "cid": r.id })),
        }
    }
}
