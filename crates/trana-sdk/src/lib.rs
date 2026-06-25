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
}

impl TranaClient {
    /// Client against the local CE node, auto-locating a trana instance per call.
    pub fn local() -> Self {
        Self::new(CeClient::local())
    }

    /// Client against a given [`CeClient`].
    pub fn new(ce: CeClient) -> Self {
        TranaClient { ce, timeout_ms: TIMEOUT_MS, pinned: None }
    }

    /// Pin every call to one specific trana node id (skip discovery). Useful for tests and for
    /// talking to your own co-located node.
    pub fn pinned(ce: CeClient, node_id: impl Into<String>) -> Self {
        TranaClient { ce, timeout_ms: TIMEOUT_MS, pinned: Some(node_id.into()) }
    }

    /// Override the per-call timeout.
    pub fn with_timeout(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
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
        let payload = serde_json::to_vec(req)?;
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

    pub async fn follow(&self, followee: &str, active: bool) -> Result<proto::OkResp> {
        self.call(proto::T_FOLLOW, &proto::FollowReq { followee: followee.into(), active }).await
    }

    // ----- karma / trust -----

    pub async fn karma(&self, node_id: &str) -> Result<proto::KarmaResp> {
        self.call(proto::T_KARMA, &proto::KarmaReq { node_id: node_id.into() }).await
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
}
