//! The mesh service: a [`ce_rs::serve::Handler`] that dispatches every `trana/*` RPC topic.
//!
//! The serve loop hands each request to [`TranaService::handle`] with the **authenticated** sender
//! (`req.from`). That sender is the `author` for any write — a caller can only ever act as itself, so
//! authorship needs no separate proof. Every reply is a JSON [`Envelope`]; errors come back as
//! `{ok:false, error}` rather than a dropped connection, so a client's `request` never times out on a
//! handled-but-failed call.

use ce_rs::serve::{Handler, Request};
use std::sync::Arc;
use trana_core::proto::{self, Envelope};

use crate::engine::Engine;

/// The mesh-facing service. Cheap to clone-share via the inner `Arc`.
#[derive(Clone)]
pub struct TranaService {
    engine: Arc<Engine>,
}

impl TranaService {
    pub fn new(engine: Arc<Engine>) -> Self {
        TranaService { engine }
    }

    /// Dispatch one request to the right engine method, returning the reply envelope bytes.
    async fn dispatch(&self, req: Request) -> Envelope {
        // Resolve the effective author: the authenticated sender, or a delegated identity if the
        // request carries a valid ce-cap "act-as" capability. A bad/forged delegation is rejected
        // here, before any handler runs.
        let from_owned = match self.engine.resolve_author(&req.from, &req.payload).await {
            Ok(f) => f,
            Err(e) => return Envelope::err(e.to_string()),
        };
        let from = from_owned.as_str();
        let p = req.payload.as_slice();
        match req.topic.as_str() {
            proto::T_PROFILE_PUT => self.write_reply(parse(p).map(|r| self.engine.profile_put(from, r))).await,
            proto::T_PROFILE_GET => match parse::<proto::ProfileGetReq>(p) {
                Ok(r) => env(self.engine.profile_get(&r.node_id).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_MEDIA_PUT => self.write_reply(parse(p).map(|r| self.engine.media_put(from, r))).await,
            proto::T_MEDIA_GET => match parse::<proto::MediaGetReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.media_get(&r.media_id)),
                Err(e) => Envelope::err(e),
            },
            proto::T_POST_CREATE => self.write_reply(parse(p).map(|r| self.engine.post_create(from, r))).await,
            proto::T_POST_GET => match parse::<proto::PostGetReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.post_get(&r.id)),
                Err(e) => Envelope::err(e),
            },
            proto::T_THREADS => match parse::<proto::ThreadsReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.threads(r)),
                Err(e) => Envelope::err(e),
            },
            proto::T_COMMENTS => match parse::<proto::CommentsReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.comments(r)),
                Err(e) => Envelope::err(e),
            },
            proto::T_VOTE => match parse::<proto::VoteReq>(p) {
                Ok(r) => env(self.engine.vote(from, r).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_FOLLOW => match parse::<proto::FollowReq>(p) {
                Ok(r) => env(self.engine.follow(from, r).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_KARMA => match parse::<proto::KarmaReq>(p) {
                Ok(r) => env(self.engine.karma(&r.node_id).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_STREAM_START => self.write_reply(parse(p).map(|r| self.engine.stream_start(from, r))).await,
            proto::T_STREAM_APPEND => self.write_reply(parse(p).map(|r| self.engine.stream_append(from, r))).await,
            proto::T_STREAM_END => match parse::<proto::StreamEndReq>(p) {
                Ok(r) => env(self.engine.stream_end(from, r).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_STREAM_GET => match parse::<proto::StreamGetReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.stream_get(&r.id)),
                Err(e) => Envelope::err(e),
            },
            proto::T_STREAMS_LIVE => Envelope::ok(&self.engine.streams_live()),
            // ----- community governance -----
            proto::T_BOARD_PUT => self.write_reply(parse(p).map(|r| self.engine.board_put(from, r))).await,
            proto::T_BOARD_GET => match parse::<proto::BoardGetReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.board_get(&r.board)),
                Err(e) => Envelope::err(e),
            },
            proto::T_BOARDS => Envelope::ok(&self.engine.boards()),
            proto::T_FEED => match parse::<proto::FeedReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.feed(r)),
                Err(e) => Envelope::err(e),
            },
            proto::T_BANVOTE => match parse::<proto::BanVoteReq>(p) {
                Ok(r) => env(self.engine.ban_vote(from, r).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_BANSTANDING => match parse::<proto::BanStandingReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.ban_standing(&r.board, &r.target)),
                Err(e) => Envelope::err(e),
            },
            proto::T_POLICY_PROPOSE => self.write_reply(parse(p).map(|r| self.engine.policy_propose(from, r))).await,
            proto::T_POLICY_VOTE => match parse::<proto::PolicyVoteReq>(p) {
                Ok(r) => env(self.engine.policy_vote(from, r).await),
                Err(e) => Envelope::err(e),
            },
            proto::T_PROPOSALS => match parse::<proto::ProposalsReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.proposals(r.board.as_deref())),
                Err(e) => Envelope::err(e),
            },
            proto::T_PROPOSAL_GET => match parse::<proto::ProposalGetReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.proposal_get(&r.id)),
                Err(e) => Envelope::err(e),
            },
            // ----- documents + versioning -----
            proto::T_DOC_PUT => self.write_reply(parse(p).map(|r| self.engine.document_put(from, r))).await,
            proto::T_DOC_GET => match parse::<proto::DocGetReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.document_get(&r.id)),
                Err(e) => Envelope::err(e),
            },
            proto::T_DOCS_BY => match parse::<proto::DocsByReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.documents_by(&r.author)),
                Err(e) => Envelope::err(e),
            },
            proto::T_BACKLINKS => match parse::<proto::BacklinksReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.backlinks(&r.id)),
                Err(e) => Envelope::err(e),
            },
            proto::T_DOC_HISTORY => match parse::<proto::DocKeyReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.document_history(&r.key)),
                Err(e) => Envelope::err(e),
            },
            proto::T_DOC_LATEST => match parse::<proto::DocKeyReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.document_latest(&r.key)),
                Err(e) => Envelope::err(e),
            },
            proto::T_DOC_DIFF => match parse::<proto::DocDiffReq>(p) {
                Ok(r) => Envelope::ok(&self.engine.document_diff(&r.from, &r.to)),
                Err(e) => Envelope::err(e),
            },
            proto::T_REPLICATE => Envelope::ok(&self.engine.replicate(p).await),
            other => Envelope::err(format!("unknown topic: {other}")),
        }
    }

    /// Await a write future (already parsed) and wrap its result in an envelope.
    async fn write_reply<F, T>(&self, parsed: Result<F, String>) -> Envelope
    where
        F: std::future::Future<Output = anyhow::Result<T>>,
        T: serde::Serialize,
    {
        match parsed {
            Ok(fut) => env(fut.await),
            Err(e) => Envelope::err(e),
        }
    }
}

impl Handler for TranaService {
    async fn handle(&self, req: Request) -> Vec<u8> {
        let topic = req.topic.clone();
        let env = self.dispatch(req).await;
        if !env.ok {
            tracing::debug!(topic = %topic, error = ?env.error, "trana: request failed");
        }
        env.encode()
    }
}

/// Parse a JSON request body, mapping decode errors to a readable string.
fn parse<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("bad request body: {e}"))
}

/// Wrap an `anyhow::Result` into an [`Envelope`].
fn env<T: serde::Serialize>(r: anyhow::Result<T>) -> Envelope {
    match r {
        Ok(v) => Envelope::ok(&v),
        Err(e) => Envelope::err(e.to_string()),
    }
}
