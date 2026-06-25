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
use trana_core::model::{Body, Media, Post, StreamEnd, StreamSegment, StreamStart};
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
