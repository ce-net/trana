//! The trana wire protocol: mesh topic names and the typed request/response payloads on each.
//!
//! trana is a **mesh-native** service: clients reach it by NodeId over libp2p request/reply (CE's
//! `/ce/rpc/1`), never over a stored ip:port or a side HTTP channel. Each logical call is one topic
//! (`trana/<noun>/<verb>/v1`); the request is a JSON-encoded struct from this module, and the reply
//! is an [`Envelope`] wrapping either the typed response or an error string. The node and the SDK
//! share these definitions so they can never drift.
//!
//! Authorship is *not* carried in request bodies — the node uses the authenticated sender (`from`)
//! the mesh hands it, so a caller can only ever act as itself.

use crate::karma::{ComputeTrust, SocialKarma, TrustScore};
use crate::model::{Body, Link, Media, MediaKind, MediaRef, StreamKind};
use crate::state::{PostView, ProfileView, StreamView};
use serde::{Deserialize, Serialize};

/// The DHT service name trana nodes advertise and clients `locate`.
pub const SERVICE: &str = "trana";

/// The pub/sub topic every node gossips accepted records on (and subscribes to for replication).
pub const GOSSIP: &str = "trana/events/v1";

// ----- request/reply topics -----

pub const T_PROFILE_PUT: &str = "trana/profile/put/v1";
pub const T_PROFILE_GET: &str = "trana/profile/get/v1";
pub const T_MEDIA_PUT: &str = "trana/media/put/v1";
pub const T_MEDIA_GET: &str = "trana/media/get/v1";
pub const T_POST_CREATE: &str = "trana/post/create/v1";
pub const T_POST_GET: &str = "trana/post/get/v1";
pub const T_THREADS: &str = "trana/threads/v1";
pub const T_COMMENTS: &str = "trana/comments/v1";
pub const T_VOTE: &str = "trana/vote/v1";
pub const T_FOLLOW: &str = "trana/follow/v1";
pub const T_DEVICE_LINK: &str = "trana/device/link/v1";
pub const T_TRUST_GRAPH: &str = "trana/trust/graph/v1";
pub const T_KARMA: &str = "trana/karma/v1";
pub const T_STREAM_START: &str = "trana/stream/start/v1";
pub const T_STREAM_APPEND: &str = "trana/stream/append/v1";
pub const T_STREAM_END: &str = "trana/stream/end/v1";
pub const T_STREAM_GET: &str = "trana/stream/get/v1";
pub const T_STREAMS_LIVE: &str = "trana/streams/live/v1";
/// Internal replication RPC: "please pull + pin this record/object".
pub const T_REPLICATE: &str = "trana/replicate/v1";

// ----- documents + content addressing + versioning -----
pub const T_DOC_PUT: &str = "trana/document/put/v1";
pub const T_DOC_GET: &str = "trana/document/get/v1";
pub const T_DOCS_BY: &str = "trana/documents/by/v1";
pub const T_BACKLINKS: &str = "trana/backlinks/v1";
pub const T_DOC_HISTORY: &str = "trana/document/history/v1";
pub const T_DOC_LATEST: &str = "trana/document/latest/v1";
pub const T_DOC_DIFF: &str = "trana/document/diff/v1";

// ----- community governance -----
pub const T_BOARD_PUT: &str = "trana/board/put/v1";
pub const T_BOARD_GET: &str = "trana/board/get/v1";
pub const T_BOARDS: &str = "trana/boards/v1";
pub const T_FEED: &str = "trana/feed/v1";
pub const T_BANVOTE: &str = "trana/banvote/v1";
pub const T_BANSTANDING: &str = "trana/banstanding/v1";
pub const T_POLICY_PROPOSE: &str = "trana/policy/propose/v1";
pub const T_POLICY_VOTE: &str = "trana/policy/vote/v1";
pub const T_PROPOSALS: &str = "trana/policy/list/v1";
pub const T_PROPOSAL_GET: &str = "trana/policy/get/v1";

/// Every request/reply topic a node serves (the pub/sub [`GOSSIP`] topic is subscribed separately).
pub const RPC_TOPICS: &[&str] = &[
    T_PROFILE_PUT,
    T_PROFILE_GET,
    T_MEDIA_PUT,
    T_MEDIA_GET,
    T_POST_CREATE,
    T_POST_GET,
    T_THREADS,
    T_COMMENTS,
    T_VOTE,
    T_FOLLOW,
    T_DEVICE_LINK,
    T_TRUST_GRAPH,
    T_KARMA,
    T_STREAM_START,
    T_STREAM_APPEND,
    T_STREAM_END,
    T_STREAM_GET,
    T_STREAMS_LIVE,
    T_REPLICATE,
    T_BOARD_PUT,
    T_BOARD_GET,
    T_BOARDS,
    T_FEED,
    T_BANVOTE,
    T_BANSTANDING,
    T_POLICY_PROPOSE,
    T_POLICY_VOTE,
    T_PROPOSALS,
    T_PROPOSAL_GET,
    T_DOC_PUT,
    T_DOC_GET,
    T_DOCS_BY,
    T_BACKLINKS,
    T_DOC_HISTORY,
    T_DOC_LATEST,
    T_DOC_DIFF,
];

/// The reply envelope on every topic. `ok` distinguishes success (decode `data` as the topic's
/// response type) from failure (read `error`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub data: serde_json::Value,
}

impl Envelope {
    /// A success envelope wrapping `data`.
    pub fn ok<T: Serialize>(data: &T) -> Envelope {
        Envelope {
            ok: true,
            error: None,
            data: serde_json::to_value(data).unwrap_or(serde_json::Value::Null),
        }
    }

    /// A failure envelope carrying `msg`.
    pub fn err(msg: impl Into<String>) -> Envelope {
        Envelope { ok: false, error: Some(msg.into()), data: serde_json::Value::Null }
    }

    /// Encode to JSON bytes for the mesh reply.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Decode an envelope, then its `data` as `T` — or the carried error.
    pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, String> {
        let env: Envelope =
            serde_json::from_slice(bytes).map_err(|e| format!("bad envelope: {e}"))?;
        if !env.ok {
            return Err(env.error.unwrap_or_else(|| "unknown error".into()));
        }
        serde_json::from_value(env.data).map_err(|e| format!("bad response body: {e}"))
    }
}

// ----- profile -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfilePutReq {
    #[serde(default)]
    pub handle: Option<String>,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub bio: String,
    #[serde(default)]
    pub avatar: Option<MediaRef>,
    #[serde(default)]
    pub links: Vec<Link>,
    /// Other NodeIds (devices) this user owns. Their compute capacity rolls into the profile.
    #[serde(default)]
    pub devices: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileGetReq {
    pub node_id: String,
}

/// A profile plus its computed trust — the headline "who is this and can I trust them?" response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileResp {
    pub profile: Option<ProfileView>,
    pub social: SocialKarma,
    pub compute: ComputeTrust,
    pub trust: TrustScore,
}

// ----- media -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaPutReq {
    pub kind: MediaKind,
    /// CE object CID of the already-uploaded bytes (client uploads with `put_object` first).
    pub object_cid: String,
    pub mime: String,
    pub size: u64,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub thumbnail: Option<MediaRef>,
    #[serde(default)]
    pub extra: std::collections::BTreeMap<String, String>,
    /// Desired replica count for the bytes across nearby nodes (0 = node default).
    #[serde(default)]
    pub replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdResp {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaGetReq {
    pub media_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaResp {
    pub media: Option<Media>,
}

// ----- posts / threads / comments -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostCreateReq {
    pub board: String,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub media: Vec<MediaRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostGetReq {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostResp {
    pub post: Option<PostView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadsReq {
    pub board: String,
    #[serde(default = "default_sort")]
    pub sort: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_sort() -> String {
    "hot".into()
}
fn default_limit() -> usize {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadsResp {
    pub threads: Vec<PostView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentsReq {
    pub root: String,
    #[serde(default = "default_sort")]
    pub sort: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentsResp {
    pub comments: Vec<PostView>,
}

// ----- vote / follow -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteReq {
    pub target: String,
    /// +1 / -1 / 0.
    pub value: i8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowReq {
    pub followee: String,
    pub active: bool,
}

/// Publish this device's consent to belong to `owner`. The caller (authenticated sender) is the
/// device; only the device can sign its own link, which is what makes the binding mutual.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceLinkReq {
    /// The NodeId this device declares it belongs to.
    pub owner: String,
    /// Whether the link is active (`false` unlinks). Defaults to `true`.
    #[serde(default = "crate::model::default_true")]
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OkResp {
    pub ok: bool,
}

// ----- karma -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KarmaReq {
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KarmaResp {
    pub social: SocialKarma,
    pub compute: ComputeTrust,
    pub trust: TrustScore,
}

// ----- personalized web-of-trust (feed personalization) -----

/// "How much do I trust these nodes?" from `viewer`'s own perspective — a personalized PageRank that
/// restarts to the viewer, so trust flows out through who they follow. Unlike the global trust used
/// for gates/bans, this is viewer-relative and meant for ranking a personalized feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonalTrustReq {
    pub viewer: String,
    /// Nodes to score. Empty = return the viewer's top-ranked nodes (up to `limit`).
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonalTrustResp {
    /// `(node_id, personalized_rank)` in 0.0–1.0, highest first.
    pub ranks: Vec<(String, f64)>,
}

// ----- streams -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStartReq {
    pub title: String,
    pub kind: StreamKind,
    #[serde(default)]
    pub board: Option<String>,
    #[serde(default)]
    pub thumbnail: Option<MediaRef>,
    #[serde(default)]
    pub extra: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAppendReq {
    pub stream: String,
    pub seq: u64,
    pub object_cid: String,
    pub duration_ms: u64,
    #[serde(default)]
    pub replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEndReq {
    pub stream: String,
    #[serde(default)]
    pub recording_cid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamGetReq {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResp {
    pub stream: Option<StreamView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsLiveResp {
    pub streams: Vec<StreamView>,
}

// ----- replication (internal) -----

/// A replication push: a full record to fold in, plus optional object CIDs the receiver should pull
/// and pin so the bytes live near the content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateReq {
    pub record: crate::record::Record,
    #[serde(default)]
    pub object_cids: Vec<String>,
}

// ----- community governance -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardPutReq {
    pub board: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub policy: crate::model::BoardPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardGetReq {
    pub board: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardResp {
    pub board: crate::state::BoardView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardsResp {
    pub boards: Vec<crate::state::BoardView>,
}

/// A feed request. `scope` is `board` (needs `board`), `all` (cross-board), or `home` (needs
/// `viewer` — thread roots from accounts they follow). `sort` is any feed algorithm name
/// (hot, top, new, best, trending, rising, controversial).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedReq {
    #[serde(default = "default_scope")]
    pub scope: String,
    #[serde(default)]
    pub board: Option<String>,
    #[serde(default)]
    pub viewer: Option<String>,
    #[serde(default = "default_feed_sort")]
    pub sort: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_scope() -> String {
    "all".into()
}
fn default_feed_sort() -> String {
    "hot".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanVoteReq {
    pub board: String,
    pub target: String,
    /// true = vote to ban, false = vote to keep / lift.
    pub support: bool,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanStandingReq {
    pub board: String,
    pub target: String,
}

/// A user's ban standing: the raw community tally plus the node's trust-weighted verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanStandingResp {
    pub standing: crate::state::BanStanding,
    /// Trust-weighted support fraction (0.0–1.0) the node computed.
    pub weighted_support: f64,
    /// The node's verdict after trust weighting.
    pub banned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyProposeReq {
    #[serde(default)]
    pub board: Option<String>,
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyVoteReq {
    pub proposal: String,
    pub support: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalsReq {
    #[serde(default)]
    pub board: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalsResp {
    pub proposals: Vec<crate::state::ProposalView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalGetReq {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalResp {
    pub proposal: Option<crate::state::ProposalView>,
}

// ----- documents + versioning -----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocPutReq {
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub file: Option<crate::model::FileRef>,
    #[serde(default)]
    pub refs: Vec<crate::model::Ref>,
    #[serde(default)]
    pub board: Option<String>,
    /// Series id when publishing a NEW VERSION of an existing document (omit to start a new series).
    #[serde(default)]
    pub series: Option<String>,
    /// The version this supersedes (the current latest id), when publishing a new version.
    #[serde(default)]
    pub prev: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocGetReq {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocResp {
    pub document: Option<crate::state::DocumentView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocsResp {
    pub documents: Vec<crate::state::DocumentView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocsByReq {
    pub author: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocKeyReq {
    /// Any version id or the series id.
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklinksReq {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacklinksResp {
    pub uris: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocDiffReq {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocDiffResp {
    pub diff: Vec<crate::state::DiffLine>,
}

/// Build the [`Body`] for a profile from a put request.
pub fn profile_body(req: ProfilePutReq) -> Body {
    Body::Profile(crate::model::Profile {
        handle: req.handle,
        display_name: req.display_name,
        bio: req.bio,
        avatar: req.avatar,
        links: req.links,
        devices: req.devices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrips_ok_and_err() {
        let env = Envelope::ok(&IdResp { id: "abc".into() });
        let bytes = env.encode();
        let got: IdResp = Envelope::decode(&bytes).unwrap();
        assert_eq!(got.id, "abc");

        let err = Envelope::err("nope").encode();
        let res: Result<IdResp, String> = Envelope::decode(&err);
        assert_eq!(res.unwrap_err(), "nope");
    }

    #[test]
    fn rpc_topics_are_unique() {
        let mut set = std::collections::HashSet::new();
        for t in RPC_TOPICS {
            assert!(set.insert(*t), "duplicate topic {t}");
        }
    }
}
