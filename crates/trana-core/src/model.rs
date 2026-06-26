//! The content model: every kind of thing a user can publish to trana.
//!
//! All of these are carried inside a [`crate::Record`]. They are plain data — no behaviour — so the
//! wire format is stable and the types compile cleanly to `wasm32`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The payload of a [`crate::Record`]. One variant per kind of publishable thing.
///
/// Mutable kinds (profile, vote, follow) are **last-write-wins per author/subject** — a newer record
/// supersedes an older one. Append kinds (media, post, stream segments) are **immutable** — each is a
/// distinct content-addressed object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Body {
    /// Create or update the author's profile (last-write-wins).
    Profile(Profile),
    /// Register a content-addressed media object (image/video/audio/podcast/document).
    Media(Media),
    /// A thread root (when `parent` is `None`) or a comment (when `parent` is set).
    Post(Post),
    /// Up/down/clear a vote on a target record (last-write-wins per voter+target).
    Vote(Vote),
    /// Follow or unfollow another user (last-write-wins per follower+followee).
    Follow(Follow),
    /// Begin a live stream; this record's id becomes the stable stream id.
    StreamStart(StreamStart),
    /// Append one content-addressed segment to a live stream (HLS-like growing playlist).
    StreamSegment(StreamSegment),
    /// End a live stream, optionally publishing a full recording object.
    StreamEnd(StreamEnd),
    /// A standalone markdown document that can reference any content (recursively).
    Document(Document),
    /// Register a board (a community namespace) and its community params. First claim sets it.
    BoardCreate(BoardCreate),
    /// A community vote to ban (or keep) a user in a board. No mods — the community decides.
    BanVote(BanVote),
    /// Propose a community policy (the substrate future AI enforcement applies). Community-voted.
    PolicyProposal(PolicyProposal),
    /// Vote a policy proposal up or down.
    PolicyVote(PolicyVote),
    /// A device's signed confirmation that it belongs to an owner (last-write-wins per device).
    DeviceLink(DeviceLink),
}

impl Body {
    /// A short, stable discriminator (`"profile"`, `"post"`, ...). Handy for indexing/logging.
    pub fn kind(&self) -> &'static str {
        match self {
            Body::Profile(_) => "profile",
            Body::Media(_) => "media",
            Body::Post(_) => "post",
            Body::Vote(_) => "vote",
            Body::Follow(_) => "follow",
            Body::StreamStart(_) => "stream_start",
            Body::StreamSegment(_) => "stream_segment",
            Body::StreamEnd(_) => "stream_end",
            Body::Document(_) => "document",
            Body::BoardCreate(_) => "board_create",
            Body::BanVote(_) => "ban_vote",
            Body::PolicyProposal(_) => "policy_proposal",
            Body::PolicyVote(_) => "policy_vote",
            Body::DeviceLink(_) => "device_link",
        }
    }
}

/// A device's signed confirmation that it belongs to `owner`. The record **author is the device**,
/// so this is the device's own consent. Pairing it with the owner's [`Profile::devices`] listing
/// that device is a *mutual* binding: a profile cannot roll a high-reputation node's compute into its
/// own trust by merely naming it — the device must also point back. Last-write-wins per device, so a
/// device has at most one current owner and can unlink with `active = false`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceLink {
    /// The NodeId this device declares it is owned by.
    pub owner: String,
    /// Whether the link is currently asserted (`false` unlinks).
    #[serde(default = "crate::model::default_true")]
    pub active: bool,
}

/// serde default for boolean fields that should default to `true`.
pub(crate) fn default_true() -> bool {
    true
}

/// A user's profile. The user is the record author (a NodeId); a person may own several devices, so
/// `devices` lists the other NodeIds they control — their aggregate compute capacity sums across all
/// of them in [`crate::karma`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    /// Optional human handle (e.g. an on-chain claimed name). Display only; not unique-enforced here.
    #[serde(default)]
    pub handle: Option<String>,
    /// Display name.
    #[serde(default)]
    pub display_name: String,
    /// Free-form bio (markdown).
    #[serde(default)]
    pub bio: String,
    /// Avatar image, by media record id.
    #[serde(default)]
    pub avatar: Option<MediaRef>,
    /// External links (website, socials, ...).
    #[serde(default)]
    pub links: Vec<Link>,
    /// Other NodeIds (devices) this user owns — their compute capacity rolls up into the profile.
    #[serde(default)]
    pub devices: Vec<String>,
}

/// A labelled external link on a profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub label: String,
    pub url: String,
}

/// What a piece of media is. Drives default handling/transcoding hints in the node and players.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Image,
    Video,
    Audio,
    Podcast,
    Document,
}

/// A content-addressed media object. The bytes themselves live in CE's blob/object store (uploaded
/// with `put_object`, which chunks + content-addresses them); this record is the *descriptor* that
/// names them and carries metadata. Keeping bytes and descriptor separate lets the (large) bytes be
/// replicated/pinned independently of the (tiny) social graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Media {
    pub kind: MediaKind,
    /// CE object CID of the bytes (the manifest hash returned by `put_object`).
    pub object_cid: String,
    /// MIME type, e.g. `video/mp4`, `image/png`, `audio/mpeg`, `application/pdf`.
    pub mime: String,
    /// Total byte size of the object.
    pub size: u64,
    /// Human title.
    #[serde(default)]
    pub title: String,
    /// Duration for time-based media (video/audio/podcast), in milliseconds.
    #[serde(default)]
    pub duration_ms: Option<u64>,
    /// Pixel dimensions for visual media.
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    /// Optional poster/thumbnail image, by media record id.
    #[serde(default)]
    pub thumbnail: Option<MediaRef>,
    /// Arbitrary extra metadata (codec, podcast episode no, captions cid, ...).
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// A reference to a [`Media`] record by its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaRef {
    pub media_id: String,
}

impl MediaRef {
    pub fn new(media_id: impl Into<String>) -> Self {
        MediaRef { media_id: media_id.into() }
    }
}

/// A post: a thread root or a comment. Reddit-like — boards contain threads, threads contain a tree
/// of comments. `parent == None` is a root; `parent == Some(id)` is a reply to that post.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Post {
    /// The board (subreddit-like namespace) this post belongs to.
    pub board: String,
    /// Parent post id; `None` for a thread root.
    #[serde(default)]
    pub parent: Option<String>,
    /// Title (thread roots). Comments usually omit it.
    #[serde(default)]
    pub title: Option<String>,
    /// Body text (markdown).
    #[serde(default)]
    pub body: String,
    /// Attached media (images in a gallery, a video, an audio clip, ...).
    #[serde(default)]
    pub media: Vec<MediaRef>,
}

impl Post {
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }
}

/// A vote on a target record (a post or comment). `value` is `+1`, `-1`, or `0` to clear. Voting is
/// the karma primitive; the voter is the record author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    /// Target record id (the post/comment being voted on).
    pub target: String,
    /// `+1` up, `-1` down, `0` clears the vote.
    pub value: i8,
}

/// Follow or unfollow another user. The follower is the record author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Follow {
    /// The NodeId being followed.
    pub followee: String,
    /// `true` follow, `false` unfollow.
    pub active: bool,
}

/// What a live stream carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Video,
    Audio,
}

/// Begin a live stream. The id of the record carrying this body is the **stream id** that segments
/// and the end-record reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamStart {
    pub title: String,
    pub kind: StreamKind,
    /// Optional board to also surface the stream in.
    #[serde(default)]
    pub board: Option<String>,
    /// Optional poster image, by media record id.
    #[serde(default)]
    pub thumbnail: Option<MediaRef>,
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// One segment of a live stream: a short, content-addressed media chunk. Players poll segments in
/// ascending `seq` until the stream ends — a fully distributed, growing HLS-like playlist.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSegment {
    /// The stream id (the `StreamStart` record id).
    pub stream: String,
    /// Monotonic sequence number, starting at 0.
    pub seq: u64,
    /// CE object CID of the segment bytes.
    pub object_cid: String,
    /// Segment duration in milliseconds.
    pub duration_ms: u64,
}

/// End a live stream. Optionally publishes a single recording object (the VOD) for replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEnd {
    pub stream: String,
    /// Optional CE object CID of the full recording (for replay after the live edge ends).
    #[serde(default)]
    pub recording_cid: Option<String>,
}

// =============================== content addressing ===============================
//
// trana cannot use HTTP URLs — content lives on the mesh, addressed by content hash / NodeId, served
// by whichever nodes hold it. So references use a clean mesh-native scheme: `trana://<kind>/<id>`.
//
//   trana://post/<record-id>       a post or comment (record id = sha256 of its canonical body)
//   trana://document/<record-id>   a document (markdown artifact)
//   trana://media/<record-id>      a media descriptor (image/video/audio/podcast/document file)
//   trana://stream/<record-id>     a live stream
//   trana://profile/<node-id>      a user profile (addressed by their NodeId)
//   trana://board/<name>           a board (community namespace)
//   trana://blob/<cid>             a raw content-addressed object (the bytes themselves)
//
// `id` is already a content address for everything except profile (NodeId) and board (name), both of
// which are stable identifiers. A reference is resolved by calling the matching mesh RPC — never an
// HTTP fetch — so the same `trana://` link resolves from any node, browser, or phone on the mesh.

/// What a [`Ref`] points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefKind {
    Post,
    Document,
    Media,
    Stream,
    Profile,
    Board,
    Blob,
}

impl RefKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RefKind::Post => "post",
            RefKind::Document => "document",
            RefKind::Media => "media",
            RefKind::Stream => "stream",
            RefKind::Profile => "profile",
            RefKind::Board => "board",
            RefKind::Blob => "blob",
        }
    }

    pub fn parse(s: &str) -> Option<RefKind> {
        Some(match s {
            "post" => RefKind::Post,
            "document" | "doc" => RefKind::Document,
            "media" => RefKind::Media,
            "stream" => RefKind::Stream,
            "profile" => RefKind::Profile,
            "board" => RefKind::Board,
            "blob" => RefKind::Blob,
            _ => return None,
        })
    }
}

/// A mesh-native content reference: `trana://<kind>/<id>`. The unit of "this post/document references
/// that content," resolvable over the mesh from anywhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ref {
    pub kind: RefKind,
    pub id: String,
}

impl Ref {
    pub fn new(kind: RefKind, id: impl Into<String>) -> Ref {
        Ref { kind, id: id.into() }
    }
    pub fn post(id: impl Into<String>) -> Ref {
        Ref::new(RefKind::Post, id)
    }
    pub fn document(id: impl Into<String>) -> Ref {
        Ref::new(RefKind::Document, id)
    }
    pub fn media(id: impl Into<String>) -> Ref {
        Ref::new(RefKind::Media, id)
    }
    pub fn stream(id: impl Into<String>) -> Ref {
        Ref::new(RefKind::Stream, id)
    }
    pub fn profile(node_id: impl Into<String>) -> Ref {
        Ref::new(RefKind::Profile, node_id)
    }
    pub fn board(name: impl Into<String>) -> Ref {
        Ref::new(RefKind::Board, name)
    }
    pub fn blob(cid: impl Into<String>) -> Ref {
        Ref::new(RefKind::Blob, cid)
    }

    /// The canonical `trana://<kind>/<id>` URI.
    pub fn to_uri(&self) -> String {
        format!("trana://{}/{}", self.kind.as_str(), self.id)
    }

    /// Parse a `trana://<kind>/<id>` URI. Returns `None` if it is not a well-formed trana ref.
    pub fn parse(uri: &str) -> Option<Ref> {
        let rest = uri.strip_prefix("trana://")?;
        let (kind, id) = rest.split_once('/')?;
        let kind = RefKind::parse(kind)?;
        if id.is_empty() {
            return None;
        }
        Some(Ref { kind, id: id.to_string() })
    }
}

/// Extract every `trana://...` reference embedded in a markdown body, in order, de-duplicated. This
/// is how a markdown post/document "references any content": you write `trana://post/<id>` links and
/// the backend can index them — no HTTP, no out-of-band metadata.
pub fn extract_refs(markdown: &str) -> Vec<Ref> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let bytes = markdown.as_bytes();
    let mut i = 0;
    while let Some(pos) = markdown[i..].find("trana://") {
        let start = i + pos;
        // Read until a character that can't be part of the URI (markdown/whitespace/closers).
        let mut end = start;
        while end < bytes.len() {
            let c = bytes[end] as char;
            if c.is_whitespace() || matches!(c, ')' | ']' | '>' | '"' | '\'' | '`' | '|') {
                break;
            }
            end += 1;
        }
        let token = markdown[start..end].trim_end_matches(|c: char| matches!(c, '.' | ',' | ';' | ':'));
        if let Some(r) = Ref::parse(token) {
            if seen.insert(r.to_uri()) {
                out.push(r);
            }
        }
        i = end.max(start + 1);
    }
    out
}

/// A binary/file payload for a document version — a PDF, image, archive, dataset, anything. The bytes
/// live in the CE object store (content-addressed); this names them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRef {
    pub object_cid: String,
    pub mime: String,
    pub size: u64,
    #[serde(default)]
    pub name: String,
}

/// A document: a **versioned** artifact — markdown *or* a binary file (PDF/dataset/...) — that can
/// reference any other content (recursively). Addressed by its record id (`trana://document/<id>`),
/// votable and karma-bearing.
///
/// Versioning is content-addressed and git-like: each edit is a new record whose `prev` points at the
/// version it supersedes and whose `series` is the stable id of the first version. The chain of
/// versions is a Merkle DAG — every version is immutable and verifiable, history can never be
/// silently rewritten, and the latest is just the tip. (A mirror into an actual ce-hub git repo is
/// the interop layer on top.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    pub title: String,
    /// Markdown body (empty for a pure-file document). May embed `trana://...` references inline.
    #[serde(default)]
    pub body: String,
    /// A binary/file payload, when this document *is* a file (PDF, image, dataset, ...).
    #[serde(default)]
    pub file: Option<FileRef>,
    /// Explicit references this document declares (in addition to any inline in `body`).
    #[serde(default)]
    pub refs: Vec<Ref>,
    /// Optional board to surface the document in.
    #[serde(default)]
    pub board: Option<String>,
    /// Stable series id (the first version's record id). `None` means this record *starts* a new
    /// series and its own id becomes the series id.
    #[serde(default)]
    pub series: Option<String>,
    /// The record id of the version this supersedes (`None` for the first version).
    #[serde(default)]
    pub prev: Option<String>,
}

// =============================== community governance ===============================
//
// trana has NO moderators. Governance is the community itself: trust-weighted up/down voting,
// trust + respect, and — the deliberate inversion of Reddit — a *grace window* that gives new and
// controversial content MORE visibility first (a chance to persuade, or to find the people who
// already agree). Only SUSTAINED rejection, decided by a trust-weighted community `BanVote` that
// meets quorum, removes a user from a community. Policies are proposed and voted forward by people
// ([`PolicyProposal`]/[`PolicyVote`]); a future AI layer enforces the policies the community passed.
// The trust-weighting and quorum math live in the node (it knows each voter's fused trust); the
// read-model carries the raw votes + the community params so any node converges to the same view.

/// Per-board community parameters (not moderator powers — there are none). Carried in the read-model
/// so every node applies the same rules.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoardPolicy {
    /// Visibility grace window for a new post, in seconds: while inside it the feed surfaces the post
    /// by ENGAGEMENT and recency (downvotes count as engagement, so controversy gets *seen*), not by
    /// net score. After it, reception decides. This is the "give it a chance" window.
    #[serde(default = "default_grace_secs")]
    pub grace_secs: u64,
    /// Trust-weighted net support fraction (0.0–1.0 of participating trust) required to ban a user.
    #[serde(default = "default_ban_support")]
    pub ban_support: f64,
    /// Minimum number of distinct ban-voters before a ban can take effect (anti-brigading quorum).
    #[serde(default = "default_ban_quorum")]
    pub ban_quorum: u32,
    /// Minimum fused trust (0.0–1.0) to create a post/comment here. 0 = open. (Node-enforced.)
    #[serde(default)]
    pub min_trust_to_post: f64,
    /// Minimum fused trust (0.0–1.0) to vote here. 0 = open. (Node-enforced; sybil resistance.)
    #[serde(default)]
    pub min_trust_to_vote: f64,
}

fn default_grace_secs() -> u64 {
    6 * 3600
}
fn default_ban_support() -> f64 {
    0.66
}
fn default_ban_quorum() -> u32 {
    10
}

impl Default for BoardPolicy {
    fn default() -> Self {
        BoardPolicy {
            grace_secs: default_grace_secs(),
            ban_support: default_ban_support(),
            ban_quorum: default_ban_quorum(),
            min_trust_to_post: 0.0,
            min_trust_to_vote: 0.0,
        }
    }
}

/// Register a board (a community namespace) and its params. First claim of a name sets it; there is
/// no owner with special powers — only the namespace + community parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoardCreate {
    pub board: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub policy: BoardPolicy,
}

/// A community member's vote on whether a user should be banned from a board. `support = true` votes
/// to ban, `false` votes to keep. The voter is the record author; one (latest) vote per voter+target.
/// A ban takes effect only when trust-weighted support clears `BoardPolicy::ban_support` AND quorum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BanVote {
    pub board: String,
    /// The NodeId the community is voting on.
    pub target: String,
    /// `true` = ban, `false` = keep / lift.
    pub support: bool,
    #[serde(default)]
    pub reason: String,
}

/// A community policy proposal — the substrate the future AI policy layer enforces. The proposal id
/// is the record id. Scoped to a board, or global when `board` is empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyProposal {
    #[serde(default)]
    pub board: Option<String>,
    pub title: String,
    /// The policy text (human-readable; an AI layer will later parse + enforce it).
    pub body: String,
}

/// A vote on a [`PolicyProposal`] (by proposal record id). One latest vote per voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyVote {
    pub proposal: String,
    /// `true` = in favor, `false` = against.
    pub support: bool,
}
