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
    /// Claim/configure a board. First claim of a name wins; its author is the board owner.
    BoardCreate(BoardCreate),
    /// Owner grants or revokes a moderator on a board.
    ModGrant(ModGrant),
    /// A moderator/owner action on a board (remove, lock, pin, ban, ...).
    ModAction(ModAction),
    /// A user reports a post/comment/user for moderator attention.
    Report(Report),
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
            Body::BoardCreate(_) => "board_create",
            Body::ModGrant(_) => "mod_grant",
            Body::ModAction(_) => "mod_action",
            Body::Report(_) => "report",
        }
    }
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

// =============================== governance / moderation ===============================
//
// trana's moderation is HYBRID and capability-shaped, with NO global admin:
//   - a board is an owned object (first claim of a name wins; the author is the owner);
//   - the owner delegates moderators via [`ModGrant`]; mods/owner act via [`ModAction`];
//   - every action is a signed, content-addressed record, so any node verifies the authority
//     chain and honors it by default — while still keeping a local policy override (node
//     sovereignty) and trust-gating who may vote/post (sybil resistance).

/// Per-board policy knobs. Trust thresholds are checked at write time by the node (it knows the
/// caller's fused trust); the read-model carries the policy so any node enforces the same rules.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoardPolicy {
    /// Minimum fused trust (0.0–1.0) required to create a post/comment. 0 = open.
    #[serde(default)]
    pub min_trust_to_post: f64,
    /// Minimum fused trust (0.0–1.0) required to up/down vote. 0 = open.
    #[serde(default)]
    pub min_trust_to_vote: f64,
    /// If true, only the owner and moderators may post (an announce-only board).
    #[serde(default)]
    pub restricted_posting: bool,
}

impl Default for BoardPolicy {
    fn default() -> Self {
        BoardPolicy { min_trust_to_post: 0.0, min_trust_to_vote: 0.0, restricted_posting: false }
    }
}

/// Claim and/or (re)configure a board. The first claim of a board name wins and fixes the owner;
/// later `BoardCreate`s from the **owner** update the title/description/policy (last-write-wins).
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

/// The owner grants or revokes a moderator on a board. Only honored when authored by the board owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModGrant {
    pub board: String,
    /// The NodeId being made (or unmade) a moderator.
    pub moderator: String,
    /// `true` grant, `false` revoke.
    pub active: bool,
}

/// What a moderator/owner does. Each is authorized iff the record author owns or mods the board.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ModActionKind {
    /// Hide a post/comment from listings (reversible).
    RemovePost { post: String },
    /// Un-hide a previously removed post/comment.
    RestorePost { post: String },
    /// Stop a thread accepting new comments.
    LockThread { root: String },
    /// Re-open a locked thread.
    UnlockThread { root: String },
    /// Pin a thread to the top of the board.
    PinThread { root: String },
    /// Unpin a thread.
    UnpinThread { root: String },
    /// Ban a user from the board until `until_ms` (0 = permanent). Their content is hidden and they
    /// cannot post/comment/vote in the board.
    BanUser { user: String, #[serde(default)] until_ms: u64 },
    /// Lift a ban.
    UnbanUser { user: String },
}

/// A moderator/owner action on a board.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModAction {
    pub board: String,
    pub kind: ModActionKind,
    /// Free-text reason / mod note (shown in the mod log).
    #[serde(default)]
    pub reason: String,
}

/// A user-filed report against a post/comment (or a user) for moderator attention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    /// The board the report is filed in (mods of that board see it).
    pub board: String,
    /// The reported record id (a post/comment id) or NodeId (a user).
    pub target: String,
    #[serde(default)]
    pub reason: String,
}
