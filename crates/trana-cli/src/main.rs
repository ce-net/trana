//! `trana` — a command-line client for the trana backend.
//!
//! Built for seeding and managing content: set your profile, post to boards, upload media (images,
//! video, audio, podcasts, documents), run live streams, vote, follow, and read trust/karma. It is a
//! thin shell over [`trana_sdk::TranaClient`]; every write is authored by your local CE node.

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use trana_core::model::{MediaKind, MediaRef, StreamKind};
use trana_core::proto::*;
use trana_sdk::TranaClient;

#[derive(Parser)]
#[command(name = "trana", about = "trana CLI — distributed social/content + trust profiles")]
struct Cli {
    /// Local CE node HTTP API base URL.
    #[arg(long, default_value = ce_rs::DEFAULT_BASE_URL)]
    node_url: String,

    /// Pin all calls to a specific trana node id (skip mesh discovery).
    #[arg(long)]
    node: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Set the local node's profile.
    ProfileSet {
        #[arg(long)]
        display_name: String,
        #[arg(long, default_value = "")]
        bio: String,
        #[arg(long)]
        handle: Option<String>,
        /// Extra NodeIds (devices) you own; their compute capacity rolls into your profile.
        #[arg(long = "device")]
        devices: Vec<String>,
        /// Avatar media id.
        #[arg(long)]
        avatar: Option<String>,
    },
    /// Show a profile + trust (defaults to the local node).
    Profile { node_id: Option<String> },
    /// Show karma + compute trust for a node (defaults to the local node).
    Karma { node_id: Option<String> },
    /// Create a thread root in a board.
    Post {
        #[arg(long)]
        board: String,
        #[arg(long)]
        title: String,
        #[arg(long, default_value = "")]
        body: String,
        /// Attach media ids.
        #[arg(long = "media")]
        media: Vec<String>,
    },
    /// Reply to a post/comment.
    Comment {
        #[arg(long)]
        board: String,
        #[arg(long)]
        parent: String,
        #[arg(long)]
        body: String,
    },
    /// Vote on a target post/comment: +1, -1, or 0 to clear.
    Vote { target: String, value: i8 },
    /// Follow / unfollow a node.
    Follow {
        node_id: String,
        #[arg(long)]
        unfollow: bool,
    },
    /// List threads in a board.
    Threads {
        board: String,
        #[arg(long, default_value = "hot")]
        sort: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// List the comment tree of a thread root.
    Comments {
        root: String,
        #[arg(long, default_value = "hot")]
        sort: String,
    },
    /// Upload a media file (bytes -> CE object store) and register its descriptor.
    Media {
        file: PathBuf,
        #[arg(long, value_enum)]
        kind: Kind,
        #[arg(long)]
        mime: Option<String>,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        duration_ms: Option<u64>,
    },
    /// Fetch media bytes by id to a file.
    Download { media_id: String, out: PathBuf },
    /// Start a live stream; prints the stream id.
    StreamStart {
        #[arg(long)]
        title: String,
        #[arg(long, value_enum, default_value_t = SKind::Video)]
        kind: SKind,
    },
    /// Append a segment file to a live stream.
    StreamAppend {
        stream: String,
        seq: u64,
        file: PathBuf,
        #[arg(long, default_value_t = 2000)]
        duration_ms: u64,
    },
    /// End a live stream, optionally publishing a recording file as the VOD.
    StreamEnd {
        stream: String,
        #[arg(long)]
        recording: Option<PathBuf>,
    },
    /// List currently-live streams.
    StreamsLive,

    // ----- community governance -----
    /// Create/register a board (community namespace) and its params.
    BoardCreate {
        board: String,
        #[arg(long, default_value = "")]
        title: String,
        #[arg(long, default_value = "")]
        description: String,
        /// Minimum fused trust (0.0-1.0) to post here. 0 = open.
        #[arg(long, default_value_t = 0.0)]
        min_trust_post: f64,
        /// Minimum fused trust (0.0-1.0) to vote here. 0 = open.
        #[arg(long, default_value_t = 0.0)]
        min_trust_vote: f64,
        /// Visibility grace window (seconds) for new posts.
        #[arg(long, default_value_t = 21600)]
        grace_secs: u64,
        /// Distinct-voter quorum required before a community ban can take effect.
        #[arg(long, default_value_t = 10)]
        ban_quorum: u32,
        /// Trust-weighted support fraction (0.0-1.0) required to ban.
        #[arg(long, default_value_t = 0.66)]
        ban_support: f64,
    },
    /// Show a board's metadata + params.
    Board { board: String },
    /// List boards.
    Boards,
    /// Show a feed ranked by any algorithm: hot, top, new, best, trending, rising, controversial.
    Feed {
        #[arg(long, default_value = "all")]
        scope: String,
        #[arg(long)]
        board: Option<String>,
        #[arg(long)]
        viewer: Option<String>,
        #[arg(long, default_value = "hot")]
        sort: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Cast a community ban vote on a user (default: vote to ban; --keep to vote to keep).
    BanVote {
        board: String,
        target: String,
        #[arg(long)]
        keep: bool,
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Show a user's community ban standing in a board (raw + trust-weighted).
    BanStanding { board: String, target: String },
    /// Propose a community policy (the substrate future AI enforcement applies).
    Propose {
        #[arg(long)]
        title: String,
        #[arg(long)]
        body: String,
        #[arg(long)]
        board: Option<String>,
    },
    /// Vote on a policy proposal (default: in favor; --against to oppose).
    PolicyVote {
        proposal: String,
        #[arg(long)]
        against: bool,
    },
    /// List policy proposals (optionally scoped to a board).
    Proposals {
        #[arg(long)]
        board: Option<String>,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Kind {
    Image,
    Video,
    Audio,
    Podcast,
    Document,
}

impl From<Kind> for MediaKind {
    fn from(k: Kind) -> Self {
        match k {
            Kind::Image => MediaKind::Image,
            Kind::Video => MediaKind::Video,
            Kind::Audio => MediaKind::Audio,
            Kind::Podcast => MediaKind::Podcast,
            Kind::Document => MediaKind::Document,
        }
    }
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum SKind {
    Video,
    Audio,
}

impl From<SKind> for StreamKind {
    fn from(k: SKind) -> Self {
        match k {
            SKind::Video => StreamKind::Video,
            SKind::Audio => StreamKind::Audio,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ce = ce_rs::CeClient::new(cli.node_url);
    let local_id = ce.status().await.context("local CE node not reachable")?.node_id;
    let t = match &cli.node {
        Some(n) => TranaClient::pinned(ce, n.clone()),
        None => TranaClient::new(ce),
    };

    match cli.cmd {
        Cmd::ProfileSet { display_name, bio, handle, devices, avatar } => {
            let r = t
                .profile_put(ProfilePutReq {
                    handle,
                    display_name,
                    bio,
                    avatar: avatar.map(MediaRef::new),
                    links: vec![],
                    devices,
                })
                .await?;
            println!("profile updated: {}", r.id);
        }
        Cmd::Profile { node_id } => {
            let id = node_id.unwrap_or(local_id);
            print_json(&t.profile_get(&id).await?)?;
        }
        Cmd::Karma { node_id } => {
            let id = node_id.unwrap_or(local_id);
            print_json(&t.karma(&id).await?)?;
        }
        Cmd::Post { board, title, body, media } => {
            let r = t
                .post_create(PostCreateReq {
                    board,
                    parent: None,
                    title: Some(title),
                    body,
                    media: media.into_iter().map(MediaRef::new).collect(),
                })
                .await?;
            println!("{}", r.id);
        }
        Cmd::Comment { board, parent, body } => {
            let r = t
                .post_create(PostCreateReq {
                    board,
                    parent: Some(parent),
                    title: None,
                    body,
                    media: vec![],
                })
                .await?;
            println!("{}", r.id);
        }
        Cmd::Vote { target, value } => {
            t.vote(&target, value).await?;
            println!("ok");
        }
        Cmd::Follow { node_id, unfollow } => {
            t.follow(&node_id, !unfollow).await?;
            println!("ok");
        }
        Cmd::Threads { board, sort, limit } => {
            print_json(&t.threads(ThreadsReq { board, sort, limit }).await?)?;
        }
        Cmd::Comments { root, sort } => {
            print_json(&t.comments(CommentsReq { root, sort }).await?)?;
        }
        Cmd::Media { file, kind, mime, title, duration_ms } => {
            let bytes = std::fs::read(&file).with_context(|| format!("read {}", file.display()))?;
            let size = bytes.len() as u64;
            let object_cid = t.upload(&bytes).await?;
            let mime = mime.unwrap_or_else(|| guess_mime(&file));
            let title = title.unwrap_or_else(|| file_name(&file));
            let r = t
                .media_put(MediaPutReq {
                    kind: kind.into(),
                    object_cid,
                    mime,
                    size,
                    title,
                    duration_ms,
                    width: None,
                    height: None,
                    thumbnail: None,
                    extra: Default::default(),
                    replicas: 0,
                })
                .await?;
            println!("{}", r.id);
        }
        Cmd::Download { media_id, out } => {
            let bytes = t.download(&media_id).await?;
            std::fs::write(&out, &bytes).with_context(|| format!("write {}", out.display()))?;
            println!("wrote {} bytes to {}", bytes.len(), out.display());
        }
        Cmd::StreamStart { title, kind } => {
            let r = t
                .stream_start(StreamStartReq {
                    title,
                    kind: kind.into(),
                    board: None,
                    thumbnail: None,
                    extra: Default::default(),
                })
                .await?;
            println!("{}", r.id);
        }
        Cmd::StreamAppend { stream, seq, file, duration_ms } => {
            let bytes = std::fs::read(&file).with_context(|| format!("read {}", file.display()))?;
            let object_cid = t.upload(&bytes).await?;
            let r = t
                .stream_append(StreamAppendReq { stream, seq, object_cid, duration_ms, replicas: 0 })
                .await?;
            println!("{}", r.id);
        }
        Cmd::StreamEnd { stream, recording } => {
            let recording_cid = match recording {
                Some(path) => {
                    let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
                    Some(t.upload(&bytes).await?)
                }
                None => None,
            };
            t.stream_end(StreamEndReq { stream, recording_cid }).await?;
            println!("ok");
        }
        Cmd::StreamsLive => {
            print_json(&t.streams_live().await?)?;
        }

        Cmd::BoardCreate {
            board,
            title,
            description,
            min_trust_post,
            min_trust_vote,
            grace_secs,
            ban_quorum,
            ban_support,
        } => {
            let mut policy = trana_core::BoardPolicy::default();
            policy.min_trust_to_post = min_trust_post;
            policy.min_trust_to_vote = min_trust_vote;
            policy.grace_secs = grace_secs;
            policy.ban_quorum = ban_quorum;
            policy.ban_support = ban_support;
            let r = t.board_put(BoardPutReq { board, title, description, policy }).await?;
            println!("{}", r.id);
        }
        Cmd::Board { board } => print_json(&t.board_get(&board).await?)?,
        Cmd::Boards => print_json(&t.boards().await?)?,
        Cmd::Feed { scope, board, viewer, sort, limit } => {
            let viewer = match viewer {
                Some(v) => Some(v),
                None if scope == "home" => Some(local_id.clone()),
                None => None,
            };
            print_json(&t.feed(FeedReq { scope, board, viewer, sort, limit }).await?)?;
        }
        Cmd::BanVote { board, target, keep, reason } => {
            t.ban_vote(&board, &target, !keep, &reason).await?;
            println!("ok");
        }
        Cmd::BanStanding { board, target } => print_json(&t.ban_standing(&board, &target).await?)?,
        Cmd::Propose { title, body, board } => {
            let r = t.policy_propose(PolicyProposeReq { board, title, body }).await?;
            println!("{}", r.id);
        }
        Cmd::PolicyVote { proposal, against } => {
            t.policy_vote(&proposal, !against).await?;
            println!("ok");
        }
        Cmd::Proposals { board } => print_json(&t.proposals(board.as_deref()).await?)?,
    }
    Ok(())
}

fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v).map_err(|e| anyhow!("encode: {e}"))?);
    Ok(())
}

fn file_name(p: &std::path::Path) -> String {
    p.file_name().and_then(|s| s.to_str()).unwrap_or("untitled").to_string()
}

/// Best-effort MIME from the file extension; falls back to octet-stream.
fn guess_mime(p: &std::path::Path) -> String {
    let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "ogg" | "opus" => "audio/ogg",
        "wav" => "audio/wav",
        "m4a" => "audio/mp4",
        "pdf" => "application/pdf",
        "md" | "markdown" => "text/markdown",
        "txt" => "text/plain",
        _ => "application/octet-stream",
    }
    .to_string()
}
