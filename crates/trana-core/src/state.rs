//! [`State`] — the materialized read-model, folded from a stream of [`Record`]s.
//!
//! Every query trana answers (a profile, a board of threads, a comment tree, a user's karma, a live
//! stream's segments) is read from here. `apply` folds one record in; the fold is **idempotent**
//! (re-applying a record by the same id is a no-op) and **order-tolerant for the mutable kinds**
//! (last-write-wins by `created_ms`), so a server, a phone, and a browser that have seen the same set
//! of records converge to the same views regardless of arrival order. That convergence is what makes
//! the backend genuinely distributed rather than a single source of truth.

use crate::model::{Body, Media, Post, Profile, StreamSegment, StreamStart};
use crate::record::Record;
use std::collections::{BTreeMap, HashMap, HashSet};

/// How to rank a feed of threads/comments. The feed algorithm in one enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortBy {
    /// Newest first.
    New,
    /// Highest net score (ups - downs) first.
    Top,
    /// Reddit-style hotness: sign-aware log score minus an age penalty (~12h scale).
    Hot,
    /// Wilson lower-bound confidence — "best": ranks by how confidently liked, not raw score, so a
    /// 9/10 beats a 40/50. Robust to small samples.
    Best,
    /// Velocity: total engagement decayed steeply by age — what is blowing up *right now*.
    Trending,
    /// Young posts gaining traction fast (score + replies per hour since posting).
    Rising,
    /// High engagement that is split (lots of both up and down votes) — the fights.
    Controversial,
}

impl SortBy {
    /// Parse a sort name; unknown values fall back to [`SortBy::Hot`].
    pub fn parse(s: &str) -> SortBy {
        match s.to_ascii_lowercase().as_str() {
            "new" => SortBy::New,
            "top" => SortBy::Top,
            "best" => SortBy::Best,
            "trending" => SortBy::Trending,
            "rising" => SortBy::Rising,
            "controversial" | "contro" => SortBy::Controversial,
            _ => SortBy::Hot,
        }
    }
}

/// A post/comment as returned to readers: the stored post plus derived vote tallies.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PostView {
    pub id: String,
    pub author: String,
    pub created_ms: u64,
    pub board: String,
    pub parent: Option<String>,
    pub title: Option<String>,
    pub body: String,
    pub media: Vec<String>,
    pub ups: u64,
    pub downs: u64,
    /// Net score (`ups - downs`).
    pub score: i64,
    /// Number of direct replies.
    pub reply_count: usize,
}

/// A profile as returned to readers: the stored profile plus the author key and last-update time.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProfileView {
    pub node_id: String,
    pub profile: Profile,
    pub updated_ms: u64,
}

/// A live (or ended) stream as returned to readers, with its ordered segment list.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamView {
    pub id: String,
    pub author: String,
    pub created_ms: u64,
    pub start: StreamStart,
    pub live: bool,
    pub recording_cid: Option<String>,
    /// Segments in ascending sequence order.
    pub segments: Vec<StreamSegment>,
    /// Total published duration so far, milliseconds.
    pub total_duration_ms: u64,
}

#[derive(Debug, Clone)]
struct StoredPost {
    author: String,
    created_ms: u64,
    post: Post,
}

#[derive(Debug, Clone)]
struct StreamRec {
    author: String,
    created_ms: u64,
    start: StreamStart,
    ended: bool,
    recording_cid: Option<String>,
    segments: BTreeMap<u64, StreamSegment>,
}

/// The materialized read-model. Build one, fold records through [`State::apply`], then query it.
#[derive(Debug, Default)]
pub struct State {
    /// Every record id ever applied — the idempotence guard.
    seen: HashSet<String>,

    profiles: HashMap<String, (Profile, u64)>,
    media: HashMap<String, (String, u64, Media)>,
    posts: HashMap<String, StoredPost>,

    /// board -> thread-root ids.
    board_threads: HashMap<String, Vec<String>>,
    /// parent id -> child ids (direct replies).
    children: HashMap<String, Vec<String>>,

    /// (voter, target) -> latest vote value (-1/0/+1).
    votes: HashMap<(String, String), i8>,
    /// target -> (ups, downs).
    tally: HashMap<String, (u64, u64)>,

    /// (follower, followee) -> active.
    follows: HashMap<(String, String), bool>,

    streams: HashMap<String, StreamRec>,
}

impl State {
    pub fn new() -> Self {
        State::default()
    }

    /// Number of distinct records folded in.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Has a record with this id already been applied?
    pub fn has(&self, id: &str) -> bool {
        self.seen.contains(id)
    }

    /// Fold one record into the model. Returns `true` if it was newly applied, `false` if it was a
    /// duplicate (same id already seen). Assumes the record was validated by [`Record::verify`].
    pub fn apply(&mut self, r: &Record) -> bool {
        if !self.seen.insert(r.id.clone()) {
            return false;
        }
        match &r.body {
            Body::Profile(p) => self.apply_profile(&r.author, r.created_ms, p),
            Body::Media(m) => {
                self.media.insert(r.id.clone(), (r.author.clone(), r.created_ms, m.clone()));
            }
            Body::Post(p) => self.apply_post(&r.id, &r.author, r.created_ms, p),
            Body::Vote(v) => self.apply_vote(&r.author, &v.target, v.value),
            Body::Follow(f) => {
                self.follows.insert((r.author.clone(), f.followee.clone()), f.active);
            }
            Body::StreamStart(s) => self.apply_stream_start(&r.id, &r.author, r.created_ms, s),
            Body::StreamSegment(s) => self.apply_stream_segment(&r.author, s),
            Body::StreamEnd(e) => self.apply_stream_end(&r.author, &e.stream, e.recording_cid.clone()),
        }
        true
    }

    fn apply_profile(&mut self, author: &str, created_ms: u64, p: &Profile) {
        match self.profiles.get(author) {
            Some((_, prev)) if *prev >= created_ms => {} // older or equal: ignore (last-write-wins)
            _ => {
                self.profiles.insert(author.to_string(), (p.clone(), created_ms));
            }
        }
    }

    fn apply_post(&mut self, id: &str, author: &str, created_ms: u64, p: &Post) {
        match &p.parent {
            None => {
                self.board_threads.entry(p.board.clone()).or_default().push(id.to_string());
            }
            Some(parent) => {
                self.children.entry(parent.clone()).or_default().push(id.to_string());
            }
        }
        self.posts.insert(
            id.to_string(),
            StoredPost { author: author.to_string(), created_ms, post: p.clone() },
        );
    }

    fn apply_vote(&mut self, voter: &str, target: &str, value: i8) {
        let value = value.clamp(-1, 1);
        let key = (voter.to_string(), target.to_string());
        let prev = self.votes.get(&key).copied().unwrap_or(0);
        if prev == value {
            return;
        }
        let (ups, downs) = self.tally.entry(target.to_string()).or_insert((0, 0));
        // Undo previous contribution.
        match prev {
            1 => *ups = ups.saturating_sub(1),
            -1 => *downs = downs.saturating_sub(1),
            _ => {}
        }
        // Apply new contribution.
        match value {
            1 => *ups += 1,
            -1 => *downs += 1,
            _ => {}
        }
        if value == 0 {
            self.votes.remove(&key);
        } else {
            self.votes.insert(key, value);
        }
    }

    fn apply_stream_start(&mut self, id: &str, author: &str, created_ms: u64, s: &StreamStart) {
        self.streams.entry(id.to_string()).or_insert_with(|| StreamRec {
            author: author.to_string(),
            created_ms,
            start: s.clone(),
            ended: false,
            recording_cid: None,
            segments: BTreeMap::new(),
        });
    }

    fn apply_stream_segment(&mut self, author: &str, s: &StreamSegment) {
        if let Some(st) = self.streams.get_mut(&s.stream) {
            // Only the stream's author may extend it.
            if st.author == author {
                st.segments.entry(s.seq).or_insert_with(|| s.clone());
            }
        }
    }

    fn apply_stream_end(&mut self, author: &str, stream: &str, recording: Option<String>) {
        if let Some(st) = self.streams.get_mut(stream) {
            if st.author == author {
                st.ended = true;
                if recording.is_some() {
                    st.recording_cid = recording;
                }
            }
        }
    }

    // ----- queries -----

    /// A user's profile, if they have published one.
    pub fn profile(&self, node_id: &str) -> Option<ProfileView> {
        self.profiles.get(node_id).map(|(p, t)| ProfileView {
            node_id: node_id.to_string(),
            profile: p.clone(),
            updated_ms: *t,
        })
    }

    /// A media descriptor by record id.
    pub fn media(&self, media_id: &str) -> Option<Media> {
        self.media.get(media_id).map(|(_, _, m)| m.clone())
    }

    /// All media authored by a user, newest first.
    pub fn media_by(&self, author: &str) -> Vec<(String, Media)> {
        let mut v: Vec<(String, u64, Media)> = self
            .media
            .iter()
            .filter(|(_, (a, _, _))| a == author)
            .map(|(id, (_, t, m))| (id.clone(), *t, m.clone()))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.into_iter().map(|(id, _, m)| (id, m)).collect()
    }

    /// A single post/comment view.
    pub fn post(&self, id: &str) -> Option<PostView> {
        let sp = self.posts.get(id)?;
        Some(self.view_post(id, sp))
    }

    fn view_post(&self, id: &str, sp: &StoredPost) -> PostView {
        let (ups, downs) = self.tally.get(id).copied().unwrap_or((0, 0));
        PostView {
            id: id.to_string(),
            author: sp.author.clone(),
            created_ms: sp.created_ms,
            board: sp.post.board.clone(),
            parent: sp.post.parent.clone(),
            title: sp.post.title.clone(),
            body: sp.post.body.clone(),
            media: sp.post.media.iter().map(|m| m.media_id.clone()).collect(),
            ups,
            downs,
            score: ups as i64 - downs as i64,
            reply_count: self.children.get(id).map(|c| c.len()).unwrap_or(0),
        }
    }

    /// Thread roots in a board, ordered by `sort`, limited to `limit` (use `now_ms` for hotness).
    pub fn threads(&self, board: &str, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        let ids = match self.board_threads.get(board) {
            Some(v) => v,
            None => return Vec::new(),
        };
        let mut views: Vec<PostView> = ids
            .iter()
            .filter_map(|id| self.posts.get(id).map(|sp| self.view_post(id, sp)))
            .collect();
        sort_views(&mut views, sort, now_ms);
        views.truncate(limit);
        views
    }

    /// Every descendant comment of a thread root, depth-first, each with its tallies. Ordered within
    /// each sibling group by `sort`.
    pub fn comments(&self, root: &str, sort: SortBy, now_ms: u64) -> Vec<PostView> {
        let mut out = Vec::new();
        self.collect_comments(root, sort, now_ms, &mut out);
        out
    }

    fn collect_comments(&self, parent: &str, sort: SortBy, now_ms: u64, out: &mut Vec<PostView>) {
        let Some(kids) = self.children.get(parent) else { return };
        let mut views: Vec<PostView> = kids
            .iter()
            .filter_map(|id| self.posts.get(id).map(|sp| self.view_post(id, sp)))
            .collect();
        sort_views(&mut views, sort, now_ms);
        for v in views {
            let id = v.id.clone();
            out.push(v);
            self.collect_comments(&id, sort, now_ms, out);
        }
    }

    /// Is `follower` following `followee`?
    pub fn is_following(&self, follower: &str, followee: &str) -> bool {
        self.follows.get(&(follower.to_string(), followee.to_string())).copied().unwrap_or(false)
    }

    /// Everyone `follower` follows.
    pub fn following(&self, follower: &str) -> Vec<String> {
        self.follows
            .iter()
            .filter(|((f, _), active)| f == follower && **active)
            .map(|((_, fe), _)| fe.clone())
            .collect()
    }

    /// Followers of `followee`.
    pub fn followers(&self, followee: &str) -> Vec<String> {
        self.follows
            .iter()
            .filter(|((_, fe), active)| fe == followee && **active)
            .map(|((f, _), _)| f.clone())
            .collect()
    }

    /// A stream view (live or ended) with ordered segments.
    pub fn stream(&self, id: &str) -> Option<StreamView> {
        self.streams.get(id).map(|st| StreamView {
            id: id.to_string(),
            author: st.author.clone(),
            created_ms: st.created_ms,
            start: st.start.clone(),
            live: !st.ended,
            recording_cid: st.recording_cid.clone(),
            segments: st.segments.values().cloned().collect(),
            total_duration_ms: st.segments.values().map(|s| s.duration_ms).sum(),
        })
    }

    /// All currently-live streams, newest first.
    pub fn live_streams(&self) -> Vec<StreamView> {
        let mut v: Vec<StreamView> = self
            .streams
            .iter()
            .filter(|(_, st)| !st.ended)
            .filter_map(|(id, _)| self.stream(id))
            .collect();
        v.sort_by(|a, b| b.created_ms.cmp(&a.created_ms));
        v
    }

    /// The social-karma aggregate for a user — the substrate [`crate::karma`] turns into a score.
    pub fn social(&self, node_id: &str) -> crate::karma::SocialKarma {
        let mut k = crate::karma::SocialKarma::default();
        for (id, sp) in &self.posts {
            if sp.author != node_id {
                continue;
            }
            let (ups, downs) = self.tally.get(id).copied().unwrap_or((0, 0));
            let net = ups as i64 - downs as i64;
            k.upvotes += ups;
            k.downvotes += downs;
            if sp.post.is_root() {
                k.posts += 1;
                k.post_score += net;
            } else {
                k.comments += 1;
                k.comment_score += net;
            }
        }
        k.followers = self.followers(node_id).len() as u64;
        k
    }
}

/// Order a slice of post views in place by `sort`.
fn sort_views(views: &mut [PostView], sort: SortBy, now_ms: u64) {
    match sort {
        SortBy::New => views.sort_by(|a, b| b.created_ms.cmp(&a.created_ms)),
        SortBy::Top => views.sort_by(|a, b| b.score.cmp(&a.score).then(b.created_ms.cmp(&a.created_ms))),
        SortBy::Hot => views.sort_by(|a, b| {
            hotness(b, now_ms)
                .partial_cmp(&hotness(a, now_ms))
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    }
}

/// A simple Reddit-like hotness: sign-aware log score minus an age penalty (~12h half-life scale).
fn hotness(v: &PostView, now_ms: u64) -> f64 {
    let s = v.score;
    let order = ((s.unsigned_abs().max(1)) as f64).log10();
    let sign = if s > 0 { 1.0 } else if s < 0 { -1.0 } else { 0.0 };
    let age_hours = (now_ms.saturating_sub(v.created_ms)) as f64 / 3_600_000.0;
    sign * order - age_hours / 12.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Follow, MediaRef, Vote};

    fn root(author: &str, board: &str, t: u64, title: &str) -> Record {
        Record::new(
            author,
            t,
            Body::Post(Post {
                board: board.into(),
                parent: None,
                title: Some(title.into()),
                body: "b".into(),
                media: vec![],
            }),
        )
        .unwrap()
    }

    fn comment(author: &str, board: &str, t: u64, parent: &str) -> Record {
        Record::new(
            author,
            t,
            Body::Post(Post {
                board: board.into(),
                parent: Some(parent.into()),
                title: None,
                body: "c".into(),
                media: vec![],
            }),
        )
        .unwrap()
    }

    fn vote(author: &str, t: u64, target: &str, value: i8) -> Record {
        Record::new(author, t, Body::Vote(Vote { target: target.into(), value })).unwrap()
    }

    #[test]
    fn apply_is_idempotent() {
        let mut s = State::new();
        let r = root("aa".repeat(32).as_str(), "b", 1, "t");
        assert!(s.apply(&r));
        assert!(!s.apply(&r), "second apply of same id is a no-op");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn threads_and_comments_tree() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let r = root(&a, "ce-dev", 10, "root");
        s.apply(&r);
        let c1 = comment(&a, "ce-dev", 11, &r.id);
        let c2 = comment(&a, "ce-dev", 12, &c1.id);
        s.apply(&c1);
        s.apply(&c2);

        let threads = s.threads("ce-dev", SortBy::New, 10, 100);
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, r.id);
        assert_eq!(threads[0].reply_count, 1);

        let comments = s.comments(&r.id, SortBy::New, 100);
        assert_eq!(comments.len(), 2, "nested comment is included");
        assert_eq!(comments[0].id, c1.id);
        assert_eq!(comments[1].id, c2.id);
    }

    #[test]
    fn votes_tally_and_change() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let voter = "bb".repeat(32);
        let r = root(&a, "b", 1, "t");
        s.apply(&r);

        s.apply(&vote(&voter, 2, &r.id, 1));
        assert_eq!(s.post(&r.id).unwrap().score, 1);
        // Change to downvote: net swings to -1, not -... double counting.
        s.apply(&vote(&voter, 3, &r.id, -1));
        let pv = s.post(&r.id).unwrap();
        assert_eq!((pv.ups, pv.downs, pv.score), (0, 1, -1));
        // Clear.
        s.apply(&vote(&voter, 4, &r.id, 0));
        assert_eq!(s.post(&r.id).unwrap().score, 0);
    }

    #[test]
    fn social_karma_splits_posts_and_comments() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let v1 = "bb".repeat(32);
        let v2 = "cc".repeat(32);
        let r = root(&a, "b", 1, "t");
        let c = comment(&a, "b", 2, &r.id);
        s.apply(&r);
        s.apply(&c);
        s.apply(&vote(&v1, 3, &r.id, 1));
        s.apply(&vote(&v2, 3, &r.id, 1));
        s.apply(&vote(&v1, 3, &c.id, -1));

        let k = s.social(&a);
        assert_eq!(k.posts, 1);
        assert_eq!(k.comments, 1);
        assert_eq!(k.post_score, 2);
        assert_eq!(k.comment_score, -1);
    }

    #[test]
    fn profile_last_write_wins() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let mut p1 = Profile::default();
        p1.display_name = "old".into();
        let mut p2 = Profile::default();
        p2.display_name = "new".into();
        s.apply(&Record::new(&a, 5, Body::Profile(p2)).unwrap());
        s.apply(&Record::new(&a, 1, Body::Profile(p1)).unwrap()); // older, must not win
        assert_eq!(s.profile(&a).unwrap().profile.display_name, "new");
    }

    #[test]
    fn streams_collect_segments_in_order() {
        use crate::model::{StreamKind, StreamSegment, StreamStart};
        let mut s = State::new();
        let a = "aa".repeat(32);
        let start = Record::new(
            &a,
            1,
            Body::StreamStart(StreamStart {
                title: "live".into(),
                kind: StreamKind::Video,
                board: None,
                thumbnail: None,
                extra: Default::default(),
            }),
        )
        .unwrap();
        s.apply(&start);
        // Apply segments out of order.
        for seq in [2u64, 0, 1] {
            s.apply(
                &Record::new(
                    &a,
                    10 + seq,
                    Body::StreamSegment(StreamSegment {
                        stream: start.id.clone(),
                        seq,
                        object_cid: format!("cid{seq}"),
                        duration_ms: 2000,
                    }),
                )
                .unwrap(),
            );
        }
        let view = s.stream(&start.id).unwrap();
        assert!(view.live);
        assert_eq!(view.segments.iter().map(|s| s.seq).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(view.total_duration_ms, 6000);
        assert_eq!(s.live_streams().len(), 1);
    }

    #[test]
    fn stream_segment_from_non_author_is_ignored() {
        use crate::model::{StreamKind, StreamSegment, StreamStart};
        let mut s = State::new();
        let a = "aa".repeat(32);
        let imposter = "bb".repeat(32);
        let start = Record::new(
            &a,
            1,
            Body::StreamStart(StreamStart {
                title: "live".into(),
                kind: StreamKind::Audio,
                board: None,
                thumbnail: None,
                extra: Default::default(),
            }),
        )
        .unwrap();
        s.apply(&start);
        s.apply(
            &Record::new(
                &imposter,
                2,
                Body::StreamSegment(StreamSegment {
                    stream: start.id.clone(),
                    seq: 0,
                    object_cid: "x".into(),
                    duration_ms: 1000,
                }),
            )
            .unwrap(),
        );
        assert!(s.stream(&start.id).unwrap().segments.is_empty());
    }

    #[test]
    fn follows_track_both_directions() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        s.apply(&Record::new(&a, 1, Body::Follow(Follow { followee: b.clone(), active: true })).unwrap());
        assert!(s.is_following(&a, &b));
        assert_eq!(s.followers(&b), vec![a.clone()]);
        s.apply(&Record::new(&a, 2, Body::Follow(Follow { followee: b.clone(), active: false })).unwrap());
        assert!(!s.is_following(&a, &b));
    }

    #[test]
    fn media_indexing() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let m = Media {
            kind: crate::model::MediaKind::Video,
            object_cid: "obj".into(),
            mime: "video/mp4".into(),
            size: 100,
            title: "clip".into(),
            duration_ms: Some(5000),
            width: Some(1920),
            height: Some(1080),
            thumbnail: None,
            extra: Default::default(),
        };
        let rec = Record::new(&a, 1, Body::Media(m.clone())).unwrap();
        s.apply(&rec);
        assert_eq!(s.media(&rec.id).unwrap().object_cid, "obj");
        assert_eq!(s.media_by(&a).len(), 1);
        // Reference it from a post.
        let _ = MediaRef::new(rec.id);
    }
}
