//! [`State`] — the materialized read-model, folded from a stream of [`Record`]s.
//!
//! Every query trana answers (a profile, a board of threads, a comment tree, a user's karma, a live
//! stream's segments) is read from here. `apply` folds one record in; the fold is **idempotent**
//! (re-applying a record by the same id is a no-op) and **order-tolerant for the mutable kinds**
//! (last-write-wins by `created_ms`), so a server, a phone, and a browser that have seen the same set
//! of records converge to the same views regardless of arrival order. That convergence is what makes
//! the backend genuinely distributed rather than a single source of truth.

use crate::model::{
    BanVote, BoardCreate, BoardPolicy, Body, Media, PolicyProposal, PolicyVote, Post, Profile,
    StreamSegment, StreamStart,
};
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

/// A board: a community namespace plus its params. No owner powers — `rank` is only used to make the
/// first-claim deterministic under out-of-order gossip.
#[derive(Debug, Clone)]
struct BoardRec {
    title: String,
    description: String,
    policy: BoardPolicy,
    created_ms: u64,
    /// The winning claim's (created_ms, record_id) — smallest wins, so ownership-of-the-name is
    /// deterministic regardless of arrival order.
    rank: (u64, String),
}

/// A board namespace view.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoardView {
    pub board: String,
    pub title: String,
    pub description: String,
    pub policy: BoardPolicy,
    pub created_ms: u64,
}

/// The community ban standing for a user in a board: who voted to ban vs keep, and whether the raw
/// (unweighted) tally already meets quorum. The node refines this with trust weighting.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BanStanding {
    pub board: String,
    pub target: String,
    /// Distinct voters supporting a ban.
    pub support: u64,
    /// Distinct voters opposing (voting to keep).
    pub oppose: u64,
    /// True if the unweighted tally clears the board's support fraction AND quorum.
    pub banned_raw: bool,
}

/// A community policy proposal with its current vote standing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProposalView {
    pub id: String,
    pub author: String,
    pub created_ms: u64,
    pub board: Option<String>,
    pub title: String,
    pub body: String,
    pub favor: u64,
    pub against: u64,
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

    // ----- community governance -----
    /// board name -> namespace + params.
    boards: HashMap<String, BoardRec>,
    /// (board, target_user) -> (voter -> support). Community ban votes.
    ban_votes: HashMap<(String, String), HashMap<String, bool>>,
    /// proposal id -> (author, created_ms, proposal).
    proposals: HashMap<String, (String, u64, PolicyProposal)>,
    /// proposal id -> (voter -> support).
    policy_votes: HashMap<String, HashMap<String, bool>>,
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
            Body::BoardCreate(b) => self.apply_board_create(&r.id, r.created_ms, b),
            Body::BanVote(b) => self.apply_ban_vote(&r.author, b),
            Body::PolicyProposal(p) => {
                self.proposals.insert(r.id.clone(), (r.author.clone(), r.created_ms, p.clone()));
            }
            Body::PolicyVote(v) => self.apply_policy_vote(&r.author, v),
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

    fn apply_board_create(&mut self, id: &str, created_ms: u64, b: &BoardCreate) {
        let rank = (created_ms, id.to_string());
        match self.boards.get_mut(&b.board) {
            // First claim (smallest rank) wins the namespace + its params, deterministically.
            Some(existing) if rank < existing.rank => {
                existing.title = b.title.clone();
                existing.description = b.description.clone();
                existing.policy = b.policy.clone();
                existing.created_ms = created_ms;
                existing.rank = rank;
            }
            Some(_) => {} // a later/non-winning claim: ignore.
            None => {
                self.boards.insert(
                    b.board.clone(),
                    BoardRec {
                        title: b.title.clone(),
                        description: b.description.clone(),
                        policy: b.policy.clone(),
                        created_ms,
                        rank,
                    },
                );
            }
        }
    }

    fn apply_ban_vote(&mut self, voter: &str, b: &BanVote) {
        self.ban_votes
            .entry((b.board.clone(), b.target.clone()))
            .or_default()
            .insert(voter.to_string(), b.support);
    }

    fn apply_policy_vote(&mut self, voter: &str, v: &PolicyVote) {
        self.policy_votes.entry(v.proposal.clone()).or_default().insert(voter.to_string(), v.support);
    }

    /// The community params for a board (defaults if the board was never explicitly created).
    fn policy_of(&self, board: &str) -> BoardPolicy {
        self.boards.get(board).map(|b| b.policy.clone()).unwrap_or_default()
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

    /// True if the post's author has been community-banned (raw, unweighted) in its board, so it
    /// should be hidden from listings.
    fn hidden(&self, sp: &StoredPost) -> bool {
        self.is_banned_raw(&sp.post.board, &sp.author)
    }

    /// Thread roots in a board, ranked by the feed algorithm `sort`, limited to `limit`. New +
    /// controversial threads get a visibility grace window (per the board's `grace_secs`) so they are
    /// seen before reception decides; banned authors' threads are hidden.
    pub fn threads(&self, board: &str, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        let grace = self.policy_of(board).grace_secs;
        let ids = match self.board_threads.get(board) {
            Some(v) => v,
            None => return Vec::new(),
        };
        let mut views: Vec<PostView> = ids
            .iter()
            .filter_map(|id| self.posts.get(id).map(|sp| (id, sp)))
            .filter(|(_, sp)| !self.hidden(sp))
            .map(|(id, sp)| self.view_post(id, sp))
            .collect();
        rank_views(&mut views, sort, now_ms, grace);
        views.truncate(limit);
        views
    }

    /// Every descendant comment of a thread root, depth-first, each with its tallies. Ordered within
    /// each sibling group by `sort`; banned authors' comments are hidden.
    pub fn comments(&self, root: &str, sort: SortBy, now_ms: u64) -> Vec<PostView> {
        let grace =
            self.posts.get(root).map(|sp| self.policy_of(&sp.post.board).grace_secs).unwrap_or(0);
        let mut out = Vec::new();
        self.collect_comments(root, sort, now_ms, grace, &mut out);
        out
    }

    fn collect_comments(
        &self,
        parent: &str,
        sort: SortBy,
        now_ms: u64,
        grace: u64,
        out: &mut Vec<PostView>,
    ) {
        let Some(kids) = self.children.get(parent) else { return };
        let mut views: Vec<PostView> = kids
            .iter()
            .filter_map(|id| self.posts.get(id).map(|sp| (id, sp)))
            .filter(|(_, sp)| !self.hidden(sp))
            .map(|(id, sp)| self.view_post(id, sp))
            .collect();
        rank_views(&mut views, sort, now_ms, grace);
        for v in views {
            let id = v.id.clone();
            out.push(v);
            self.collect_comments(&id, sort, now_ms, grace, out);
        }
    }

    /// A cross-board feed of every (non-hidden) thread root, ranked by `sort`.
    pub fn all_feed(&self, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        let mut views: Vec<PostView> = self
            .posts
            .iter()
            .filter(|(_, sp)| sp.post.is_root() && !self.hidden(sp))
            .map(|(id, sp)| self.view_post(id, sp))
            .collect();
        rank_views(&mut views, sort, now_ms, default_grace());
        views.truncate(limit);
        views
    }

    /// A personalized home feed: thread roots authored by anyone `viewer` follows, ranked by `sort`.
    pub fn home_feed(&self, viewer: &str, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        let follows: HashSet<String> = self.following(viewer).into_iter().collect();
        let mut views: Vec<PostView> = self
            .posts
            .iter()
            .filter(|(_, sp)| sp.post.is_root() && follows.contains(&sp.author) && !self.hidden(sp))
            .map(|(id, sp)| self.view_post(id, sp))
            .collect();
        rank_views(&mut views, sort, now_ms, default_grace());
        views.truncate(limit);
        views
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

    // ----- community governance queries -----

    /// A board's namespace metadata + community params (defaults if never explicitly created).
    pub fn board(&self, board: &str) -> BoardView {
        let b = self.boards.get(board);
        BoardView {
            board: board.to_string(),
            title: b.map(|x| x.title.clone()).unwrap_or_default(),
            description: b.map(|x| x.description.clone()).unwrap_or_default(),
            policy: self.policy_of(board),
            created_ms: b.map(|x| x.created_ms).unwrap_or(0),
        }
    }

    /// Every explicitly-created board, newest first.
    pub fn boards(&self) -> Vec<BoardView> {
        let mut v: Vec<BoardView> = self.boards.keys().map(|k| self.board(k)).collect();
        v.sort_by(|a, b| b.created_ms.cmp(&a.created_ms));
        v
    }

    /// The community params for a board (public accessor).
    pub fn board_policy(&self, board: &str) -> BoardPolicy {
        self.policy_of(board)
    }

    /// The raw (unweighted) community ban standing for a user in a board.
    pub fn ban_standing(&self, board: &str, target: &str) -> BanStanding {
        let mut support = 0u64;
        let mut oppose = 0u64;
        if let Some(votes) = self.ban_votes.get(&(board.to_string(), target.to_string())) {
            for &s in votes.values() {
                if s {
                    support += 1;
                } else {
                    oppose += 1;
                }
            }
        }
        BanStanding {
            board: board.to_string(),
            target: target.to_string(),
            support,
            oppose,
            banned_raw: self.is_banned_raw(board, target),
        }
    }

    /// Is a user community-banned in a board by the raw (one-person-one-vote) tally? A ban needs at
    /// least `ban_quorum` distinct voters and a `ban_support` fraction in favor. The node refines
    /// this with trust weighting; this is the convergent default every node agrees on.
    pub fn is_banned_raw(&self, board: &str, target: &str) -> bool {
        let votes = match self.ban_votes.get(&(board.to_string(), target.to_string())) {
            Some(v) => v,
            None => return false,
        };
        let support = votes.values().filter(|&&s| s).count() as u64;
        let total = votes.len() as u64;
        if total == 0 {
            return false;
        }
        let policy = self.policy_of(board);
        total >= policy.ban_quorum as u64
            && (support as f64) >= policy.ban_support * (total as f64)
            && support * 2 > total
    }

    /// Raw per-voter ban votes for a user in a board: `(voter, support)` — for the node to apply
    /// trust weighting.
    pub fn ban_votes_raw(&self, board: &str, target: &str) -> Vec<(String, bool)> {
        self.ban_votes
            .get(&(board.to_string(), target.to_string()))
            .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default()
    }

    /// A policy proposal with its current vote standing.
    pub fn proposal(&self, id: &str) -> Option<ProposalView> {
        let (author, ms, p) = self.proposals.get(id)?;
        let (favor, against) = self.policy_tally(id);
        Some(ProposalView {
            id: id.to_string(),
            author: author.clone(),
            created_ms: *ms,
            board: p.board.clone(),
            title: p.title.clone(),
            body: p.body.clone(),
            favor,
            against,
        })
    }

    /// All policy proposals (optionally scoped to a board), newest first.
    pub fn proposals(&self, board: Option<&str>) -> Vec<ProposalView> {
        let mut v: Vec<ProposalView> = self
            .proposals
            .iter()
            .filter(|(_, (_, _, p))| match board {
                Some(b) => p.board.as_deref() == Some(b),
                None => true,
            })
            .filter_map(|(id, _)| self.proposal(id))
            .collect();
        v.sort_by(|a, b| b.created_ms.cmp(&a.created_ms));
        v
    }

    fn policy_tally(&self, proposal: &str) -> (u64, u64) {
        match self.policy_votes.get(proposal) {
            Some(m) => {
                let favor = m.values().filter(|&&s| s).count() as u64;
                (favor, m.len() as u64 - favor)
            }
            None => (0, 0),
        }
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

/// Default visibility grace window (seconds) for cross-board / home feeds (boards can override).
fn default_grace() -> u64 {
    6 * 3600
}

/// Rank a slice of post views in place by the feed algorithm `sort`. `grace` is the board's
/// visibility window in seconds (only [`SortBy::Hot`], the default community feed, uses it).
fn rank_views(views: &mut [PostView], sort: SortBy, now_ms: u64, grace: u64) {
    let key = |v: &PostView| -> f64 {
        match sort {
            SortBy::New => v.created_ms as f64,
            SortBy::Top => v.score as f64,
            SortBy::Hot => chance_hot(v, now_ms, grace),
            SortBy::Best => wilson_lower_bound(v.ups, v.downs),
            SortBy::Trending => trending(v, now_ms),
            SortBy::Rising => rising(v, now_ms),
            SortBy::Controversial => controversial(v),
        }
    };
    views.sort_by(|a, b| {
        key(b)
            .partial_cmp(&key(a))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.created_ms.cmp(&a.created_ms))
    });
}

fn age_hours(v: &PostView, now_ms: u64) -> f64 {
    now_ms.saturating_sub(v.created_ms) as f64 / 3_600_000.0
}

/// The community feed score — trana's deliberate inversion of Reddit. **During the grace window** a
/// post is ranked by ENGAGEMENT magnitude (ups + downs, sign-agnostic) plus a decaying visibility
/// boost: controversy and dissent get *seen*, never buried, so a post has a real chance to persuade
/// people or find the ones who already agree. **After** the window, net reception takes over — so
/// sustained approval persists and sustained "booing" fades (and is what motivates a community ban).
fn chance_hot(v: &PostView, now_ms: u64, grace_secs: u64) -> f64 {
    const GRACE_BOOST: f64 = 3.0;
    let age_secs = now_ms.saturating_sub(v.created_ms) / 1000;
    let engagement = (v.ups + v.downs) as f64;
    let base = (engagement + 1.0).log10(); // sign-agnostic: a fight ranks like a love-in
    let recency = -age_hours(v, now_ms) / 12.0;

    if grace_secs > 0 && age_secs <= grace_secs {
        // Inside the window: decaying boost, no net-score penalty — everyone gets a chance.
        let grace_frac = 1.0 - (age_secs as f64 / grace_secs as f64);
        base + grace_frac * GRACE_BOOST + recency
    } else {
        // Out of the window: reception decides (sign-aware log score).
        let s = v.score;
        let sign = if s > 0 { 1.0 } else if s < 0 { -1.0 } else { 0.0 };
        let reception = sign * ((s.unsigned_abs()) as f64 + 1.0).log10();
        base * 0.25 + reception + recency
    }
}

/// Wilson score lower bound (95%) — "best": how confidently a post is liked, robust to sample size.
fn wilson_lower_bound(ups: u64, downs: u64) -> f64 {
    let n = (ups + downs) as f64;
    if n == 0.0 {
        return 0.0;
    }
    let z = 1.959_963_984_540_054_f64;
    let phat = ups as f64 / n;
    (phat + z * z / (2.0 * n) - z * ((phat * (1.0 - phat) + z * z / (4.0 * n)) / n).sqrt())
        / (1.0 + z * z / n)
}

/// Velocity: total engagement decayed steeply by age — what is blowing up right now (sign-agnostic).
fn trending(v: &PostView, now_ms: u64) -> f64 {
    let engagement = (v.ups + v.downs) as f64;
    engagement / (age_hours(v, now_ms) + 2.0).powf(1.5)
}

/// Young posts gaining traction fast: positive score + replies per (early) hour.
fn rising(v: &PostView, now_ms: u64) -> f64 {
    let momentum = v.score.max(0) as f64 + v.reply_count as f64;
    momentum / (age_hours(v, now_ms) + 2.0)
}

/// High engagement that is split — the fights. Zero unless there are both up and down votes.
fn controversial(v: &PostView) -> f64 {
    if v.ups == 0 || v.downs == 0 {
        return 0.0;
    }
    let (lo, hi) = if v.ups <= v.downs { (v.ups, v.downs) } else { (v.downs, v.ups) };
    (v.ups + v.downs) as f64 * (lo as f64 / hi as f64)
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
