//! [`State`] — the materialized read-model, folded from a stream of [`Record`]s.
//!
//! Every query trana answers (a profile, a board of threads, a comment tree, a user's karma, a live
//! stream's segments) is read from here. `apply` folds one record in; the fold is **idempotent**
//! (re-applying a record by the same id is a no-op) and **order-tolerant for the mutable kinds**
//! (last-write-wins by `created_ms`), so a server, a phone, and a browser that have seen the same set
//! of records converge to the same views regardless of arrival order. That convergence is what makes
//! the backend genuinely distributed rather than a single source of truth.

use crate::model::{
    BanVote, BoardCreate, BoardPolicy, Body, DeviceLink, Document, FileRef, Media, PolicyProposal,
    PolicyVote, Post, Profile, Ref, StreamSegment, StreamStart,
};
use crate::karma::decay;
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
    /// Author of the winning claim — a deterministic, in-log trust anchor used to seed the
    /// web-of-trust propagation (the people who bootstrapped communities).
    creator: String,
}

/// A document as returned to readers: the markdown artifact, its references (forward links), the
/// references that point AT it (backlinks), and its vote tally — documents are votable like posts.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DocumentView {
    pub id: String,
    pub author: String,
    pub created_ms: u64,
    pub title: String,
    pub body: String,
    /// Present when this document version is a binary file (PDF/dataset/...).
    pub file: Option<FileRef>,
    pub board: Option<String>,
    /// All references this document makes (declared `refs` + any parsed from the markdown body), as
    /// `trana://...` URIs — the clean, mesh-resolvable content addresses.
    pub refs: Vec<String>,
    /// `trana://...` URIs of content that references THIS document (backlinks).
    pub referenced_by: Vec<String>,
    // ----- version control -----
    /// Stable series id (the first version's record id) — the artifact's identity across edits.
    pub series: String,
    /// The version this one supersedes, if any.
    pub prev: Option<String>,
    /// 1-based version number within the series.
    pub version: u32,
    /// Total versions in the series.
    pub versions: u32,
    /// True if this is the newest version in its series.
    pub is_latest: bool,
    pub ups: u64,
    pub downs: u64,
    pub score: i64,
}

/// One line of a unified diff between two document versions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffLine {
    /// `" "` context, `"-"` removed, `"+"` added.
    pub op: String,
    pub text: String,
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

    /// target -> voter -> (latest vote value -1/+1, the vote record's `created_ms`). Retaining the
    /// voter identity and timestamp is what lets the node compute *trust-weighted, time-decayed*
    /// scores (see [`State::weighted_score`]); the raw `tally` below stays for the cheap path.
    target_votes: HashMap<String, HashMap<String, (i8, u64)>>,
    /// target -> (ups, downs), raw one-vote-one-count.
    tally: HashMap<String, (u64, u64)>,

    /// (follower, followee) -> active.
    follows: HashMap<(String, String), bool>,

    streams: HashMap<String, StreamRec>,

    /// document id -> (author, created_ms, document).
    documents: HashMap<String, (String, u64, Document)>,
    /// series id -> version record ids (a document's edit history; git-like version chain).
    doc_series: HashMap<String, Vec<String>>,
    /// referenced content id -> the `trana://...` URIs that reference it (backlinks).
    backlinks: HashMap<String, Vec<String>>,

    // ----- community governance -----
    /// board name -> namespace + params.
    boards: HashMap<String, BoardRec>,
    /// (board, target_user) -> (voter -> support). Community ban votes.
    ban_votes: HashMap<(String, String), HashMap<String, bool>>,
    /// proposal id -> (author, created_ms, proposal).
    proposals: HashMap<String, (String, u64, PolicyProposal)>,
    /// proposal id -> (voter -> support).
    policy_votes: HashMap<String, HashMap<String, bool>>,

    /// device (the record author) -> (claimed owner, active, created_ms). The device's own signed
    /// consent to belong to an owner; last-write-wins. Combined with the owner's `Profile.devices`
    /// to gate compute-trust roll-up (see [`State::is_device_linked`]).
    device_links: HashMap<String, (String, bool, u64)>,
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
            Body::Vote(v) => self.apply_vote(&r.author, &v.target, v.value, r.created_ms),
            Body::Follow(f) => {
                self.follows.insert((r.author.clone(), f.followee.clone()), f.active);
            }
            Body::StreamStart(s) => self.apply_stream_start(&r.id, &r.author, r.created_ms, s),
            Body::StreamSegment(s) => self.apply_stream_segment(&r.author, s),
            Body::StreamEnd(e) => self.apply_stream_end(&r.author, &e.stream, e.recording_cid.clone()),
            Body::Document(d) => self.apply_document(&r.id, &r.author, r.created_ms, d),
            Body::BoardCreate(b) => self.apply_board_create(&r.id, &r.author, r.created_ms, b),
            Body::BanVote(b) => self.apply_ban_vote(&r.author, b),
            Body::PolicyProposal(p) => {
                self.proposals.insert(r.id.clone(), (r.author.clone(), r.created_ms, p.clone()));
            }
            Body::PolicyVote(v) => self.apply_policy_vote(&r.author, v),
            Body::DeviceLink(d) => self.apply_device_link(&r.author, r.created_ms, d),
        }
        true
    }

    fn apply_device_link(&mut self, device: &str, created_ms: u64, d: &DeviceLink) {
        match self.device_links.get(device) {
            Some((_, _, prev)) if *prev >= created_ms => {} // older or equal: ignore (LWW).
            _ => {
                self.device_links
                    .insert(device.to_string(), (d.owner.clone(), d.active, created_ms));
            }
        }
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

    fn apply_vote(&mut self, voter: &str, target: &str, value: i8, created_ms: u64) {
        let value = value.clamp(-1, 1);
        let voters = self.target_votes.entry(target.to_string()).or_default();
        let prev = voters.get(voter).map(|(v, _)| *v).unwrap_or(0);
        if prev == value {
            // Same stance: keep the contribution, but advance the timestamp if this vote is newer so
            // time-decay reflects the most recent expression of it.
            if value != 0 {
                if let Some(slot) = voters.get_mut(voter) {
                    if created_ms > slot.1 {
                        slot.1 = created_ms;
                    }
                }
            }
            return;
        }
        // Raw tally (disjoint field from target_votes; both borrows are fine).
        let (ups, downs) = self.tally.entry(target.to_string()).or_insert((0, 0));
        match prev {
            1 => *ups = ups.saturating_sub(1),
            -1 => *downs = downs.saturating_sub(1),
            _ => {}
        }
        match value {
            1 => *ups += 1,
            -1 => *downs += 1,
            _ => {}
        }
        // Reverse index (voter identity + timestamp), for trust-weighted / decayed scoring.
        if value == 0 {
            voters.remove(voter);
        } else {
            voters.insert(voter.to_string(), (value, created_ms));
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

    fn apply_document(&mut self, id: &str, author: &str, created_ms: u64, d: &Document) {
        // Index backlinks: every ref this document makes (declared + inline in markdown) records a
        // backlink from this document to the referenced content.
        let self_uri = crate::model::Ref::document(id).to_uri();
        for r in self.doc_all_refs(d) {
            let entry = self.backlinks.entry(r.id.clone()).or_default();
            if !entry.contains(&self_uri) {
                entry.push(self_uri.clone());
            }
        }
        // Index the version into its series (the first version's id, or this id if it starts one).
        let series = d.series.clone().unwrap_or_else(|| id.to_string());
        let chain = self.doc_series.entry(series).or_default();
        if !chain.contains(&id.to_string()) {
            chain.push(id.to_string());
        }
        self.documents.insert(id.to_string(), (author.to_string(), created_ms, d.clone()));
    }

    /// The series id a document version belongs to.
    fn series_of(&self, id: &str) -> String {
        match self.documents.get(id) {
            Some((_, _, d)) => d.series.clone().unwrap_or_else(|| id.to_string()),
            None => id.to_string(),
        }
    }

    /// Version ids of a series (or the series containing `key`), ordered oldest -> newest by time.
    fn series_versions(&self, key: &str) -> Vec<String> {
        let series = self.series_of(key);
        let mut ids = self.doc_series.get(&series).cloned().unwrap_or_default();
        ids.sort_by_key(|id| self.documents.get(id).map(|(_, t, _)| *t).unwrap_or(0));
        ids
    }

    /// All references a document makes: its declared `refs` plus any parsed inline from the markdown,
    /// de-duplicated.
    fn doc_all_refs(&self, d: &Document) -> Vec<Ref> {
        let mut all = d.refs.clone();
        for r in crate::model::extract_refs(&d.body) {
            if !all.contains(&r) {
                all.push(r);
            }
        }
        all
    }

    fn apply_board_create(&mut self, id: &str, author: &str, created_ms: u64, b: &BoardCreate) {
        let rank = (created_ms, id.to_string());
        match self.boards.get_mut(&b.board) {
            // First claim (smallest rank) wins the namespace + its params, deterministically.
            Some(existing) if rank < existing.rank => {
                existing.title = b.title.clone();
                existing.description = b.description.clone();
                existing.policy = b.policy.clone();
                existing.created_ms = created_ms;
                existing.rank = rank;
                existing.creator = author.to_string();
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
                        creator: author.to_string(),
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

    /// A document version by id, with its refs, backlinks, vote tally, and version-control position.
    pub fn document(&self, id: &str) -> Option<DocumentView> {
        let (author, created_ms, d) = self.documents.get(id)?;
        let (ups, downs) = self.tally.get(id).copied().unwrap_or((0, 0));
        let chain = self.series_versions(id);
        let version = chain.iter().position(|v| v == id).map(|i| i as u32 + 1).unwrap_or(1);
        Some(DocumentView {
            id: id.to_string(),
            author: author.clone(),
            created_ms: *created_ms,
            title: d.title.clone(),
            body: d.body.clone(),
            file: d.file.clone(),
            board: d.board.clone(),
            refs: self.doc_all_refs(d).iter().map(|r| r.to_uri()).collect(),
            referenced_by: self.backlinks.get(id).cloned().unwrap_or_default(),
            series: self.series_of(id),
            prev: d.prev.clone(),
            version,
            versions: chain.len() as u32,
            is_latest: chain.last().map(|v| v == id).unwrap_or(true),
            ups,
            downs,
            score: ups as i64 - downs as i64,
        })
    }

    /// The full version history of a document (or the series containing `key`), oldest -> newest.
    pub fn document_history(&self, key: &str) -> Vec<DocumentView> {
        self.series_versions(key).iter().filter_map(|id| self.document(id)).collect()
    }

    /// The newest version of a document's series.
    pub fn document_latest(&self, key: &str) -> Option<DocumentView> {
        self.series_versions(key).last().and_then(|id| self.document(id))
    }

    /// A unified line diff between two document versions' markdown bodies. For binary-file versions
    /// (no text body) it returns a single line summarizing the change.
    pub fn document_diff(&self, from_id: &str, to_id: &str) -> Vec<DiffLine> {
        let from = self.documents.get(from_id).map(|(_, _, d)| d);
        let to = self.documents.get(to_id).map(|(_, _, d)| d);
        let (from, to) = match (from, to) {
            (Some(a), Some(b)) => (a, b),
            _ => return Vec::new(),
        };
        if from.file.is_some() || to.file.is_some() {
            let a = from.file.as_ref().map(|f| f.size).unwrap_or(0);
            let b = to.file.as_ref().map(|f| f.size).unwrap_or(0);
            let same = from.file.as_ref().map(|f| &f.object_cid) == to.file.as_ref().map(|f| &f.object_cid);
            let msg = if same { "binary unchanged".into() } else { format!("binary changed ({a} -> {b} bytes)") };
            return vec![DiffLine { op: " ".into(), text: msg }];
        }
        line_diff(&from.body, &to.body)
    }

    /// All documents authored by a user, newest first.
    pub fn documents_by(&self, author: &str) -> Vec<DocumentView> {
        let mut ids: Vec<(String, u64)> = self
            .documents
            .iter()
            .filter(|(_, (a, _, _))| a == author)
            .map(|(id, (_, t, _))| (id.clone(), *t))
            .collect();
        ids.sort_by(|a, b| b.1.cmp(&a.1));
        ids.into_iter().filter_map(|(id, _)| self.document(&id)).collect()
    }

    /// The `trana://...` URIs that reference content `id` (backlinks) — works for any kind of target.
    pub fn backlinks(&self, id: &str) -> Vec<String> {
        self.backlinks.get(id).cloned().unwrap_or_default()
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

    /// The trust-weighted, time-decayed net score of a single target: every distinct voter
    /// contributes `sign(vote) * decay(age, half_life) * weight(voter)`. With `weight = |_| 1.0` and
    /// `half_life_secs = 0` this is exactly the raw net score (`ups - downs`). This is the primitive
    /// that makes a vote from a low-trust account barely move the needle.
    pub fn weighted_score(
        &self,
        target: &str,
        now_ms: u64,
        half_life_secs: u64,
        weight: &dyn Fn(&str) -> f64,
    ) -> f64 {
        match self.target_votes.get(target) {
            None => 0.0,
            Some(voters) => voters
                .iter()
                .map(|(voter, (val, ts))| {
                    let age = now_ms.saturating_sub(*ts) / 1000;
                    (*val as f64) * decay(age, half_life_secs) * weight(voter)
                })
                .sum(),
        }
    }

    /// The social-karma aggregate for a user, with the **effective** score computed under a voter
    /// `weight` function and time-decay (`half_life_secs`). The raw counts (posts, comments,
    /// up/downvotes, raw net scores, followers) are unaffected — only `effective_score` reflects the
    /// weighting. Pass `weight = |_| 1.0`, `half_life_secs = 0` for the raw, undecayed aggregate
    /// (see [`State::social`]).
    pub fn social_weighted(
        &self,
        node_id: &str,
        now_ms: u64,
        half_life_secs: u64,
        weight: &dyn Fn(&str) -> f64,
    ) -> crate::karma::SocialKarma {
        let mut k = crate::karma::SocialKarma::default();
        let mut effective = 0.0_f64;
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
            effective += self.weighted_score(id, now_ms, half_life_secs, weight);
        }
        // Documents are votable content too: their score counts toward the author's karma.
        for (id, (a, _, _)) in &self.documents {
            if a != node_id {
                continue;
            }
            let (ups, downs) = self.tally.get(id).copied().unwrap_or((0, 0));
            k.upvotes += ups;
            k.downvotes += downs;
            k.post_score += ups as i64 - downs as i64;
            effective += self.weighted_score(id, now_ms, half_life_secs, weight);
        }
        k.followers = self.followers(node_id).len() as u64;
        k.effective_score = effective;
        k
    }

    /// The raw social-karma aggregate (every voter counts 1, no decay). `effective_score` equals the
    /// raw [`crate::karma::SocialKarma::karma`]. The node uses [`State::social_weighted`] with a
    /// real voter-trust weight; this is the cheap, weightless view for clients without a rank table.
    pub fn social(&self, node_id: &str) -> crate::karma::SocialKarma {
        self.social_weighted(node_id, 0, 0, &|_| 1.0)
    }

    /// Has `device` published an active [`crate::model::DeviceLink`] declaring it belongs to `owner`?
    /// This is the device's half of the mutual binding; the owner's half is listing the device in
    /// their `Profile.devices`. Only when both hold does the device's compute count toward `owner`.
    pub fn is_device_linked(&self, owner: &str, device: &str) -> bool {
        matches!(self.device_links.get(device), Some((o, active, _)) if *active && o == owner)
    }

    /// Author of every explicitly-created board (deduplicated) — the in-log, deterministic seed set
    /// for [`State::trust_graph`]: the people the community let bootstrap its spaces.
    pub fn board_creators(&self) -> Vec<String> {
        let mut v: Vec<String> = self.boards.values().map(|b| b.creator.clone()).collect();
        v.sort();
        v.dedup();
        v
    }

    /// Web-of-trust propagation: a personalized PageRank over the **follow graph** (a follow is a
    /// vouch from follower → followee), restarting to `seeds` (the pre-trusted anchor set, e.g.
    /// board creators + configured roots). Returns each reachable node's rank normalized so the most
    /// trusted node is `1.0`. Deterministic in the folded follow set + seeds, so every node computes
    /// the same ranks. A sybil ring that only follows itself has no inbound edge from the
    /// seed-connected component, so its rank stays ~0 no matter how densely it cross-follows — which
    /// is what stops cheap karma farming when this rank weights votes.
    pub fn trust_graph(
        &self,
        seeds: &[(String, f64)],
        damping: f64,
        iters: usize,
    ) -> HashMap<String, f64> {
        // Adjacency over active follows.
        let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut nodes: HashSet<&str> = HashSet::new();
        for ((f, fe), active) in &self.follows {
            if *active {
                out.entry(f.as_str()).or_default().push(fe.as_str());
                nodes.insert(f.as_str());
                nodes.insert(fe.as_str());
            }
        }
        // Personalization (restart) vector from the seeds, normalized to sum 1.
        let mut seed: HashMap<&str, f64> = HashMap::new();
        let seed_sum: f64 = seeds.iter().map(|(_, w)| w.max(0.0)).sum();
        if seed_sum > 0.0 {
            for (n, w) in seeds {
                let w = w.max(0.0);
                if w > 0.0 {
                    nodes.insert(n.as_str());
                    *seed.entry(n.as_str()).or_insert(0.0) += w / seed_sum;
                }
            }
        } else {
            // No seeds: uniform restart over all nodes — still useful for ranking, but weaker sybil
            // resistance (no trust anchor). Callers should supply seeds in production.
            if nodes.is_empty() {
                return HashMap::new();
            }
            let u = 1.0 / nodes.len() as f64;
            for n in &nodes {
                seed.insert(n, u);
            }
        }
        let damping = damping.clamp(0.0, 0.99);
        let mut rank: HashMap<&str, f64> = seed.clone();
        for _ in 0..iters.max(1) {
            let mut next: HashMap<&str, f64> = HashMap::new();
            for (n, s) in &seed {
                *next.entry(n).or_insert(0.0) += (1.0 - damping) * s;
            }
            for (src, dsts) in &out {
                let r = rank.get(src).copied().unwrap_or(0.0);
                if r <= 0.0 || dsts.is_empty() {
                    continue;
                }
                let share = damping * r / dsts.len() as f64;
                for d in dsts {
                    *next.entry(d).or_insert(0.0) += share;
                }
            }
            rank = next;
        }
        // Normalize so the top node is 1.0 → ranks read as a 0..1 trust fraction.
        let max = rank.values().copied().fold(0.0_f64, f64::max);
        if max > 0.0 {
            for v in rank.values_mut() {
                *v /= max;
            }
        }
        rank.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
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

/// A unified line diff (LCS-based) between two texts — the diff-tracking under document versioning.
fn line_diff(a: &str, b: &str) -> Vec<DiffLine> {
    let al: Vec<&str> = a.lines().collect();
    let bl: Vec<&str> = b.lines().collect();
    let (n, m) = (al.len(), bl.len());
    // LCS length table.
    let mut dp = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i][j] = if al[i] == bl[j] { dp[i + 1][j + 1] + 1 } else { dp[i + 1][j].max(dp[i][j + 1]) };
        }
    }
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    let line = |op: &str, t: &str| DiffLine { op: op.into(), text: t.into() };
    while i < n && j < m {
        if al[i] == bl[j] {
            out.push(line(" ", al[i]));
            i += 1;
            j += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            out.push(line("-", al[i]));
            i += 1;
        } else {
            out.push(line("+", bl[j]));
            j += 1;
        }
    }
    while i < n {
        out.push(line("-", al[i]));
        i += 1;
    }
    while j < m {
        out.push(line("+", bl[j]));
        j += 1;
    }
    out
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
        // Raw social() => effective_score equals raw karma (every voter counts 1, no decay).
        assert!((k.effective_score - k.karma() as f64).abs() < 1e-9);
    }

    #[test]
    fn effective_karma_is_trust_weighted() {
        // The Reddit-reliability property: karma you cannot farm. A post upvoted by one trusted
        // member and a swarm of zero-trust sybils earns ~1 effective karma, not 3.
        let mut s = State::new();
        let author = "aa".repeat(32);
        let trusted = "bb".repeat(32);
        let sybil1 = "c1".repeat(32);
        let sybil2 = "c2".repeat(32);
        let r = root(&author, "b", 1, "t");
        s.apply(&r);
        s.apply(&vote(&trusted, 10, &r.id, 1));
        s.apply(&vote(&sybil1, 10, &r.id, 1));
        s.apply(&vote(&sybil2, 10, &r.id, 1));
        assert_eq!(s.social(&author).karma(), 3, "raw karma counts every vote");
        let weight = |v: &str| if v == trusted.as_str() { 1.0 } else { 0.0 };
        let k = s.social_weighted(&author, 100_000, 0, &weight);
        assert!((k.effective_score - 1.0).abs() < 1e-9, "only the trusted upvote moves karma");
        assert_eq!(k.karma(), 3, "raw counts are untouched by weighting");
    }

    #[test]
    fn weighted_score_decays_with_age() {
        let mut s = State::new();
        let author = "aa".repeat(32);
        let voter = "bb".repeat(32);
        let r = root(&author, "b", 0, "t");
        s.apply(&r);
        s.apply(&vote(&voter, 0, &r.id, 1)); // vote at t = 0 ms
        let unit = |_: &str| 1.0;
        let fresh = s.weighted_score(&r.id, 0, 100, &unit);
        let aged = s.weighted_score(&r.id, 100_000, 100, &unit); // 100 s old, 100 s half-life
        assert!((fresh - 1.0).abs() < 1e-9);
        assert!((aged - 0.5).abs() < 1e-9, "one half-life halves the vote weight, got {aged}");
    }

    #[test]
    fn trust_graph_isolates_sybil_ring() {
        // A seed vouches (follows) one honest node. A dense sybil ring only cross-follows itself.
        // Seeded propagation gives the honest node real rank and the ring ~0 — so cross-voting
        // cannot bootstrap trust.
        let mut s = State::new();
        let seed = "aa".repeat(32);
        let honest = "bb".repeat(32);
        let s1 = "c1".repeat(32);
        let s2 = "c2".repeat(32);
        let s3 = "c3".repeat(32);
        let follow = |a: &str, b: &str, t: u64| {
            Record::new(a, t, Body::Follow(Follow { followee: b.into(), active: true })).unwrap()
        };
        s.apply(&follow(&seed, &honest, 1));
        s.apply(&follow(&s1, &s2, 1));
        s.apply(&follow(&s2, &s3, 1));
        s.apply(&follow(&s3, &s1, 1));
        s.apply(&follow(&s1, &s3, 1));
        let ranks = s.trust_graph(&[(seed.clone(), 1.0)], 0.85, 30);
        let honest_rank = ranks.get(&honest).copied().unwrap_or(0.0);
        let sybil_rank = ranks.get(&s1).copied().unwrap_or(0.0);
        assert!(honest_rank > 0.5, "seed-vouched node gains real rank, got {honest_rank}");
        assert!(sybil_rank < 1e-6, "isolated sybil ring stays ~0, got {sybil_rank}");
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
    fn device_link_requires_device_consent() {
        use crate::model::DeviceLink;
        let mut s = State::new();
        let owner = "aa".repeat(32);
        let device = "bb".repeat(32);
        let link = |owner: &str, active: bool, t: u64| {
            Record::new(&device, t, Body::DeviceLink(DeviceLink { owner: owner.into(), active }))
                .unwrap()
        };
        // A profile naming a device is not enough — without the device's own link it stays unbound.
        assert!(!s.is_device_linked(&owner, &device));
        // The device signs its consent (author = device).
        s.apply(&link(&owner, true, 5));
        assert!(s.is_device_linked(&owner, &device));
        // The device only consented to `owner`; it is not bound to anyone else.
        assert!(!s.is_device_linked(&"cc".repeat(32), &device));
        // The device can revoke (last-write-wins by time)...
        s.apply(&link(&owner, false, 6));
        assert!(!s.is_device_linked(&owner, &device));
        // ...and a stale older re-link must not resurrect the binding.
        s.apply(&link(&owner, true, 4));
        assert!(!s.is_device_linked(&owner, &device));
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

    // ----- community governance -----
    use crate::model::{BanVote, BoardCreate, BoardPolicy, PolicyProposal, PolicyVote};

    fn ban_vote(voter: &str, board: &str, target: &str, support: bool, t: u64) -> Record {
        Record::new(
            voter,
            t,
            Body::BanVote(BanVote { board: board.into(), target: target.into(), support, reason: "".into() }),
        )
        .unwrap()
    }

    #[test]
    fn board_create_first_claim_wins() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let mut pol = BoardPolicy::default();
        pol.min_trust_to_vote = 0.3;
        // Two claims of the same board; the earlier (smaller created_ms) must win.
        let late = Record::new(&a, 50, Body::BoardCreate(BoardCreate {
            board: "ce".into(), title: "Late".into(), description: "".into(), policy: BoardPolicy::default(),
        })).unwrap();
        let early = Record::new(&b, 10, Body::BoardCreate(BoardCreate {
            board: "ce".into(), title: "Early".into(), description: "first".into(), policy: pol,
        })).unwrap();
        // Apply out of order: late first, then early.
        s.apply(&late);
        s.apply(&early);
        let bv = s.board("ce");
        assert_eq!(bv.title, "Early", "earliest claim wins regardless of arrival order");
        assert!((bv.policy.min_trust_to_vote - 0.3).abs() < 1e-9);
    }

    #[test]
    fn community_ban_needs_quorum_and_majority() {
        let mut s = State::new();
        let target = "ff".repeat(32);
        // A board with a small quorum so the test is concise.
        let mut pol = BoardPolicy::default();
        pol.ban_quorum = 3;
        pol.ban_support = 0.66;
        s.apply(&Record::new(&"00".repeat(32), 1, Body::BoardCreate(BoardCreate {
            board: "b".into(), title: "".into(), description: "".into(), policy: pol,
        })).unwrap());

        // 2 support, 0 oppose: below quorum (3) → not banned.
        s.apply(&ban_vote(&"11".repeat(32), "b", &target, true, 2));
        s.apply(&ban_vote(&"22".repeat(32), "b", &target, true, 2));
        assert!(!s.is_banned_raw("b", &target));

        // 3rd supporter → quorum met, 3/3 in favor → banned.
        s.apply(&ban_vote(&"33".repeat(32), "b", &target, true, 2));
        assert!(s.is_banned_raw("b", &target));
        let st = s.ban_standing("b", &target);
        assert_eq!((st.support, st.oppose), (3, 0));

        // Opposition pulls it back below the 66% support fraction → un-banned (community keeps them).
        s.apply(&ban_vote(&"44".repeat(32), "b", &target, false, 3));
        s.apply(&ban_vote(&"55".repeat(32), "b", &target, false, 3));
        assert!(!s.is_banned_raw("b", &target), "3/5 = 60% < 66% support → not banned");
    }

    #[test]
    fn banned_authors_content_is_hidden() {
        let mut s = State::new();
        let outcast = "ee".repeat(32);
        let mut pol = BoardPolicy::default();
        pol.ban_quorum = 2;
        s.apply(&Record::new(&"00".repeat(32), 1, Body::BoardCreate(BoardCreate {
            board: "b".into(), title: "".into(), description: "".into(), policy: pol,
        })).unwrap());
        let p = root(&outcast, "b", 5, "unpopular take");
        s.apply(&p);
        assert_eq!(s.threads("b", SortBy::New, 10, 100).len(), 1);
        // Community bans the author.
        s.apply(&ban_vote(&"11".repeat(32), "b", &outcast, true, 6));
        s.apply(&ban_vote(&"22".repeat(32), "b", &outcast, true, 6));
        assert!(s.is_banned_raw("b", &outcast));
        assert_eq!(s.threads("b", SortBy::New, 10, 100).len(), 0, "banned author hidden from feed");
    }

    #[test]
    fn grace_window_gives_controversial_posts_visibility() {
        // The signature behavior: during the grace window a heavily-DOWNVOTED young post still
        // outranks an old, mildly-positive post in the default (Hot) feed — controversy gets seen.
        let mut s = State::new();
        let board = "b"; // default policy: 6h grace
        let now: u64 = 100_000_000; // ms
        // Old post (10h old, +5 net) — well past grace.
        let old = root(&"a1".repeat(16).repeat(2), board, now - 10 * 3_600_000, "old & liked");
        // Young controversial post (10 min old, net NEGATIVE but lots of engagement).
        let young = root(&"b1".repeat(16).repeat(2), board, now - 10 * 60_000, "hot take");
        s.apply(&old);
        s.apply(&young);
        // old: +5 net (5 ups)
        for i in 0..5 { s.apply(&vote(&format!("{:064x}", 1000 + i), now, &old.id, 1)); }
        // young: 3 up / 9 down = net -6, engagement 12
        for i in 0..3 { s.apply(&vote(&format!("{:064x}", 2000 + i), now, &young.id, 1)); }
        for i in 0..9 { s.apply(&vote(&format!("{:064x}", 3000 + i), now, &young.id, -1)); }

        let feed = s.threads(board, SortBy::Hot, 10, now);
        assert_eq!(feed[0].id, young.id, "controversial young post is surfaced first during grace");
        // But Top (pure reception) correctly ranks the liked old post above the booed one.
        let top = s.threads(board, SortBy::Top, 10, now);
        assert_eq!(top[0].id, old.id);
        // Controversial sort surfaces the split post.
        let contro = s.threads(board, SortBy::Controversial, 10, now);
        assert_eq!(contro[0].id, young.id);
    }

    #[test]
    fn policy_proposals_and_votes() {
        let mut s = State::new();
        let prop = Record::new(&"aa".repeat(32), 1, Body::PolicyProposal(PolicyProposal {
            board: Some("b".into()), title: "No spam".into(), body: "links only with context".into(),
        })).unwrap();
        s.apply(&prop);
        s.apply(&Record::new(&"11".repeat(32), 2, Body::PolicyVote(PolicyVote { proposal: prop.id.clone(), support: true })).unwrap());
        s.apply(&Record::new(&"22".repeat(32), 2, Body::PolicyVote(PolicyVote { proposal: prop.id.clone(), support: true })).unwrap());
        s.apply(&Record::new(&"33".repeat(32), 2, Body::PolicyVote(PolicyVote { proposal: prop.id.clone(), support: false })).unwrap());
        let pv = s.proposal(&prop.id).unwrap();
        assert_eq!((pv.favor, pv.against), (2, 1));
        assert_eq!(s.proposals(Some("b")).len(), 1);
        assert_eq!(s.proposals(Some("other")).len(), 0);
    }

    #[test]
    fn home_feed_is_follows_only() {
        let mut s = State::new();
        let me = "aa".repeat(32);
        let friend = "bb".repeat(32);
        let stranger = "cc".repeat(32);
        s.apply(&Record::new(&me, 1, Body::Follow(crate::model::Follow { followee: friend.clone(), active: true })).unwrap());
        let fp = root(&friend, "b", 10, "from a friend");
        let sp = root(&stranger, "b", 11, "from a stranger");
        s.apply(&fp);
        s.apply(&sp);
        let home = s.home_feed(&me, SortBy::New, 10, 100);
        assert_eq!(home.len(), 1);
        assert_eq!(home[0].id, fp.id);
        // The global feed shows both.
        assert_eq!(s.all_feed(SortBy::New, 10, 100).len(), 2);
    }

    // ----- content addressing + documents -----
    use crate::model::{extract_refs, Document, Ref};

    #[test]
    fn ref_uri_roundtrip_and_extract() {
        assert_eq!(Ref::post("abc").to_uri(), "trana://post/abc");
        assert_eq!(Ref::parse("trana://post/abc"), Some(Ref::post("abc")));
        assert_eq!(Ref::parse("https://example.com"), None);
        assert_eq!(Ref::parse("trana://post/"), None);
        let md = "see [this](trana://document/d1) and trana://media/m2, also trana://post/p3.";
        let refs = extract_refs(md);
        assert_eq!(refs, vec![Ref::document("d1"), Ref::media("m2"), Ref::post("p3")]);
    }

    #[test]
    fn documents_refs_backlinks_votes_and_karma() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let p0 = root(&a, "b", 1, "target");
        s.apply(&p0);
        let doc = Record::new(
            &a,
            2,
            Body::Document(Document {
                title: "essay".into(),
                body: format!("citing trana://post/{}", p0.id),
                file: None,
                refs: vec![Ref::media("m9")],
                board: Some("b".into()),
                series: None,
                prev: None,
            }),
        )
        .unwrap();
        s.apply(&doc);

        let dv = s.document(&doc.id).unwrap();
        assert!(dv.refs.contains(&Ref::media("m9").to_uri()), "declared ref present");
        assert!(dv.refs.contains(&Ref::post(&p0.id).to_uri()), "inline markdown ref parsed");
        // Backlink: the post knows the document references it.
        assert_eq!(s.backlinks(&p0.id), vec![Ref::document(&doc.id).to_uri()]);
        // Documents are votable and contribute to the author's karma.
        s.apply(&vote(&"bb".repeat(32), 3, &doc.id, 1));
        assert_eq!(s.document(&doc.id).unwrap().score, 1);
        assert_eq!(s.social(&a).post_score, 1);
        assert_eq!(s.documents_by(&a).len(), 1);
    }

    fn doc(author: &str, t: u64, title: &str, body: &str, series: Option<&str>, prev: Option<&str>) -> Record {
        Record::new(
            author,
            t,
            Body::Document(Document {
                title: title.into(),
                body: body.into(),
                file: None,
                refs: vec![],
                board: None,
                series: series.map(|s| s.into()),
                prev: prev.map(|s| s.into()),
            }),
        )
        .unwrap()
    }

    #[test]
    fn documents_are_versioned_and_diffable() {
        let mut s = State::new();
        let a = "aa".repeat(32);
        let v1 = doc(&a, 1, "notes", "line one\nline two\n", None, None);
        s.apply(&v1);
        let v2 = doc(&a, 2, "notes", "line one\nline two changed\nline three\n", Some(&v1.id), Some(&v1.id));
        s.apply(&v2);
        let v3 = doc(&a, 3, "notes", "line one\nline two changed\nline three\n", Some(&v1.id), Some(&v2.id));
        s.apply(&v3);

        // History is the full chain, oldest -> newest.
        let hist = s.document_history(&v2.id);
        assert_eq!(hist.iter().map(|d| d.id.clone()).collect::<Vec<_>>(), vec![v1.id.clone(), v2.id.clone(), v3.id.clone()]);
        // Version numbering + latest flag.
        assert_eq!(s.document(&v1.id).unwrap().version, 1);
        assert_eq!(s.document(&v2.id).unwrap().version, 2);
        assert_eq!(s.document(&v2.id).unwrap().versions, 3);
        assert!(!s.document(&v2.id).unwrap().is_latest);
        assert!(s.document(&v3.id).unwrap().is_latest);
        assert_eq!(s.document_latest(&v1.id).unwrap().id, v3.id);

        // Diff v1 -> v2: line two replaced, line three added.
        let d = s.document_diff(&v1.id, &v2.id);
        assert!(d.iter().any(|l| l.op == "-" && l.text == "line two"));
        assert!(d.iter().any(|l| l.op == "+" && l.text == "line two changed"));
        assert!(d.iter().any(|l| l.op == "+" && l.text == "line three"));
        // The series collapses to one identity despite three records.
        assert_eq!(s.documents_by(&a).len(), 3); // each version is a record...
        assert_eq!(s.document_history(&v1.id).len(), 3); // ...but they share one series.
    }
}
