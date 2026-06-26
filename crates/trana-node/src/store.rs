//! Local persistence: an append-only record log plus the in-memory [`State`] folded from it.
//!
//! Every accepted [`Record`] is appended to `<data>/trana/log.jsonl` and folded into a [`State`] held
//! behind a `std::Mutex`. On startup the log is replayed to rebuild the view. The log is the local
//! durability anchor; cross-node durability comes from gossip + object replication ([`crate::replicate`]).
//! Records are content-addressed, so replay and gossip are both idempotent — applying the same id
//! twice is a no-op, which is exactly what makes the store safe to feed from many sources at once.

use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use trana_core::karma::SocialKarma;
use trana_core::record::Record;
use trana_core::state::{PostView, ProfileView, SortBy, State, StreamView};
use trana_core::Media;

/// The durable store: a mutex-guarded [`State`] backed by an append-only JSONL log.
pub struct Store {
    state: Mutex<State>,
    log: Mutex<File>,
    log_path: PathBuf,
}

impl Store {
    /// Open (or create) the store under `data_dir`, replaying any existing log into the read-model.
    pub fn open(data_dir: &Path) -> Result<Store> {
        let dir = data_dir.join("trana");
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let log_path = dir.join("log.jsonl");

        let mut state = State::new();
        let mut replayed = 0usize;
        if log_path.exists() {
            let f = File::open(&log_path).with_context(|| format!("open {}", log_path.display()))?;
            for line in BufReader::new(f).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Record>(&line) {
                    Ok(rec) => match rec.verify() {
                        Ok(()) => {
                            if state.apply(&rec) {
                                replayed += 1;
                            }
                        }
                        Err(e) => tracing::warn!(id = %rec.id, error = %e, "replay: skipping invalid record"),
                    },
                    Err(e) => tracing::warn!(error = %e, "replay: skipping unparseable log line"),
                }
            }
        }
        tracing::info!(records = replayed, path = %log_path.display(), "trana store opened");

        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("open append {}", log_path.display()))?;

        Ok(Store { state: Mutex::new(state), log: Mutex::new(log), log_path })
    }

    /// Validate and fold a record in. Returns `true` if it was newly applied (and persisted), `false`
    /// if it was a duplicate. Invalid records are rejected with an error. Safe to call from the local
    /// API, gossip, and directed replication alike.
    pub fn ingest(&self, rec: &Record) -> Result<bool> {
        rec.verify().map_err(|e| anyhow::anyhow!("invalid record: {e}"))?;
        let newly = {
            let mut st = self.state.lock().unwrap();
            st.apply(rec)
        };
        if newly {
            let line = serde_json::to_string(rec)?;
            let mut log = self.log.lock().unwrap();
            writeln!(log, "{line}")?;
            log.flush()?;
        }
        Ok(newly)
    }

    /// Path to the on-disk log (for diagnostics).
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// Number of distinct records held.
    pub fn len(&self) -> usize {
        self.state.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // ----- read-through queries (each takes + releases the lock; never held across an await) -----

    pub fn profile(&self, node_id: &str) -> Option<ProfileView> {
        self.state.lock().unwrap().profile(node_id)
    }

    pub fn social(&self, node_id: &str) -> SocialKarma {
        self.state.lock().unwrap().social(node_id)
    }

    /// Trust-weighted, time-decayed social aggregate (the node's real karma view). `weight` maps a
    /// voter to their trust weight (its web-of-trust rank).
    pub fn social_weighted(
        &self,
        node_id: &str,
        now_ms: u64,
        half_life_secs: u64,
        weight: &dyn Fn(&str) -> f64,
    ) -> SocialKarma {
        self.state.lock().unwrap().social_weighted(node_id, now_ms, half_life_secs, weight)
    }

    /// Web-of-trust ranks over the follow graph, restarting to `seeds`. See
    /// [`trana_core::state::State::trust_graph`].
    pub fn trust_graph(
        &self,
        seeds: &[(String, f64)],
        damping: f64,
        iters: usize,
    ) -> std::collections::HashMap<String, f64> {
        self.state.lock().unwrap().trust_graph(seeds, damping, iters)
    }

    /// Board creators — the in-log seed anchor for the web of trust.
    pub fn board_creators(&self) -> Vec<String> {
        self.state.lock().unwrap().board_creators()
    }

    pub fn media(&self, media_id: &str) -> Option<Media> {
        self.state.lock().unwrap().media(media_id)
    }

    pub fn post(&self, id: &str) -> Option<PostView> {
        self.state.lock().unwrap().post(id)
    }

    pub fn threads(&self, board: &str, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        self.state.lock().unwrap().threads(board, sort, limit, now_ms)
    }

    pub fn comments(&self, root: &str, sort: SortBy, now_ms: u64) -> Vec<PostView> {
        self.state.lock().unwrap().comments(root, sort, now_ms)
    }

    pub fn stream(&self, id: &str) -> Option<StreamView> {
        self.state.lock().unwrap().stream(id)
    }

    pub fn live_streams(&self) -> Vec<StreamView> {
        self.state.lock().unwrap().live_streams()
    }

    // ----- community governance -----

    pub fn board(&self, board: &str) -> trana_core::state::BoardView {
        self.state.lock().unwrap().board(board)
    }

    pub fn boards(&self) -> Vec<trana_core::state::BoardView> {
        self.state.lock().unwrap().boards()
    }

    pub fn board_policy(&self, board: &str) -> trana_core::BoardPolicy {
        self.state.lock().unwrap().board_policy(board)
    }

    pub fn ban_standing(&self, board: &str, target: &str) -> trana_core::state::BanStanding {
        self.state.lock().unwrap().ban_standing(board, target)
    }

    pub fn ban_votes_raw(&self, board: &str, target: &str) -> Vec<(String, bool)> {
        self.state.lock().unwrap().ban_votes_raw(board, target)
    }

    pub fn proposal(&self, id: &str) -> Option<trana_core::state::ProposalView> {
        self.state.lock().unwrap().proposal(id)
    }

    pub fn proposals(&self, board: Option<&str>) -> Vec<trana_core::state::ProposalView> {
        self.state.lock().unwrap().proposals(board)
    }

    pub fn all_feed(&self, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        self.state.lock().unwrap().all_feed(sort, limit, now_ms)
    }

    pub fn home_feed(&self, viewer: &str, sort: SortBy, limit: usize, now_ms: u64) -> Vec<PostView> {
        self.state.lock().unwrap().home_feed(viewer, sort, limit, now_ms)
    }

    // ----- documents + versioning -----

    pub fn document(&self, id: &str) -> Option<trana_core::state::DocumentView> {
        self.state.lock().unwrap().document(id)
    }
    pub fn document_history(&self, key: &str) -> Vec<trana_core::state::DocumentView> {
        self.state.lock().unwrap().document_history(key)
    }
    pub fn document_latest(&self, key: &str) -> Option<trana_core::state::DocumentView> {
        self.state.lock().unwrap().document_latest(key)
    }
    pub fn document_diff(&self, from: &str, to: &str) -> Vec<trana_core::state::DiffLine> {
        self.state.lock().unwrap().document_diff(from, to)
    }
    pub fn documents_by(&self, author: &str) -> Vec<trana_core::state::DocumentView> {
        self.state.lock().unwrap().documents_by(author)
    }
    pub fn backlinks(&self, id: &str) -> Vec<String> {
        self.state.lock().unwrap().backlinks(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trana_core::model::{Body, Post};

    fn post_rec(author: &str, t: u64) -> Record {
        Record::new(
            author,
            t,
            Body::Post(Post {
                board: "b".into(),
                parent: None,
                title: Some("t".into()),
                body: "x".into(),
                media: vec![],
            }),
        )
        .unwrap()
    }

    #[test]
    fn ingest_persists_and_dedupes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let r = post_rec(&"aa".repeat(32), 1);
        assert!(store.ingest(&r).unwrap());
        assert!(!store.ingest(&r).unwrap(), "duplicate is not re-applied");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn reopen_replays_log() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path()).unwrap();
            store.ingest(&post_rec(&"aa".repeat(32), 1)).unwrap();
            store.ingest(&post_rec(&"aa".repeat(32), 2)).unwrap();
        }
        // Reopen: the two records must come back from the log.
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(store.threads("b", SortBy::New, 10, 100).len(), 2);
    }

    #[test]
    fn invalid_record_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let mut r = post_rec(&"aa".repeat(32), 1);
        r.id = "deadbeef".into(); // corrupt the content address
        assert!(store.ingest(&r).is_err());
    }
}
