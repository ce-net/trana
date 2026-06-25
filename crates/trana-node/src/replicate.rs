//! Replication: gossip records network-wide and pin media bytes on nearby nodes.
//!
//! Two mechanisms, both content-addressed and idempotent:
//!
//! 1. **Record gossip.** Every accepted record is published on the [`GOSSIP`](proto::GOSSIP) pub/sub
//!    topic. Every trana node subscribes, so the (tiny) social graph converges everywhere. A node
//!    folding a gossiped record is a no-op if it already has that id.
//! 2. **Object placement.** A record's (large) media bytes are *not* in the gossip; they live in CE's
//!    object store. After a write, the origin asks a few **nearby** trana instances (picked by
//!    `locate`, which ranks by trust + capacity + recency and spreads across fault domains) to pull
//!    and pin those object CIDs, so the bytes live close to where they'll be served from.
//!
//! "Auto-spawning on closeby nodes" is exactly this placement step: [`select_replicas`] is the same
//! selection a deployer would feed to `mesh_deploy` to stand up a new replica; here we use it to push
//! pins to already-running instances.

use anyhow::Result;
use ce_rs::locate::{locate, LocateOpts};
use ce_rs::CeClient;
use std::sync::Arc;
use std::time::Duration;
use trana_core::proto::{self, ReplicateReq};
use trana_core::record::Record;

use crate::store::Store;

/// Default number of nearby replicas to pin media bytes onto.
pub const DEFAULT_REPLICAS: usize = 3;

/// Per-replica request timeout.
const REPLICATE_TIMEOUT_MS: u64 = 8_000;

/// Drives gossip + nearby placement for one node.
#[derive(Clone)]
pub struct Replicator {
    ce: CeClient,
    self_id: String,
}

impl Replicator {
    pub fn new(ce: CeClient, self_id: String) -> Self {
        Replicator { ce, self_id }
    }

    /// Gossip a freshly-accepted record to the whole network and, if it carries object CIDs, push
    /// pin requests to a few nearby instances. Best-effort: replication failures are logged, never
    /// fatal to the originating write.
    pub async fn broadcast(&self, record: Record, object_cids: Vec<String>, replicas: usize) {
        let req = ReplicateReq { record, object_cids: object_cids.clone() };
        let payload = match serde_json::to_vec(&req) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "replicate: encode failed");
                return;
            }
        };

        // 1. Network-wide record gossip.
        if let Err(e) = self.ce.publish(proto::GOSSIP, &payload).await {
            tracing::warn!(error = %e, "replicate: gossip publish failed");
        }

        // 2. Directed pin of media bytes onto nearby instances (only when there are bytes to place).
        if object_cids.is_empty() || replicas == 0 {
            return;
        }
        let targets = match self.select_replicas(replicas).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "replicate: peer selection failed");
                return;
            }
        };
        for node in targets {
            match self.ce.request(&node, proto::T_REPLICATE, &payload, REPLICATE_TIMEOUT_MS).await {
                Ok(_) => tracing::debug!(node = %node, cids = object_cids.len(), "replicate: pinned on peer"),
                Err(e) => tracing::warn!(node = %node, error = %e, "replicate: peer pin failed"),
            }
        }
    }

    /// Pick up to `want` nearby live trana instances (excluding self) to host replicas.
    pub async fn select_replicas(&self, want: usize) -> Result<Vec<String>> {
        let opts = LocateOpts { want: want + 1, ..Default::default() };
        let instances = locate(&self.ce, proto::SERVICE, &opts).await?;
        Ok(instances
            .into_iter()
            .map(|i| i.node_id)
            .filter(|id| id != &self.self_id)
            .take(want)
            .collect())
    }

    /// Pull and pin the object CIDs in a replication request into the local CE object store, so this
    /// node now holds a copy. Verification is intrinsic — `get_object` checks every chunk's hash.
    pub async fn pull_objects(&self, cids: &[String]) {
        for cid in cids {
            match self.ce.get_object(cid).await {
                Ok(bytes) => tracing::debug!(cid = %cid, bytes = bytes.len(), "replicate: pulled object"),
                Err(e) => tracing::warn!(cid = %cid, error = %e, "replicate: object pull failed"),
            }
        }
    }
}

/// Background task: subscribe to the gossip topic and fold every replicated record into the store,
/// pulling any referenced media bytes. Runs until `shutdown` resolves. This is what makes a node a
/// passive replica of the whole network's social graph, not just a server of its own writes.
pub async fn run_gossip(
    ce: CeClient,
    store: Arc<Store>,
    replicator: Replicator,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    use futures_util::StreamExt as _;

    ce.subscribe(proto::GOSSIP).await?;
    let mut backoff = Duration::from_millis(250);
    tokio::pin!(shutdown);

    loop {
        let stream = match ce.messages_stream().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "gossip: stream open failed; backing off");
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(10));
                continue;
            }
        };
        backoff = Duration::from_millis(250);
        tokio::pin!(stream);

        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                item = stream.next() => match item {
                    Some(Ok(m)) => {
                        // Only gossip pub/sub messages (no reply_token) on our topic concern us;
                        // request messages are handled by the serve loop.
                        if m.topic != proto::GOSSIP || m.reply_token.is_some() {
                            continue;
                        }
                        let Ok(payload) = m.payload() else { continue };
                        ingest_replication(&store, &replicator, &payload).await;
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "gossip: stream error; reconnecting");
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}

/// Decode a [`ReplicateReq`], fold its record in, and pull its objects. Shared by the gossip loop and
/// the directed `T_REPLICATE` handler.
pub async fn ingest_replication(store: &Store, replicator: &Replicator, payload: &[u8]) -> bool {
    let req: ReplicateReq = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "replicate: bad payload");
            return false;
        }
    };
    let newly = match store.ingest(&req.record) {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "replicate: rejected record");
            return false;
        }
    };
    // Pull bytes only for records we hadn't already seen (avoids redundant fetches on re-gossip).
    if newly && !req.object_cids.is_empty() {
        replicator.pull_objects(&req.object_cids).await;
    }
    newly
}
