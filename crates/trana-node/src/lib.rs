//! trana-node — the distributed backend daemon, as a library.
//!
//! Wire it up with [`run`]: open the [`store::Store`], build the [`engine::Engine`], then run three
//! cooperating tasks until shutdown — the mesh [`service::TranaService`] serve loop, the gossip
//! replication subscriber, and the DHT re-advertise heartbeat that keeps the node discoverable via
//! `locate`. The binary in `main.rs` is a thin CLI over this.

pub mod auth;
pub mod compute;
pub mod engine;
pub mod replicate;
pub mod service;
pub mod store;

#[cfg(feature = "gateway")]
pub mod gateway;

use anyhow::{Context, Result};
use ce_rs::CeClient;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::compute::ComputeProbe;
use crate::engine::Engine;
use crate::replicate::Replicator;
use crate::service::TranaService;
use crate::store::Store;

/// How often to re-advertise the trana service on the DHT (provider records expire).
const ADVERTISE_INTERVAL: Duration = Duration::from_secs(60);

/// Everything the node needs, assembled and ready to serve. Build with [`Node::open`].
pub struct Node {
    pub ce: CeClient,
    pub self_id: String,
    pub engine: Arc<Engine>,
    pub store: Arc<Store>,
}

impl Node {
    /// Open the store under `data_dir` and assemble the engine against the CE node at `ce`.
    pub async fn open(ce: CeClient, data_dir: &Path) -> Result<Node> {
        let self_id = ce.status().await.context("query local node status")?.node_id;
        let store = Arc::new(Store::open(data_dir)?);
        let compute = ComputeProbe::new(ce.clone());
        let replicator = Replicator::new(ce.clone(), self_id.clone());
        let engine = Arc::new(Engine::new(store.clone(), ce.clone(), compute, replicator));
        Ok(Node { ce, self_id, engine, store })
    }
}

/// Run the node until `shutdown` resolves: serve mesh RPCs, replicate via gossip, and stay
/// discoverable. Returns once all tasks have stopped.
pub async fn run(node: Node, shutdown: impl std::future::Future<Output = ()> + Send + 'static) -> Result<()> {
    let Node { ce, self_id, engine, store } = node;
    tracing::info!(node = %self_id, records = store.len(), "trana-node starting");

    // A single shutdown signal fanned out to every task.
    let (tx, _rx) = tokio::sync::broadcast::channel::<()>(1);
    let signal_tx = tx.clone();
    tokio::spawn(async move {
        shutdown.await;
        let _ = signal_tx.send(());
    });
    let sub = |tx: &tokio::sync::broadcast::Sender<()>| {
        let mut rx = tx.subscribe();
        async move {
            let _ = rx.recv().await;
        }
    };

    // 1. Serve the mesh RPC topics.
    let service = TranaService::new(engine.clone());
    let serve = {
        let ce = ce.clone();
        let stop = sub(&tx);
        tokio::spawn(async move {
            if let Err(e) = ce_rs::serve::serve(&ce, trana_core::proto::RPC_TOPICS, &service, stop).await {
                tracing::error!(error = %e, "serve loop exited with error");
            }
        })
    };

    // 2. Replicate via gossip.
    let gossip = {
        let ce = ce.clone();
        let store = store.clone();
        let replicator = engine.replicator().clone();
        let stop = sub(&tx);
        tokio::spawn(async move {
            if let Err(e) = replicate::run_gossip(ce, store, replicator, stop).await {
                tracing::error!(error = %e, "gossip loop exited with error");
            }
        })
    };

    // 3. Stay discoverable via the DHT.
    let advertise = {
        let ce = ce.clone();
        let stop = sub(&tx);
        tokio::spawn(async move {
            if let Err(e) =
                ce_rs::locate::register(&ce, trana_core::proto::SERVICE, ADVERTISE_INTERVAL, stop).await
            {
                tracing::error!(error = %e, "advertise loop exited with error");
            }
        })
    };

    let _ = tokio::try_join!(serve, gossip, advertise);
    tracing::info!("trana-node stopped");
    Ok(())
}
