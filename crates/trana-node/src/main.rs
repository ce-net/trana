//! `trana-node` — run the trana distributed backend against a local CE node.
//!
//! It joins the mesh as the `trana` service, serves every `trana/*` RPC, replicates via gossip, and
//! keeps itself discoverable. Optionally (feature `gateway`) it also exposes a local REST facade so
//! plain-HTTP frontends can build on the same engine.

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use trana_node::{run, Node};

#[derive(Parser)]
#[command(name = "trana-node", about = "trana distributed social/content backend (mesh-native)")]
struct Cli {
    /// Local CE node HTTP API base URL.
    #[arg(long, default_value = ce_rs::DEFAULT_BASE_URL)]
    node_url: String,

    /// Data directory (the trana log lives in `<data-dir>/trana/`). Defaults to the CE data dir.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// If set (and built with `--features gateway`), serve a local REST facade on this port.
    #[arg(long)]
    gateway_port: Option<u16>,
}

/// Default data dir: the CE node's data dir, so trana state sits beside the chain/key.
fn default_data_dir() -> PathBuf {
    directories::ProjectDirs::from("", "", "ce")
        .map(|d| d.data_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from(".trana"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trana_node=info,trana=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);
    let ce = ce_rs::CeClient::new(cli.node_url);

    // Fail fast with a clear message if the local node isn't up.
    ce.health().await.context("local CE node not reachable — is `ce start` running?")?;

    let node = Node::open(ce, &data_dir).await?;
    tracing::info!(node = %node.self_id, data = %data_dir.display(), "trana-node ready");

    // Optional HTTP gateway.
    #[cfg(feature = "gateway")]
    if let Some(port) = cli.gateway_port {
        let engine = node.engine.clone();
        let self_id = node.self_id.clone();
        tokio::spawn(async move {
            if let Err(e) = trana_node::gateway::serve(engine, self_id, port).await {
                tracing::error!(error = %e, "gateway exited with error");
            }
        });
    }
    #[cfg(not(feature = "gateway"))]
    if cli.gateway_port.is_some() {
        tracing::warn!("--gateway-port set but binary was built without the `gateway` feature; ignoring");
    }

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
    };
    run(node, shutdown).await
}
