//! # trana-core — the reusable heart of trana
//!
//! trana is a **distributed social/content backend** built on CE primitives: threads (Reddit-like
//! discussion), posts, images, video, audio, podcasts, documents, and live streaming — every record
//! content-addressed and replicated across the mesh, with a karma/trust layer that fuses social
//! reputation with on-chain compute reputation (a node's uptime, capacity, and price).
//!
//! This crate is the **pure, WASM-clean** core. It has no async runtime, no HTTP client, no libp2p —
//! only `serde`, `sha2`, `hex`, `bincode`, and `ed25519-dalek`. That is deliberate: a phone or a
//! browser tab can compile it to `wasm32` and *contribute* — verify records, fold the read-model,
//! compute trust, and chunk/address media client-side — not just consume an API. The node crate
//! (`trana-node`) layers the mesh transport, persistence, replication, and the live API on top.
//!
//! ## The model in one paragraph
//!
//! Everything written to trana is a [`Record`]: an authored, content-addressed, signature-bearing
//! envelope around a [`Body`] (a profile update, a media descriptor, a post/comment, a vote, a
//! follow, or a live-stream start/segment/end). A record's `id` is the sha256 of its canonical body,
//! so the same content always has the same id (dedup, trustless replication). Folding a stream of
//! records through [`State`] yields the materialized read-model every query answers from. Because
//! the fold is pure and order-tolerant, any node — server, phone, or browser — converges to the same
//! views from the same set of records.
//!
//! ## Trust
//!
//! [`karma`] turns the read-model plus a node's on-chain [`karma::ComputeTrust`] into a single
//! transparent [`karma::TrustScore`]. Trust is the point: the more a profile has proven (good posts,
//! delivered compute, uptime), the more it can be entrusted with critical tasks. The formula is
//! tunable ([`karma::Weights`]) and every component is exposed, never a black box.

pub mod karma;
pub mod model;
pub mod proto;
pub mod record;
pub mod state;

pub use model::{
    Body, Link, Media, MediaKind, MediaRef, Post, Profile, StreamEnd, StreamKind, StreamSegment,
    StreamStart, Vote,
};
pub use record::{canonical_bytes, Record, RecordError};
pub use state::{PostView, ProfileView, SortBy, State, StreamView};

/// Hex sha256 of `bytes` — the content id used everywhere in trana (matches CE's `/blobs` keying).
pub fn cid(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// A 64-hex-char CE NodeId (an ed25519 public key). Authors, voters, owners are all NodeIds.
pub type NodeIdHex = String;
