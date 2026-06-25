//! Optional local HTTP/REST facade over the engine (feature `gateway`, off by default).
//!
//! The canonical trana API is mesh-native (request/reply by NodeId). This gateway is a convenience
//! for plain-HTTP frontends and quick `curl` exploration on the operator's own machine: it drives the
//! exact same [`Engine`], with every write attributed to the **local node's** identity. It is not a
//! public ingress and binds wherever the operator points it; put it behind `ce-serve`/`ce-expose` for
//! anything outward-facing.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use trana_core::proto::*;

use crate::engine::Engine;

#[derive(Clone)]
struct Ctx {
    engine: Arc<Engine>,
    self_id: String,
}

/// Serve the REST facade on `0.0.0.0:port` until the process exits.
pub async fn serve(engine: Arc<Engine>, self_id: String, port: u16) -> anyhow::Result<()> {
    let ctx = Ctx { engine, self_id };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/profile/:node_id", get(profile_get))
        .route("/profile", post(profile_put))
        .route("/media", post(media_put))
        .route("/media/:id", get(media_get))
        .route("/post", post(post_create))
        .route("/post/:id", get(post_get))
        .route("/threads/:board", get(threads))
        .route("/comments/:root", get(comments))
        .route("/vote", post(vote))
        .route("/follow", post(follow))
        .route("/karma/:node_id", get(karma))
        .route("/stream/start", post(stream_start))
        .route("/stream/append", post(stream_append))
        .route("/stream/end", post(stream_end))
        .route("/stream/:id", get(stream_get))
        .route("/streams/live", get(streams_live))
        .with_state(ctx);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "trana gateway listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Map an `anyhow::Result<T>` into a JSON response (200 with the value, or 500 with `{error}`).
fn out<T: serde::Serialize>(r: anyhow::Result<T>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() })))
                .into_response()
        }
    }
}

#[derive(serde::Deserialize)]
struct SortQuery {
    sort: Option<String>,
    limit: Option<usize>,
}

async fn profile_get(State(c): State<Ctx>, Path(node_id): Path<String>) -> Response {
    out(c.engine.profile_get(&node_id).await)
}

async fn profile_put(State(c): State<Ctx>, Json(req): Json<ProfilePutReq>) -> Response {
    out(c.engine.profile_put(&c.self_id, req).await)
}

async fn media_put(State(c): State<Ctx>, Json(req): Json<MediaPutReq>) -> Response {
    out(c.engine.media_put(&c.self_id, req).await)
}

async fn media_get(State(c): State<Ctx>, Path(id): Path<String>) -> Response {
    Json(c.engine.media_get(&id)).into_response()
}

async fn post_create(State(c): State<Ctx>, Json(req): Json<PostCreateReq>) -> Response {
    out(c.engine.post_create(&c.self_id, req).await)
}

async fn post_get(State(c): State<Ctx>, Path(id): Path<String>) -> Response {
    Json(c.engine.post_get(&id)).into_response()
}

async fn threads(State(c): State<Ctx>, Path(board): Path<String>, Query(q): Query<SortQuery>) -> Response {
    let req = ThreadsReq {
        board,
        sort: q.sort.unwrap_or_else(|| "hot".into()),
        limit: q.limit.unwrap_or(50),
    };
    Json(c.engine.threads(req)).into_response()
}

async fn comments(State(c): State<Ctx>, Path(root): Path<String>, Query(q): Query<SortQuery>) -> Response {
    let req = CommentsReq { root, sort: q.sort.unwrap_or_else(|| "hot".into()) };
    Json(c.engine.comments(req)).into_response()
}

async fn vote(State(c): State<Ctx>, Json(req): Json<VoteReq>) -> Response {
    out(c.engine.vote(&c.self_id, req).await)
}

async fn follow(State(c): State<Ctx>, Json(req): Json<FollowReq>) -> Response {
    out(c.engine.follow(&c.self_id, req).await)
}

async fn karma(State(c): State<Ctx>, Path(node_id): Path<String>) -> Response {
    out(c.engine.karma(&node_id).await)
}

async fn stream_start(State(c): State<Ctx>, Json(req): Json<StreamStartReq>) -> Response {
    out(c.engine.stream_start(&c.self_id, req).await)
}

async fn stream_append(State(c): State<Ctx>, Json(req): Json<StreamAppendReq>) -> Response {
    out(c.engine.stream_append(&c.self_id, req).await)
}

async fn stream_end(State(c): State<Ctx>, Json(req): Json<StreamEndReq>) -> Response {
    out(c.engine.stream_end(&c.self_id, req).await)
}

async fn stream_get(State(c): State<Ctx>, Path(id): Path<String>) -> Response {
    Json(c.engine.stream_get(&id)).into_response()
}

async fn streams_live(State(c): State<Ctx>) -> Response {
    Json(c.engine.streams_live()).into_response()
}
