// trana TypeScript SDK — one easy, type-safe handle over the whole distributed backend.
//
// The frontend calls `trana.posts.create(...)`, `trana.feed.hot(board)`, `trana.media.upload(bytes)`
// and never thinks about which node serves it, how many replicas there are, or how content is
// chunked and addressed — discovery, failover, and content addressing are handled underneath.

import { TranaClient, type ClientOptions, TranaError } from "./client.js";
import type {
  BanStandingResp,
  BoardPolicy,
  BoardView,
  KarmaResp,
  Media,
  MediaKind,
  MediaRef,
  NodeId,
  PostView,
  ProfileResp,
  ProposalView,
  Sort,
  StreamKind,
  StreamView,
} from "./types.js";

export * from "./types.js";
export { TranaError, type ClientOptions } from "./client.js";

const T = {
  profilePut: "trana/profile/put/v1",
  profileGet: "trana/profile/get/v1",
  mediaPut: "trana/media/put/v1",
  mediaGet: "trana/media/get/v1",
  postCreate: "trana/post/create/v1",
  postGet: "trana/post/get/v1",
  threads: "trana/threads/v1",
  comments: "trana/comments/v1",
  vote: "trana/vote/v1",
  follow: "trana/follow/v1",
  karma: "trana/karma/v1",
  streamStart: "trana/stream/start/v1",
  streamAppend: "trana/stream/append/v1",
  streamEnd: "trana/stream/end/v1",
  streamGet: "trana/stream/get/v1",
  streamsLive: "trana/streams/live/v1",
  boardPut: "trana/board/put/v1",
  boardGet: "trana/board/get/v1",
  boards: "trana/boards/v1",
  feed: "trana/feed/v1",
  banVote: "trana/banvote/v1",
  banStanding: "trana/banstanding/v1",
  policyPropose: "trana/policy/propose/v1",
  policyVote: "trana/policy/vote/v1",
  proposals: "trana/policy/list/v1",
  proposalGet: "trana/policy/get/v1",
} as const;

export interface ProfileInput {
  handle?: string;
  displayName?: string;
  bio?: string;
  avatar?: string; // media id
  links?: { label: string; url: string }[];
  devices?: NodeId[];
}

export interface MediaInput {
  kind: MediaKind;
  objectCid: string;
  mime: string;
  size: number;
  title?: string;
  durationMs?: number;
  width?: number;
  height?: number;
  thumbnail?: string;
  replicas?: number;
}

export interface PostInput {
  board: string;
  parent?: string;
  title?: string;
  body?: string;
  media?: string[]; // media ids
}

export interface FeedOptions {
  scope?: "board" | "all" | "home";
  board?: string;
  viewer?: NodeId;
  sort?: Sort;
  limit?: number;
}

/** The main SDK handle. */
export class Trana {
  readonly client: TranaClient;

  constructor(opts: ClientOptions = {}) {
    this.client = new TranaClient(opts);
  }

  /** Warm up discovery (optional; the first call discovers lazily anyway). */
  async ready(): Promise<NodeId[]> {
    return this.client.instancesNow(true);
  }

  /** Pin every call to a specific trana node id (tests / co-located node). */
  pin(nodeId: NodeId): this {
    this.client.pin(nodeId);
    return this;
  }

  // ----- profiles + trust -----
  readonly profile = {
    set: (p: ProfileInput): Promise<{ id: string }> =>
      this.client.call(T.profilePut, {
        handle: p.handle ?? null,
        display_name: p.displayName ?? "",
        bio: p.bio ?? "",
        avatar: p.avatar ? ({ media_id: p.avatar } as MediaRef) : null,
        links: p.links ?? [],
        devices: p.devices ?? [],
      }),
    get: (nodeId: NodeId): Promise<ProfileResp> => this.client.call(T.profileGet, { node_id: nodeId }),
  };

  /** A node's karma + on-chain compute trust + the fused score. */
  karma(nodeId: NodeId): Promise<KarmaResp> {
    return this.client.call(T.karma, { node_id: nodeId });
  }

  // ----- media (images / video / audio / podcasts / documents) -----
  readonly media = {
    /** Upload raw bytes to the content-addressed store; returns the object CID. */
    upload: (bytes: Uint8Array): Promise<string> => this.client.putObject(bytes),
    /** Register a media descriptor over already-uploaded bytes; returns the media id. */
    put: (m: MediaInput): Promise<{ id: string }> =>
      this.client.call(T.mediaPut, {
        kind: m.kind,
        object_cid: m.objectCid,
        mime: m.mime,
        size: m.size,
        title: m.title ?? "",
        duration_ms: m.durationMs ?? null,
        width: m.width ?? null,
        height: m.height ?? null,
        thumbnail: m.thumbnail ? ({ media_id: m.thumbnail } as MediaRef) : null,
        extra: {},
        replicas: m.replicas ?? 0,
      }),
    /** Upload bytes AND register the descriptor in one call; returns the media id. */
    add: async (kind: MediaKind, mime: string, bytes: Uint8Array, title = ""): Promise<string> => {
      const objectCid = await this.client.putObject(bytes);
      const r = await this.media.put({ kind, objectCid, mime, size: bytes.length, title });
      return r.id;
    },
    get: (mediaId: string): Promise<{ media: Media | null }> =>
      this.client.call(T.mediaGet, { media_id: mediaId }),
    /** Fetch the actual bytes for a media id. */
    download: async (mediaId: string): Promise<Uint8Array> => {
      const { media } = await this.media.get(mediaId);
      if (!media) throw new TranaError(`no such media: ${mediaId}`);
      return this.client.getObject(media.object_cid);
    },
  };

  // ----- threads / posts / comments -----
  readonly posts = {
    create: (p: PostInput): Promise<{ id: string }> =>
      this.client.call(T.postCreate, {
        board: p.board,
        parent: p.parent ?? null,
        title: p.title ?? null,
        body: p.body ?? "",
        media: (p.media ?? []).map((id) => ({ media_id: id }) as MediaRef),
      }),
    get: (id: string): Promise<{ post: PostView | null }> => this.client.call(T.postGet, { id }),
    /** Reply to a post/comment. */
    reply: (board: string, parent: string, body: string): Promise<{ id: string }> =>
      this.posts.create({ board, parent, body }),
  };

  /** Thread roots in a board, ranked by `sort`. */
  threads(board: string, opts: { sort?: Sort; limit?: number } = {}): Promise<{ threads: PostView[] }> {
    return this.client.call(T.threads, { board, sort: opts.sort ?? "hot", limit: opts.limit ?? 50 });
  }

  /** The full comment tree under a thread root. */
  comments(root: string, opts: { sort?: Sort } = {}): Promise<{ comments: PostView[] }> {
    return this.client.call(T.comments, { root, sort: opts.sort ?? "hot" });
  }

  /** Up/down/clear a vote: +1, -1, or 0. */
  vote(target: string, value: 1 | -1 | 0): Promise<{ ok: boolean }> {
    return this.client.call(T.vote, { target, value });
  }

  follow(followee: NodeId, active = true): Promise<{ ok: boolean }> {
    return this.client.call(T.follow, { followee, active });
  }

  // ----- feeds (the feed algorithm) -----
  /** A feed by scope + ranking. Convenience helpers below cover the common cases. */
  feed(opts: FeedOptions = {}): Promise<{ threads: PostView[] }> {
    return this.client.call(T.feed, {
      scope: opts.scope ?? "all",
      board: opts.board ?? null,
      viewer: opts.viewer ?? null,
      sort: opts.sort ?? "hot",
      limit: opts.limit ?? 50,
    });
  }
  hot = (board?: string, limit = 50) => this.feed({ scope: board ? "board" : "all", board, sort: "hot", limit });
  trending = (board?: string, limit = 50) =>
    this.feed({ scope: board ? "board" : "all", board, sort: "trending", limit });
  /** Personalized feed of thread roots from accounts `viewer` follows. */
  home = (viewer: NodeId, sort: Sort = "hot", limit = 50) => this.feed({ scope: "home", viewer, sort, limit });

  // ----- live streaming -----
  readonly streams = {
    start: (s: { title: string; kind: StreamKind; board?: string }): Promise<{ id: string }> =>
      this.client.call(T.streamStart, {
        title: s.title,
        kind: s.kind,
        board: s.board ?? null,
        thumbnail: null,
        extra: {},
      }),
    /** Append a content-addressed segment (upload bytes first with `media.upload`). */
    append: (s: { stream: string; seq: number; objectCid: string; durationMs: number }): Promise<{ id: string }> =>
      this.client.call(T.streamAppend, {
        stream: s.stream,
        seq: s.seq,
        object_cid: s.objectCid,
        duration_ms: s.durationMs,
        replicas: 0,
      }),
    /** Upload a segment's bytes and append it in one call. */
    pushSegment: async (stream: string, seq: number, bytes: Uint8Array, durationMs: number): Promise<string> => {
      const objectCid = await this.client.putObject(bytes);
      const r = await this.streams.append({ stream, seq, objectCid, durationMs });
      return r.id;
    },
    end: (stream: string, recordingCid?: string): Promise<{ ok: boolean }> =>
      this.client.call(T.streamEnd, { stream, recording_cid: recordingCid ?? null }),
    get: (id: string): Promise<{ stream: StreamView | null }> => this.client.call(T.streamGet, { id }),
    live: (): Promise<{ streams: StreamView[] }> => this.client.call(T.streamsLive, {}),
  };

  // ----- community governance -----
  readonly boards = {
    create: (b: {
      board: string;
      title?: string;
      description?: string;
      policy?: Partial<BoardPolicy>;
    }): Promise<{ id: string }> =>
      this.client.call(T.boardPut, {
        board: b.board,
        title: b.title ?? "",
        description: b.description ?? "",
        policy: {
          grace_secs: b.policy?.grace_secs ?? 21600,
          ban_support: b.policy?.ban_support ?? 0.66,
          ban_quorum: b.policy?.ban_quorum ?? 10,
          min_trust_to_post: b.policy?.min_trust_to_post ?? 0,
          min_trust_to_vote: b.policy?.min_trust_to_vote ?? 0,
        },
      }),
    get: (board: string): Promise<{ board: BoardView }> => this.client.call(T.boardGet, { board }),
    list: (): Promise<{ boards: BoardView[] }> => this.client.call(T.boards, {}),
  };

  /** Cast a community ban vote (`support=true` to ban, `false` to keep). */
  banVote(board: string, target: NodeId, support: boolean, reason = ""): Promise<{ ok: boolean }> {
    return this.client.call(T.banVote, { board, target, support, reason });
  }

  /** A user's ban standing in a board (raw tally + the node's trust-weighted verdict). */
  banStanding(board: string, target: NodeId): Promise<BanStandingResp> {
    return this.client.call(T.banStanding, { board, target });
  }

  readonly policy = {
    propose: (p: { title: string; body: string; board?: string }): Promise<{ id: string }> =>
      this.client.call(T.policyPropose, { title: p.title, body: p.body, board: p.board ?? null }),
    vote: (proposal: string, support: boolean): Promise<{ ok: boolean }> =>
      this.client.call(T.policyVote, { proposal, support }),
    list: (board?: string): Promise<{ proposals: ProposalView[] }> =>
      this.client.call(T.proposals, { board: board ?? null }),
    get: (id: string): Promise<{ proposal: ProposalView | null }> => this.client.call(T.proposalGet, { id }),
  };
}

export default Trana;
