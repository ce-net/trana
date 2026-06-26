// trana wire types — the TypeScript mirror of `trana_core::proto` / `model`.
//
// Hand-kept in lockstep with the Rust definitions so the SDK is type-safe end to end. Money is a
// decimal string (base units, 10^18 = 1 credit) because the values exceed JS's safe integer range.

/** A CE NodeId — an ed25519 public key as 64 lowercase hex chars. Authors, voters, owners. */
export type NodeId = string;

/** Base-unit credit amount as a decimal string. */
export type Amount = string;

export type MediaKind = "image" | "video" | "audio" | "podcast" | "document";
export type StreamKind = "video" | "audio";

/** Feed ranking algorithms. */
export type Sort = "hot" | "new" | "top" | "best" | "trending" | "rising" | "controversial";

export interface MediaRef {
  media_id: string;
}

export interface Link {
  label: string;
  url: string;
}

export interface Profile {
  handle?: string | null;
  display_name: string;
  bio: string;
  avatar?: MediaRef | null;
  links: Link[];
  /** Other NodeIds (devices) this user owns; their compute capacity rolls into the profile. */
  devices: NodeId[];
}

export interface ProfileView {
  node_id: NodeId;
  profile: Profile;
  updated_ms: number;
}

export interface Media {
  kind: MediaKind;
  object_cid: string;
  mime: string;
  size: number;
  title: string;
  duration_ms?: number | null;
  width?: number | null;
  height?: number | null;
  thumbnail?: MediaRef | null;
  extra: Record<string, string>;
}

export interface PostView {
  id: string;
  author: NodeId;
  created_ms: number;
  board: string;
  parent: string | null;
  title: string | null;
  body: string;
  media: string[];
  ups: number;
  downs: number;
  score: number;
  reply_count: number;
}

export interface BoardPolicy {
  grace_secs: number;
  ban_support: number;
  ban_quorum: number;
  min_trust_to_post: number;
  min_trust_to_vote: number;
}

export interface BoardView {
  board: string;
  title: string;
  description: string;
  policy: BoardPolicy;
  created_ms: number;
}

export interface SocialKarma {
  posts: number;
  comments: number;
  post_score: number;
  comment_score: number;
  upvotes: number;
  downvotes: number;
  followers: number;
}

export interface ComputeTrust {
  devices: number;
  jobs_hosted: number;
  heartbeats_hosted: number;
  expiries: number;
  earned_base: string;
  cpu_cores: number;
  mem_mb: number;
  uptime: number;
  avg_price_base: string;
}

export interface TrustScore {
  karma: number;
  social_term: number;
  delivered_work: number;
  reliability: number;
  compute_term: number;
  combined: number;
}

export interface ProfileResp {
  profile: ProfileView | null;
  social: SocialKarma;
  compute: ComputeTrust;
  trust: TrustScore;
}

export interface KarmaResp {
  social: SocialKarma;
  compute: ComputeTrust;
  trust: TrustScore;
}

export interface StreamSegment {
  stream: string;
  seq: number;
  object_cid: string;
  duration_ms: number;
}

export interface StreamStart {
  title: string;
  kind: StreamKind;
  board?: string | null;
  thumbnail?: MediaRef | null;
  extra: Record<string, string>;
}

export interface StreamView {
  id: string;
  author: NodeId;
  created_ms: number;
  start: StreamStart;
  live: boolean;
  recording_cid: string | null;
  segments: StreamSegment[];
  total_duration_ms: number;
}

export interface BanStanding {
  board: string;
  target: NodeId;
  support: number;
  oppose: number;
  banned_raw: boolean;
}

export interface BanStandingResp {
  standing: BanStanding;
  weighted_support: number;
  banned: boolean;
}

export interface ProposalView {
  id: string;
  author: NodeId;
  created_ms: number;
  board: string | null;
  title: string;
  body: string;
  favor: number;
  against: number;
}

// ----- content addressing + documents + versioning -----

export type RefKind = "post" | "document" | "media" | "stream" | "profile" | "board" | "blob";

export interface Ref {
  kind: RefKind;
  id: string;
}

export interface FileRef {
  object_cid: string;
  mime: string;
  size: number;
  name: string;
}

export interface DocumentView {
  id: string;
  author: NodeId;
  created_ms: number;
  title: string;
  body: string;
  file: FileRef | null;
  board: string | null;
  /** Forward references this document makes, as `trana://...` URIs. */
  refs: string[];
  /** Backlinks: `trana://...` URIs that reference this document. */
  referenced_by: string[];
  series: string;
  prev: string | null;
  version: number;
  versions: number;
  is_latest: boolean;
  ups: number;
  downs: number;
  score: number;
}

export interface DiffLine {
  /** `" "` context, `"-"` removed, `"+"` added. */
  op: string;
  text: string;
}

/** The reply envelope on every mesh RPC. */
export interface Envelope {
  ok: boolean;
  error?: string | null;
  data: unknown;
}
