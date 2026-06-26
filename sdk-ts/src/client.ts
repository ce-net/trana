// Transport: reach the trana mesh service through a local CE node's HTTP API, with discovery and
// failover handled here so callers never think about *which* instance they hit. This is the layer
// that makes "the frontend doesn't deal with load balancers and distribution" true.

import type { Envelope } from "./types.js";

export interface ClientOptions {
  /** CE node HTTP API base URL (default http://127.0.0.1:8844). The node routes to trana over the mesh. */
  node?: string;
  /** Node API bearer token for mutating calls (mesh/request, blob upload). In a browser behind
   *  ce-serve's mesh-bridge this is injected for you; locally, pass the node's api token. */
  token?: string;
  /** Per-request timeout in ms (default 10000). */
  timeoutMs?: number;
  /** Override fetch (tests, custom transports). Defaults to global fetch. */
  fetch?: typeof fetch;
  /** How long a discovered instance list is reused before re-discovery, ms (default 15000). */
  discoveryTtlMs?: number;
  /** Act as another identity via a ce-cap capability: writes are attributed to `author` and carry
   *  `cap` (a `ce grant <this-device> --can trana:act` token signed by `author`). */
  actAs?: { author: string; cap: string };
}

const SERVICE = "trana";
const CHUNK_SIZE = 1024 * 1024; // 1 MiB — matches trana_core::data::DEFAULT_CHUNK_SIZE.
const MANIFEST_KIND = "ce-object-v1";

export class TranaError extends Error {}

function toHex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

function fromHex(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  // A Uint8Array is a valid BufferSource at runtime; the cast sidesteps TS 5.7's generic
  // Uint8Array<ArrayBufferLike> variance against BufferSource.
  const digest = await crypto.subtle.digest("SHA-256", bytes as BufferSource);
  return toHex(new Uint8Array(digest));
}

interface Manifest {
  kind: string;
  chunk_size: number;
  total_size: number;
  chunks: string[];
}

/**
 * Low-level client: discovers live trana instances, sends mesh RPCs with failover, and moves
 * content-addressed bytes (chunked) to/from the CE blob store. The high-level `Trana` facade in
 * index.ts wraps this with typed, ergonomic methods.
 */
export class TranaClient {
  private node: string;
  private token?: string;
  private timeoutMs: number;
  private fetchImpl: typeof fetch;
  private discoveryTtlMs: number;
  private instances: string[] = [];
  private discoveredAt = 0;
  private cursor = 0; // round-robin start, so load spreads across instances.
  private actAs?: { author: string; cap: string };

  constructor(opts: ClientOptions = {}) {
    this.node = (opts.node ?? "http://127.0.0.1:8844").replace(/\/+$/, "");
    this.token = opts.token;
    this.timeoutMs = opts.timeoutMs ?? 10000;
    this.fetchImpl = opts.fetch ?? globalThis.fetch.bind(globalThis);
    this.discoveryTtlMs = opts.discoveryTtlMs ?? 15000;
    this.actAs = opts.actAs;
  }

  private headers(json: boolean): Record<string, string> {
    const h: Record<string, string> = {};
    if (json) h["content-type"] = "application/json";
    if (this.token) h["authorization"] = `Bearer ${this.token}`;
    return h;
  }

  private url(path: string): string {
    return `${this.node}${path}`;
  }

  /** Discover live trana instances (cached for discoveryTtlMs). Falls back to the local node id. */
  async instancesNow(force = false): Promise<string[]> {
    const fresh = Date.now() - this.discoveredAt < this.discoveryTtlMs && this.instances.length > 0;
    if (fresh && !force) return this.instances;
    const found = new Set<string>();
    try {
      const r = await this.fetchImpl(this.url(`/discovery/find/${SERVICE}`), { headers: this.headers(false) });
      if (r.ok) {
        const j = (await r.json()) as { providers?: string[] };
        for (const p of j.providers ?? []) found.add(p);
      }
    } catch {
      /* fall through to local */
    }
    // The local node may serve trana co-located and never appears in its own discovery list.
    try {
      const r = await this.fetchImpl(this.url(`/status`), { headers: this.headers(false) });
      if (r.ok) {
        const j = (await r.json()) as { node_id?: string };
        if (j.node_id) found.add(j.node_id);
      }
    } catch {
      /* ignore */
    }
    this.instances = [...found];
    this.discoveredAt = Date.now();
    return this.instances;
  }

  /** Pin all calls to one trana node id (skip discovery) — for tests or a co-located node. */
  pin(nodeId: string): void {
    this.instances = [nodeId];
    this.discoveredAt = Number.MAX_SAFE_INTEGER;
  }

  /** Send a typed mesh RPC to trana with failover across instances; returns the decoded `T`. */
  async call<T>(topic: string, req: unknown): Promise<T> {
    // When acting as a delegated identity, merge _as/_cap into the (object) request body.
    const body =
      this.actAs && req && typeof req === "object" && !Array.isArray(req)
        ? { ...(req as Record<string, unknown>), _as: this.actAs.author, _cap: this.actAs.cap }
        : req;
    const payloadHex = toHex(new TextEncoder().encode(JSON.stringify(body)));
    const instances = await this.instancesNow();
    if (instances.length === 0) throw new TranaError("no trana instance reachable via the CE node");

    let lastErr: unknown;
    const n = instances.length;
    for (let i = 0; i < n; i++) {
      const to = instances[(this.cursor + i) % n]!;
      try {
        const reply = await this.meshRequest(to, topic, payloadHex);
        this.cursor = (this.cursor + i + 1) % n; // advance round-robin past the instance that worked
        return this.decode<T>(reply);
      } catch (e) {
        lastErr = e;
        // Transport failure → try the next instance. Application errors (decoded) throw immediately.
        if (e instanceof TranaError && (e as TranaError & { app?: boolean }).app) throw e;
      }
    }
    throw new TranaError(`all trana instances failed: ${String(lastErr)}`);
  }

  private async meshRequest(to: string, topic: string, payloadHex: string): Promise<Uint8Array> {
    const body = JSON.stringify({ to, topic, payload_hex: payloadHex, timeout_ms: this.timeoutMs });
    const r = await this.fetchImpl(this.url(`/mesh/request`), {
      method: "POST",
      headers: this.headers(true),
      body,
    });
    if (!r.ok) throw new TranaError(`mesh request HTTP ${r.status}`);
    const j = (await r.json()) as { payload_hex?: string };
    return fromHex(j.payload_hex ?? "");
  }

  private decode<T>(replyBytes: Uint8Array): T {
    const env = JSON.parse(new TextDecoder().decode(replyBytes)) as Envelope;
    if (!env.ok) {
      const err = new TranaError(env.error ?? "trana error") as TranaError & { app?: boolean };
      err.app = true; // an application-level error: do not fail over, surface it.
      throw err;
    }
    return env.data as T;
  }

  // ----- content-addressed blobs -----

  /** Upload one blob; returns its sha256 hex (the CID). */
  async putBlob(bytes: Uint8Array): Promise<string> {
    const r = await this.fetchImpl(this.url(`/blobs`), {
      method: "POST",
      headers: this.headers(false),
      body: bytes as BodyInit,
    });
    if (!r.ok) throw new TranaError(`blob upload HTTP ${r.status}`);
    const j = (await r.json()) as { hash?: string };
    if (!j.hash) throw new TranaError("blob upload returned no hash");
    return j.hash;
  }

  /** Fetch one blob by CID. */
  async getBlob(cid: string): Promise<Uint8Array> {
    const r = await this.fetchImpl(this.url(`/blobs/${cid}`), { headers: this.headers(false) });
    if (!r.ok) throw new TranaError(`blob ${cid} HTTP ${r.status}`);
    return new Uint8Array(await r.arrayBuffer());
  }

  /**
   * Upload an object of any size: split into content-addressed chunks, store each, then the
   * manifest. Returns the object CID. Works in the browser and Node, so a phone genuinely
   * contributes the bytes.
   */
  async putObject(bytes: Uint8Array): Promise<string> {
    const chunks: string[] = [];
    for (let off = 0; off < bytes.length || (off === 0 && bytes.length === 0); off += CHUNK_SIZE) {
      if (bytes.length === 0) break;
      const slice = bytes.subarray(off, Math.min(off + CHUNK_SIZE, bytes.length));
      const cid = await sha256Hex(slice);
      const stored = await this.putBlob(slice);
      if (stored !== cid) throw new TranaError(`blob store hash mismatch (${stored} != ${cid})`);
      chunks.push(cid);
    }
    const manifest: Manifest = {
      kind: MANIFEST_KIND,
      chunk_size: CHUNK_SIZE,
      total_size: bytes.length,
      chunks,
    };
    return this.putBlob(new TextEncoder().encode(JSON.stringify(manifest)));
  }

  /** Fetch an object by CID: resolve the manifest, pull + verify every chunk, reassemble. */
  async getObject(objectCid: string): Promise<Uint8Array> {
    const manifest = JSON.parse(new TextDecoder().decode(await this.getBlob(objectCid))) as Manifest;
    if (manifest.kind !== MANIFEST_KIND) throw new TranaError(`unsupported manifest kind ${manifest.kind}`);
    const out = new Uint8Array(manifest.total_size);
    let off = 0;
    for (const cid of manifest.chunks) {
      const chunk = await this.getBlob(cid);
      if ((await sha256Hex(chunk)) !== cid) throw new TranaError(`chunk ${cid} failed verification`);
      out.set(chunk, off);
      off += chunk.length;
    }
    if (off !== manifest.total_size) throw new TranaError("reassembled size mismatch");
    return out;
  }
}
