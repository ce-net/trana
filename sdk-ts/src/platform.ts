// Per-platform client tuning — the trana analog of viewport/interest scoping.
//
// One mesh transport, three clients (native / desktop browser / mobile browser); identical wire,
// only the budgets differ, so a phone stays cheap and a million clients stays affordable. Read the
// Profile for your platform and size feed pages, media preload, prefetch, concurrency, and reconnect
// backoff accordingly. Discovery + failover are already handled by the client; this bounds per-client
// cost on top.

export type Platform = "native" | "desktop-browser" | "mobile-browser";

export interface Profile {
  feedPageSize: number;
  prefetchPages: number;
  maxMediaAutoloadBytes: number;
  mediaAutoplay: boolean;
  thumbnailsOnly: boolean;
  concurrency: number;
  discoveryTtlMs: number;
  reconnectBaseMs: number;
  reconnectMaxMs: number;
}

const PROFILES: Record<Platform, Profile> = {
  native: {
    feedPageSize: 100,
    prefetchPages: 3,
    maxMediaAutoloadBytes: 64 * 1024 * 1024,
    mediaAutoplay: true,
    thumbnailsOnly: false,
    concurrency: 16,
    discoveryTtlMs: 30000,
    reconnectBaseMs: 250,
    reconnectMaxMs: 10000,
  },
  "desktop-browser": {
    feedPageSize: 50,
    prefetchPages: 2,
    maxMediaAutoloadBytes: 16 * 1024 * 1024,
    mediaAutoplay: true,
    thumbnailsOnly: false,
    concurrency: 8,
    discoveryTtlMs: 15000,
    reconnectBaseMs: 500,
    reconnectMaxMs: 15000,
  },
  "mobile-browser": {
    feedPageSize: 20,
    prefetchPages: 1,
    maxMediaAutoloadBytes: 2 * 1024 * 1024,
    mediaAutoplay: false,
    thumbnailsOnly: true,
    concurrency: 4,
    discoveryTtlMs: 10000,
    reconnectBaseMs: 1000,
    reconnectMaxMs: 30000,
  },
};

/** The tuned budgets for a platform. */
export function profile(platform: Platform): Profile {
  return PROFILES[platform];
}

/** Capped exponential reconnect backoff for a 0-based attempt, in ms. */
export function reconnectBackoffMs(platform: Platform, attempt: number): number {
  const p = PROFILES[platform];
  const shifted = p.reconnectBaseMs * 2 ** Math.min(attempt, 16);
  return Math.min(shifted, p.reconnectMaxMs);
}

/** Detect the current platform in a browser/Node environment (best-effort; override when you know). */
export function detectPlatform(): Platform {
  const nav = (globalThis as { navigator?: { userAgent?: string } }).navigator;
  if (!nav?.userAgent) return "native";
  return /Mobi|Android|iPhone|iPad/i.test(nav.userAgent) ? "mobile-browser" : "desktop-browser";
}
