//! Per-platform client tuning — the trana analog of viewport/interest scoping.
//!
//! One mesh transport, three clients: a native app (full in-process node), a desktop browser (WASM
//! node), and a mobile browser (WASM-only over wss). The wire is identical; only the **budgets**
//! differ, so a phone stays cheap and a million clients stays affordable. The frontend reads a
//! [`Profile`] for its platform and sizes its requests accordingly — page sizes, media preload,
//! prefetch depth, fetch concurrency, and reconnect backoff. Discovery + failover are already handled
//! by [`crate::TranaClient`]; this is the cost-bounding layer on top.

/// Where the client runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// A native app with a full in-process CE node.
    Native,
    /// A desktop browser running a WASM node (or proxying a local node).
    DesktopBrowser,
    /// A mobile browser, WASM-only over a relay wss — the tightest budget.
    MobileBrowser,
}

/// Cost budgets for one platform. Every field bounds per-client work so the network scales.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Profile {
    /// Threads/comments to pull per feed page.
    pub feed_page_size: usize,
    /// How many pages ahead to prefetch.
    pub prefetch_pages: usize,
    /// Max bytes of media to auto-load (preview/autoplay) before requiring an explicit tap.
    pub max_media_autoload_bytes: u64,
    /// Whether video/audio may auto-play (vs. tap-to-play).
    pub media_autoplay: bool,
    /// Load only thumbnails by default, fetching full media on demand (saves a phone's data/battery).
    pub thumbnails_only: bool,
    /// Max concurrent content/object fetches.
    pub concurrency: usize,
    /// How long to reuse a discovered instance list before re-discovering (ms).
    pub discovery_ttl_ms: u64,
    /// Reconnect backoff floor and ceiling (ms).
    pub reconnect_base_ms: u64,
    pub reconnect_max_ms: u64,
}

impl Platform {
    /// The tuned budgets for this platform.
    pub fn profile(self) -> Profile {
        match self {
            Platform::Native => Profile {
                feed_page_size: 100,
                prefetch_pages: 3,
                max_media_autoload_bytes: 64 * 1024 * 1024,
                media_autoplay: true,
                thumbnails_only: false,
                concurrency: 16,
                discovery_ttl_ms: 30_000,
                reconnect_base_ms: 250,
                reconnect_max_ms: 10_000,
            },
            Platform::DesktopBrowser => Profile {
                feed_page_size: 50,
                prefetch_pages: 2,
                max_media_autoload_bytes: 16 * 1024 * 1024,
                media_autoplay: true,
                thumbnails_only: false,
                concurrency: 8,
                discovery_ttl_ms: 15_000,
                reconnect_base_ms: 500,
                reconnect_max_ms: 15_000,
            },
            Platform::MobileBrowser => Profile {
                feed_page_size: 20,
                prefetch_pages: 1,
                max_media_autoload_bytes: 2 * 1024 * 1024,
                media_autoplay: false,
                thumbnails_only: true,
                concurrency: 4,
                discovery_ttl_ms: 10_000,
                reconnect_base_ms: 1_000,
                reconnect_max_ms: 30_000,
            },
        }
    }

    /// Reconnect backoff for `attempt` (0-based): capped exponential, deterministic.
    pub fn reconnect_backoff_ms(self, attempt: u32) -> u64 {
        let p = self.profile();
        let shifted = p.reconnect_base_ms.saturating_mul(1u64 << attempt.min(16));
        shifted.min(p.reconnect_max_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_tighten_toward_mobile() {
        let n = Platform::Native.profile();
        let d = Platform::DesktopBrowser.profile();
        let m = Platform::MobileBrowser.profile();
        assert!(n.feed_page_size > d.feed_page_size && d.feed_page_size > m.feed_page_size);
        assert!(n.max_media_autoload_bytes > m.max_media_autoload_bytes);
        assert!(n.concurrency >= d.concurrency && d.concurrency >= m.concurrency);
        assert!(m.thumbnails_only && !n.thumbnails_only);
        assert!(!m.media_autoplay && n.media_autoplay);
    }

    #[test]
    fn backoff_is_capped_exponential() {
        let p = Platform::MobileBrowser;
        assert_eq!(p.reconnect_backoff_ms(0), 1_000);
        assert_eq!(p.reconnect_backoff_ms(1), 2_000);
        assert_eq!(p.reconnect_backoff_ms(100), 30_000); // capped
    }
}
