//! Compute-trust aggregation: turn a user's devices into a [`ComputeTrust`] from CE's on-chain facts.
//!
//! A trana profile's "what can this person's machines do, and have they delivered?" answer is not
//! invented by trana — it is read from CE: `/history/:node_id` (proven, paid work, earnings,
//! no-shows) and `/atlas` (advertised cores/memory and recency). A person owns several NodeIds
//! (devices); this rolls all of them into one aggregate so the profile reflects the whole fleet.

use ce_rs::CeClient;
use std::time::{SystemTime, UNIX_EPOCH};
use trana_core::karma::ComputeTrust;

/// A device is "seen recently" (counts toward uptime) if its last atlas signal is within this many
/// seconds. Matches the atlas broadcast cadence (60s) with headroom for jitter.
const RECENT_SECS: u64 = 180;

/// Reads CE's reputation substrate to build [`ComputeTrust`] aggregates.
#[derive(Clone)]
pub struct ComputeProbe {
    ce: CeClient,
}

impl ComputeProbe {
    pub fn new(ce: CeClient) -> Self {
        ComputeProbe { ce }
    }

    /// Aggregate the on-chain compute reputation of every NodeId in `devices`. Unreachable history or
    /// a missing atlas entry degrades gracefully (that device simply contributes zeros), so the call
    /// never fails the whole profile because one device is offline.
    pub async fn aggregate(&self, devices: &[String]) -> ComputeTrust {
        let mut trust = ComputeTrust::default();
        if devices.is_empty() {
            return trust;
        }
        // One atlas snapshot covers every device.
        let atlas = self.ce.atlas().await.unwrap_or_default();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);

        for dev in devices {
            let hist = self.ce.history(dev).await.ok();
            let entry = atlas.iter().find(|e| &e.node_id == dev);

            let (jobs, hbeats, expiries, earned) = match &hist {
                Some(h) => (
                    h.jobs_hosted,
                    h.heartbeats_hosted,
                    h.expiries,
                    h.earned.base().max(0) as u128,
                ),
                None => (0, 0, 0, 0),
            };
            let (cores, mem, seen) = match entry {
                Some(e) => (
                    e.cpu_cores,
                    e.mem_mb as u64,
                    now.saturating_sub(e.last_seen_secs) <= RECENT_SECS,
                ),
                None => (0, 0, false),
            };
            trust.add_device(jobs, hbeats, expiries, earned, cores, mem, seen);
        }
        trust
    }

    /// Web-of-trust seed candidates drawn from CE's compute reputation: nodes that have actually
    /// **delivered** paid work (jobs + heartbeats) — the hard-to-fake, economically-costly signal —
    /// each weighted by `ln(1 + delivered)` (capped). Bounded to the top `max` atlas advertisers so
    /// at most `max` `/history` lookups happen. Nodes that only *advertise* capacity but have
    /// delivered nothing are NOT seeded, so merely claiming cores buys no trust. With device binding
    /// (P0a) a profile cannot borrow these nodes' reputation either, so this is a sound anchor.
    pub async fn seed_nodes(&self, max: usize) -> Vec<(String, f64)> {
        let mut atlas = self.ce.atlas().await.unwrap_or_default();
        if atlas.is_empty() {
            return Vec::new();
        }
        // Prefer the largest advertisers, then confirm with delivered work.
        atlas.sort_by(|a, b| b.cpu_cores.cmp(&a.cpu_cores));
        atlas.truncate(max);
        let mut seeds = Vec::new();
        for e in atlas {
            if let Ok(h) = self.ce.history(&e.node_id).await {
                let delivered = h.jobs_hosted + h.heartbeats_hosted;
                if delivered > 0 {
                    let w = (delivered as f64).ln_1p().min(3.0);
                    seeds.push((e.node_id, w));
                }
            }
        }
        seeds
    }
}
