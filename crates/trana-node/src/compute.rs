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
}
