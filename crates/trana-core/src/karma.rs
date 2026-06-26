//! Trust math: fuse **social karma** with **on-chain compute reputation** into one transparent
//! [`TrustScore`].
//!
//! The whole point of trana is trust: a profile that has proven itself — good posts, delivered
//! compute, real uptime — should be the one you hand a critical task to. This module is the pure
//! formula. Every input and every component of the output is exposed; nothing is hidden, and the
//! blend is tunable via [`Weights`]. The node ([`crate::state::State::social`] for the social half,
//! `/history` + `/atlas` for the compute half) supplies the inputs.

use serde::{Deserialize, Serialize};

/// The social-reputation aggregate for a user, summed across all their posts and comments.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SocialKarma {
    /// Number of thread roots authored.
    pub posts: u64,
    /// Number of comments authored.
    pub comments: u64,
    /// Net score across the user's thread roots (ups - downs), raw one-vote-one-count.
    pub post_score: i64,
    /// Net score across the user's comments, raw.
    pub comment_score: i64,
    /// Total upvotes received (posts + comments).
    pub upvotes: u64,
    /// Total downvotes received.
    pub downvotes: u64,
    /// Follower count.
    pub followers: u64,
    /// The **effective** net karma that actually drives trust: every received vote weighted by the
    /// voter's own trust (so a sybil's upvote barely counts) and decayed by age. Equal to the raw
    /// [`Self::karma`] when computed with a unit weight and no decay (the degraded, weightless path).
    /// This is the number that makes karma hard to farm: you cannot build it without approval from
    /// already-trusted members.
    #[serde(default)]
    pub effective_score: f64,
}

impl SocialKarma {
    /// Reddit-style "karma": the combined raw net score of posts and comments (for display).
    pub fn karma(&self) -> i64 {
        self.post_score + self.comment_score
    }
}

/// Exponential time-decay multiplier in `(0, 1]`: a signal `age_secs` old is worth
/// `2^(-age/half_life)` of its fresh value. `half_life_secs == 0` disables decay (always `1.0`);
/// a very large half-life (e.g. `u64::MAX`) likewise yields ~`1.0`, which is how the raw, undecayed
/// path is expressed.
pub fn decay(age_secs: u64, half_life_secs: u64) -> f64 {
    if half_life_secs == 0 {
        return 1.0;
    }
    2f64.powf(-(age_secs as f64) / half_life_secs as f64)
}

/// The on-chain compute-reputation aggregate for a user — rolled up across all the NodeIds (devices)
/// they own. Sourced from CE's `/history/:node_id` (proven, paid work) and `/atlas` (advertised
/// capacity). This is what lets a profile answer "what can this person's machines actually do, and
/// have they delivered before?" via the trana API.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ComputeTrust {
    /// Distinct NodeIds (devices) summed into this aggregate.
    pub devices: u32,
    /// Jobs delivered as host (settled) across all devices.
    pub jobs_hosted: u64,
    /// Long-running heartbeats delivered as host.
    pub heartbeats_hosted: u64,
    /// Bids the user's hosts left to expire (no-shows) — a reliability ding.
    pub expiries: u64,
    /// Total credits earned hosting (post-burn), in base units, summed across devices.
    pub earned_base: u128,
    /// Total advertised CPU cores across devices.
    pub cpu_cores: u32,
    /// Total advertised memory (MiB) across devices.
    pub mem_mb: u64,
    /// Fraction of devices seen recently in the atlas (0.0–1.0) — an uptime proxy.
    pub uptime: f64,
    /// Inferred average price per delivered unit of work, base units (earned / delivered). 0 if no
    /// delivery yet. CE has no advertised price field, so this is derived, not quoted.
    pub avg_price_base: u128,
}

impl ComputeTrust {
    /// Total proven, paid work delivered (jobs + heartbeats) — the headline reliability signal.
    pub fn delivered_work(&self) -> u64 {
        self.jobs_hosted + self.heartbeats_hosted
    }

    /// Reliability in 0.0–1.0: delivered work vs. delivered + no-shows. 1.0 when there are no
    /// expiries (including a brand-new host with no history — innocent until proven flaky).
    pub fn reliability(&self) -> f64 {
        let delivered = self.delivered_work();
        let total = delivered + self.expiries;
        if total == 0 {
            return 1.0;
        }
        delivered as f64 / total as f64
    }

    /// Fold one device's `/history` + `/atlas` facts into the aggregate. `seen_recently` feeds the
    /// uptime proxy. Call once per device the user owns.
    #[allow(clippy::too_many_arguments)]
    pub fn add_device(
        &mut self,
        jobs_hosted: u64,
        heartbeats_hosted: u64,
        expiries: u64,
        earned_base: u128,
        cpu_cores: u32,
        mem_mb: u64,
        seen_recently: bool,
    ) {
        self.devices += 1;
        self.jobs_hosted += jobs_hosted;
        self.heartbeats_hosted += heartbeats_hosted;
        self.expiries += expiries;
        self.earned_base = self.earned_base.saturating_add(earned_base);
        self.cpu_cores += cpu_cores;
        self.mem_mb += mem_mb;
        // uptime is recomputed as a running fraction of devices seen recently.
        let seen_count = (self.uptime * (self.devices - 1) as f64).round() as u128
            + if seen_recently { 1 } else { 0 };
        self.uptime = seen_count as f64 / self.devices as f64;
        // Recompute inferred average price.
        let delivered = self.delivered_work() as u128;
        self.avg_price_base = if delivered == 0 { 0 } else { self.earned_base / delivered };
    }
}

/// Tunable blend weights. Trust is contextual — a board moderator and a compute scheduler weigh the
/// two halves differently — so the formula is never hard-coded.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Weights {
    /// Weight on the social half (trust-weighted, decayed karma).
    pub social: f64,
    /// Weight on the compute half.
    pub compute: f64,
    /// Weight on the web-of-trust half (the node's rank in the follow graph). Only blended in when a
    /// graph rank is supplied; dropped otherwise.
    pub graph: f64,
    /// Logistic scale for the effective social karma. Smaller than the raw-karma era because the
    /// effective score is a sum of voter *weights* (each ≤ 1), not a raw vote count.
    pub social_scale: f64,
    /// Logistic scale for delivered compute work.
    pub compute_scale: f64,
    /// Half-life (seconds) for time-decay of social votes. ~90 days by default.
    pub half_life_secs: u64,
}

impl Default for Weights {
    fn default() -> Self {
        // Three hard-to-fake signals. Compute (economically costly) is the heaviest anchor; the
        // web-of-trust graph gates cold-start (you need inbound trust from established members);
        // social karma is itself trust-weighted so it cannot be farmed by a sybil ring.
        Weights {
            social: 0.3,
            compute: 0.4,
            graph: 0.3,
            social_scale: 12.0,
            compute_scale: 25.0,
            half_life_secs: 90 * 24 * 3600,
        }
    }
}

/// The fused trust verdict: a single `combined` score in 0.0–1.0 plus every component that produced
/// it, so a UI can show *why* a profile is (un)trusted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrustScore {
    /// Raw social karma (net post + comment score) — for display.
    pub karma: i64,
    /// The effective (trust-weighted, decayed) social score that fed the social term.
    pub social_score: f64,
    /// Social term in 0.0–1.0 after the logistic squash.
    pub social_term: f64,
    /// Delivered compute work (jobs + heartbeats).
    pub delivered_work: u64,
    /// Reliability in 0.0–1.0.
    pub reliability: f64,
    /// Compute term in 0.0–1.0 (work, gated by reliability).
    pub compute_term: f64,
    /// Web-of-trust rank in 0.0–1.0 (0 when no graph was computed).
    pub graph_rank: f64,
    /// Graph term in 0.0–1.0 (currently the rank itself).
    pub graph_term: f64,
    /// Final fused score in 0.0–1.0.
    pub combined: f64,
}

/// Squash any real number into (0,1) with a logistic curve of the given scale.
fn logistic(x: f64, scale: f64) -> f64 {
    1.0 / (1.0 + (-x / scale).exp())
}

/// Fuse trust-weighted social karma, compute reputation and web-of-trust rank into a [`TrustScore`].
///
/// `graph_rank` is the node's rank in the follow graph (see [`crate::state::State::trust_graph`]),
/// `Some(0.0..=1.0)` when a graph was computed or `None` to drop the graph half from the blend
/// entirely (degraded clients that never ran the propagation). The social term reads
/// `social.effective_score`, so an account whose upvotes came only from low-trust peers gets a low
/// social term no matter how many raw upvotes it amassed — that is what makes karma reliable.
pub fn trust_score(
    social: &SocialKarma,
    compute: &ComputeTrust,
    graph_rank: Option<f64>,
    w: &Weights,
) -> TrustScore {
    let social_term = logistic(social.effective_score, w.social_scale);

    let reliability = compute.reliability();
    let compute_term = logistic(compute.delivered_work() as f64, w.compute_scale) * reliability;

    let (graph_term, graph_w) = match graph_rank {
        Some(g) => (g.clamp(0.0, 1.0), w.graph),
        None => (0.0, 0.0),
    };

    let denom = (w.social + w.compute + graph_w).max(f64::EPSILON);
    let combined =
        (w.social * social_term + w.compute * compute_term + graph_w * graph_term) / denom;

    TrustScore {
        karma: social.karma(),
        social_score: social.effective_score,
        social_term,
        delivered_work: compute.delivered_work(),
        reliability,
        compute_term,
        graph_rank: graph_term,
        graph_term,
        combined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reliability_handles_newcomer_and_flaky() {
        let mut fresh = ComputeTrust::default();
        assert_eq!(fresh.reliability(), 1.0, "no history => benefit of the doubt");

        fresh.jobs_hosted = 9;
        fresh.expiries = 1;
        assert!((fresh.reliability() - 0.9).abs() < 1e-9);
    }

    #[test]
    fn add_device_rolls_up() {
        let mut c = ComputeTrust::default();
        c.add_device(10, 0, 0, 1_000, 8, 16_000, true);
        c.add_device(0, 0, 0, 0, 4, 8_000, false);
        assert_eq!(c.devices, 2);
        assert_eq!(c.cpu_cores, 12);
        assert_eq!(c.mem_mb, 24_000);
        assert_eq!(c.jobs_hosted, 10);
        // One of two devices seen recently => uptime 0.5.
        assert!((c.uptime - 0.5).abs() < 1e-9);
        // 1000 earned over 10 delivered => avg price 100.
        assert_eq!(c.avg_price_base, 100);
    }

    #[test]
    fn trust_rewards_both_halves_monotonically() {
        let w = Weights::default();
        let nobody = trust_score(&SocialKarma::default(), &ComputeTrust::default(), None, &w);

        let mut social = SocialKarma::default();
        social.post_score = 200;
        social.effective_score = 30.0; // trust-weighted approval from real members
        let socially_strong = trust_score(&social, &ComputeTrust::default(), None, &w);
        assert!(socially_strong.combined > nobody.combined);

        let mut compute = ComputeTrust::default();
        compute.jobs_hosted = 100;
        let compute_strong = trust_score(&SocialKarma::default(), &compute, None, &w);
        assert!(compute_strong.combined > nobody.combined);

        let both = trust_score(&social, &compute, None, &w);
        assert!(both.combined > socially_strong.combined);
        assert!(both.combined > compute_strong.combined);
        assert!(both.combined <= 1.0 && both.combined >= 0.0);
    }

    #[test]
    fn flaky_compute_is_discounted() {
        let w = Weights::default();
        let mut reliable = ComputeTrust::default();
        reliable.jobs_hosted = 50;
        let mut flaky = ComputeTrust::default();
        flaky.jobs_hosted = 50;
        flaky.expiries = 50; // half the time a no-show
        let a = trust_score(&SocialKarma::default(), &reliable, None, &w);
        let b = trust_score(&SocialKarma::default(), &flaky, None, &w);
        assert!(a.combined > b.combined);
    }

    #[test]
    fn graph_rank_lifts_trust_and_is_droppable() {
        let w = Weights::default();
        let social = SocialKarma::default();
        let compute = ComputeTrust::default();
        // A node well-embedded in the web of trust outranks an unconnected one...
        let connected = trust_score(&social, &compute, Some(1.0), &w);
        let isolated = trust_score(&social, &compute, Some(0.0), &w);
        assert!(connected.combined > isolated.combined);
        // ...and passing None drops the graph half entirely (degraded client), so it cannot drag
        // the score down the way an explicit 0.0 rank does.
        let no_graph = trust_score(&social, &compute, None, &w);
        assert!(no_graph.combined >= isolated.combined);
    }

    #[test]
    fn decay_halves_each_half_life() {
        assert!((decay(0, 100) - 1.0).abs() < 1e-9);
        assert!((decay(100, 100) - 0.5).abs() < 1e-9);
        assert!((decay(200, 100) - 0.25).abs() < 1e-9);
        assert_eq!(decay(10_000, 0), 1.0, "half_life 0 disables decay");
    }
}
