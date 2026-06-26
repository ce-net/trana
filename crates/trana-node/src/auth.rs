//! Delegated identity ("act-as") via ce-iam / ce-cap capabilities — the production auth flow.
//!
//! trana's base identity is the authenticated mesh sender (the CE node verified its Ed25519
//! signature). On top of that, a user can **delegate**: from one device/identity `U`, issue a
//! capability to another device/agent `D` authorizing it to act as `U` in trana
//! (`ce grant <D> --can trana:act`). When `D` presents that capability, trana attributes the content
//! to `U`, not `D`.
//!
//! Security rests on ce-cap's chain rule: [`ce_cap::authorize`] requires the capability chain to
//! **root at the claimed identity's own key**. We verify with `self_id = U` and `accepted_roots =
//! [U]`, so a chain that authorizes acting-as-`U` can only have been signed by `U`. An attacker
//! cannot forge it, and a revoked delegation is refused via the on-chain revocation set.

use anyhow::{anyhow, Result};
use ce_rs::CeClient;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

/// The capability ability a holder must be granted to act as the issuer in trana.
pub const ACT_ABILITY: &str = "trana:act";

fn hex32(s: &str) -> Result<[u8; 32]> {
    hex::decode(s).ok().and_then(|b| b.try_into().ok()).ok_or_else(|| anyhow!("bad node id: {s}"))
}

/// Verify that `requester` (the authenticated sender) may act as `claimed_author`, by presenting the
/// ce-cap capability chain `cap_token`. Returns `Ok(())` only if the chain roots at `claimed_author`,
/// grants [`ACT_ABILITY`] to `requester`, is unexpired, and is not revoked.
pub async fn verify_act_as(
    ce: &CeClient,
    claimed_author: &str,
    requester: &str,
    cap_token: &str,
) -> Result<()> {
    let u = hex32(claimed_author)?;
    let d = hex32(requester)?;
    let chain = ce_cap::decode_chain(cap_token).map_err(|e| anyhow!("bad capability token: {e}"))?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|x| x.as_secs()).unwrap_or(0);
    // Consult the on-chain revocation set so a revoked delegation is refused.
    let revoked: HashSet<(String, u64)> =
        ce.revoked().await.unwrap_or_default().into_iter().collect();
    let is_revoked = |issuer: &[u8; 32], nonce: u64| revoked.contains(&(hex::encode(issuer), nonce));
    ce_cap::authorize(&u, &[u], &[], now, &d, ACT_ABILITY, &chain, &is_revoked)
        .map_err(|e| anyhow!("capability does not authorize acting as {claimed_author}: {e}"))
}
