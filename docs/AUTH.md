# trana auth — identity + ce-iam capability delegation

trana has two layers of authentication, both rooted in CE identity (a NodeId is an ed25519 public
key, and is exactly a ce-iam principal).

## 1. Base identity (always on)

Every write is attributed to the **authenticated mesh sender**. The local CE node verifies the
sender's ed25519 signature before the request reaches trana, so the `from` a handler sees is
cryptographically real. trana stamps content with that NodeId — there is no author field in a request
to forge. A peer can only ever act as itself.

## 2. Delegation — "act-as" via ce-iam / ce-cap (production auth flow)

A person owns many devices/agents. trana lets one identity authorize another to act on its behalf,
using a **ce-cap capability** (the same primitive ce-iam mints and verifies). No shared keys, no
passwords — a signed, attenuating, revocable capability.

### Flow

```
# 1. The user U (on any of their devices) grants device/agent D the right to act as U in trana:
ce grant <D-node-id> --can trana:act --resource self        # prints a capability token, signed by U

# 2. D holds that token and acts as U — e.g. via the CLI:
trana --as <U-node-id> --cap <token> post --board ce-dev --title "from my phone" --body "hi"

# or the SDKs:
#   Rust:  TranaClient::new(ce).with_act_as(u_node_id, token)
#   TS:    new Trana({ node, actAs: { author: uNodeId, cap: token } })
```

The SDK merges `_as` (claimed author) + `_cap` (token) into the request; the serving node verifies the
delegation **before any handler runs** and attributes the content to U.

### Why it is unforgeable

Verification calls `ce_cap::authorize` with `self_id = U` and `accepted_roots = [U]`. ce-cap requires
the capability chain to **root at the claimed identity's own key**, so a token authorizing
"act as U" can only have been signed by U. Concretely, trana checks that the chain:

- roots at **U** (so only U can delegate acting-as-U — an attacker cannot mint it);
- grants the **`trana:act`** ability to the leaf holder;
- whose leaf audience is the **authenticated sender** D;
- is **unexpired** and **not revoked** (consulted against CE's on-chain revocation set).

Anything else — a token signed by a third party, a malformed token, an expired/revoked one, or a
claimed author with no token at all — is **rejected**. (Proven by `e2e/e2e-trana-iam.sh`.)

### Attenuation & revocation

Because it is a ce-cap chain, a delegation can be **attenuated** (D can sub-delegate a narrower
capability to another agent, never broader) and **revoked** on-chain by the issuer
(`ce revoke <nonce>`), after which trana refuses it on the next request. Add an `--expires 30d` to
`ce grant` for time-boxed delegation.

### Where it runs

`crate::auth::verify_act_as` (trana-node) does the verification; `Engine::resolve_author` resolves the
effective author for every write; `service.rs` calls it up front so a forged delegation never reaches
a handler. Reads are unaffected (they carry no author).
