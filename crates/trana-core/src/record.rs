//! [`Record`] — the authored, content-addressed envelope every write becomes.
//!
//! A record's identity is derived, not assigned: `id = sha256(canonical_bytes(author, created_ms,
//! body))`. Two records with the same author, timestamp, and body collapse to the same id, which is
//! what makes replication trustless and idempotent — a node can be handed a record by anyone and
//! recompute its id to check it wasn't tampered with.
//!
//! ## Authorship
//!
//! `author` is a CE NodeId (an ed25519 public key, 64 hex). When a write enters trana over the mesh,
//! the node sets `author` to the *authenticated* sender (the local CE node verified its signature),
//! so authorship is trustworthy at ingest. `sig` additionally lets a record carry a detached
//! ed25519 signature over its canonical bytes, so a record stays self-verifying as it is gossiped and
//! re-served by replicas that are not its author — [`Record::verify`] checks it against the author
//! key. `sig` may be empty in deployments that rely solely on ingest-time authentication.

use crate::model::Body;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Errors that can arise validating a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
    /// The stored `id` does not equal the recomputed content hash.
    IdMismatch { expected: String, got: String },
    /// `author` is not 64 hex chars (a NodeId / ed25519 public key).
    BadAuthor,
    /// `sig` is present but not 128 hex chars (64-byte ed25519 signature).
    BadSignatureFormat,
    /// `sig` did not verify against the author key.
    BadSignature,
    /// Canonical encoding failed (should not happen for valid bodies).
    Encoding,
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::IdMismatch { expected, got } => {
                write!(f, "record id mismatch: claims {expected}, hashes to {got}")
            }
            RecordError::BadAuthor => write!(f, "author is not a 64-hex NodeId"),
            RecordError::BadSignatureFormat => write!(f, "signature is not 128 hex chars"),
            RecordError::BadSignature => write!(f, "signature did not verify against author key"),
            RecordError::Encoding => write!(f, "canonical encoding failed"),
        }
    }
}

impl std::error::Error for RecordError {}

/// The canonical, deterministic byte encoding hashed to form a record's id and signed for `sig`.
///
/// Uses `bincode` over the `(author, created_ms, body)` tuple — bincode is order-deterministic and
/// has no map-ordering ambiguity, so the same logical record always produces the same bytes (hence
/// the same id) on every platform, including `wasm32`.
pub fn canonical_bytes(author: &str, created_ms: u64, body: &Body) -> Result<Vec<u8>, RecordError> {
    bincode::serialize(&(author, created_ms, body)).map_err(|_| RecordError::Encoding)
}

/// An authored, content-addressed record — the unit of everything written to trana.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// Content hash of the canonical body — `sha256(canonical_bytes(author, created_ms, body))`.
    pub id: String,
    /// Author NodeId (ed25519 public key, 64 hex).
    pub author: String,
    /// Author-asserted creation time, unix milliseconds.
    pub created_ms: u64,
    /// The payload.
    pub body: Body,
    /// Detached ed25519 signature over the canonical bytes (128 hex), or empty.
    #[serde(default)]
    pub sig: String,
}

impl Record {
    /// Build a record from its parts, computing the content-addressed id. `sig` is left empty; call
    /// [`Record::signed`] instead to attach a signature, or [`Record::sign_with`] to sign in place.
    pub fn new(author: impl Into<String>, created_ms: u64, body: Body) -> Result<Self, RecordError> {
        let author = author.into();
        let id = crate::cid(&canonical_bytes(&author, created_ms, &body)?);
        Ok(Record { id, author, created_ms, body, sig: String::new() })
    }

    /// The canonical bytes this record hashes/signs over.
    pub fn canonical(&self) -> Result<Vec<u8>, RecordError> {
        canonical_bytes(&self.author, self.created_ms, &self.body)
    }

    /// Recompute the id from the body and check it matches `self.id`. Cheap integrity check that
    /// needs no keys — run it on every record received from the network before applying it.
    pub fn verify_id(&self) -> Result<(), RecordError> {
        let got = crate::cid(&self.canonical()?);
        if got != self.id {
            return Err(RecordError::IdMismatch { expected: self.id.clone(), got });
        }
        Ok(())
    }

    /// Full validation: well-formed author, matching id, and — if `sig` is non-empty — a valid
    /// ed25519 signature over the canonical bytes by the author key. Empty `sig` is accepted (ingest
    /// authenticated the author); a present-but-bad `sig` is rejected.
    pub fn verify(&self) -> Result<(), RecordError> {
        if self.author.len() != 64 || hex::decode(&self.author).is_err() {
            return Err(RecordError::BadAuthor);
        }
        self.verify_id()?;
        if self.sig.is_empty() {
            return Ok(());
        }
        self.verify_sig()
    }

    /// Verify the detached signature against the author key. Errors if `sig` is malformed or invalid.
    pub fn verify_sig(&self) -> Result<(), RecordError> {
        use ed25519_dalek::{Signature, VerifyingKey};
        let pk_bytes: [u8; 32] = hex::decode(&self.author)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or(RecordError::BadAuthor)?;
        let sig_bytes: [u8; 64] = hex::decode(&self.sig)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or(RecordError::BadSignatureFormat)?;
        let vk = VerifyingKey::from_bytes(&pk_bytes).map_err(|_| RecordError::BadAuthor)?;
        let sig = Signature::from_bytes(&sig_bytes);
        let msg = self.canonical()?;
        vk.verify_strict(&msg, &sig).map_err(|_| RecordError::BadSignature)
    }

    /// Attach a precomputed signature (128 hex) and return the record.
    pub fn signed(mut self, sig_hex: impl Into<String>) -> Self {
        self.sig = sig_hex.into();
        self
    }

    /// Sign this record in place with a raw 32-byte ed25519 secret key whose public half equals
    /// `author`. Used by clients that hold their own key (e.g. a phone). Servers usually leave `sig`
    /// empty and rely on authenticated ingest.
    pub fn sign_with(&mut self, secret: &[u8; 32]) -> Result<(), RecordError> {
        use ed25519_dalek::{Signer, SigningKey};
        let sk = SigningKey::from_bytes(secret);
        // Bind author to this key so a record can't be signed under someone else's name.
        let pk = hex::encode(sk.verifying_key().to_bytes());
        if pk != self.author {
            return Err(RecordError::BadAuthor);
        }
        let msg = self.canonical()?;
        self.sig = hex::encode(sk.sign(&msg).to_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Post, Vote};

    fn post_body() -> Body {
        Body::Post(Post {
            board: "ce-dev".into(),
            parent: None,
            title: Some("hello".into()),
            body: "first post".into(),
            media: vec![],
        })
    }

    #[test]
    fn id_is_content_addressed_and_stable() {
        let a = Record::new("ab".repeat(32), 1000, post_body()).unwrap();
        let b = Record::new("ab".repeat(32), 1000, post_body()).unwrap();
        assert_eq!(a.id, b.id, "same author+time+body => same id");
        let c = Record::new("ab".repeat(32), 1001, post_body()).unwrap();
        assert_ne!(a.id, c.id, "different time => different id");
    }

    #[test]
    fn verify_id_catches_tampering() {
        let mut r = Record::new("cd".repeat(32), 5, post_body()).unwrap();
        r.verify().unwrap();
        // Tamper with the body but keep the old id.
        if let Body::Post(p) = &mut r.body {
            p.body = "tampered".into();
        }
        assert!(matches!(r.verify_id(), Err(RecordError::IdMismatch { .. })));
    }

    #[test]
    fn empty_sig_is_accepted() {
        let r = Record::new("ef".repeat(32), 9, Body::Vote(Vote { target: "x".into(), value: 1 }))
            .unwrap();
        assert!(r.sig.is_empty());
        r.verify().unwrap();
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        use ed25519_dalek::SigningKey;
        // Deterministic key from fixed bytes (no rng needed in the test).
        let secret = [7u8; 32];
        let sk = SigningKey::from_bytes(&secret);
        let author = hex::encode(sk.verifying_key().to_bytes());

        let mut r = Record::new(author, 42, post_body()).unwrap();
        r.sign_with(&secret).unwrap();
        assert_eq!(r.sig.len(), 128);
        r.verify().unwrap();

        // A wrong-author signature is refused at signing time.
        let mut bad = Record::new("aa".repeat(32), 42, post_body()).unwrap();
        assert!(matches!(bad.sign_with(&secret), Err(RecordError::BadAuthor)));
    }

    #[test]
    fn corrupt_signature_is_rejected() {
        let secret = [9u8; 32];
        let sk = ed25519_dalek::SigningKey::from_bytes(&secret);
        let author = hex::encode(sk.verifying_key().to_bytes());
        let mut r = Record::new(author, 1, post_body()).unwrap();
        r.sign_with(&secret).unwrap();
        // Flip a byte in the signature.
        let mut bytes = hex::decode(&r.sig).unwrap();
        bytes[0] ^= 0xff;
        r.sig = hex::encode(bytes);
        assert!(matches!(r.verify(), Err(RecordError::BadSignature)));
    }
}
