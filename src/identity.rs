use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chacha20poly1305::{
    aead::{Aead, AeadCore, OsRng as ChaChaOsRng},
    ChaCha20Poly1305, KeyInit,
};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use hkdf::Hkdf;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::path::{Path, PathBuf};
use x25519_dalek::{PublicKey as X25519Public, StaticSecret as X25519Secret};

const KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;
const RESULT_ENCRYPTION_INFO: &[u8] = b"boinc-quota-result-encryption";

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    /// base64-encoded 32-byte signing key scalar
    signing_key_b64: String,
    /// base64-encoded 32-byte X25519 secret (optional for backwards compatibility)
    #[serde(default)]
    encryption_key_b64: Option<String>,
}

impl IdentityFile {
    fn from_identity(identity: &Identity) -> Self {
        Self {
            signing_key_b64: BASE64.encode(identity.signing_key.to_bytes()),
            encryption_key_b64: Some(BASE64.encode(identity.encryption_secret.to_bytes())),
        }
    }

    fn signing_key(&self) -> Option<SigningKey> {
        decode_fixed_key::<KEY_BYTES>(&self.signing_key_b64)
            .map(|bytes| SigningKey::from_bytes(&bytes))
    }

    fn encryption_secret(&self) -> Option<X25519Secret> {
        self.encryption_key_b64
            .as_deref()
            .and_then(decode_fixed_key::<KEY_BYTES>)
            .map(X25519Secret::from)
    }
}

pub struct Identity {
    signing_key: SigningKey,
    encryption_secret: X25519Secret,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedBlob {
    pub ephemeral_pubkey: [u8; 32],
    /// 12 bytes
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[derive(Debug)]
pub enum CryptoError {
    InvalidKey,
    Decrypt,
}

impl Identity {
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let encryption_secret = X25519Secret::random_from_rng(OsRng);
        Self {
            signing_key,
            encryption_secret,
        }
    }

    pub fn default_path() -> PathBuf {
        if let Ok(p) = std::env::var("BOINC_IDENTITY") {
            return PathBuf::from(p);
        }
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".boinc-quota")
            .join("identity.json")
    }

    pub fn load_or_generate(path: &Path) -> Self {
        if let Some(identity) = Self::load(path) {
            if identity.needs_encryption_key_migration(path) {
                identity.save(path);
            }
            return identity;
        }

        let id = Self::generate();
        id.save(path);
        id
    }

    fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read_to_string(path).ok()?;
        let file = serde_json::from_str::<IdentityFile>(&data).ok()?;
        let signing_key = file.signing_key()?;
        let encryption_secret = file
            .encryption_secret()
            .unwrap_or_else(|| X25519Secret::random_from_rng(OsRng));

        Some(Self {
            signing_key,
            encryption_secret,
        })
    }

    fn needs_encryption_key_migration(&self, path: &Path) -> bool {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|data| serde_json::from_str::<IdentityFile>(&data).ok())
            .is_some_and(|file| file.encryption_key_b64.is_none())
    }

    pub fn save(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = IdentityFile::from_identity(self);
        if let Ok(json) = serde_json::to_string_pretty(&file) {
            let _ = std::fs::write(path, json);
        }
    }

    /// 32-byte Ed25519 public key
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Short hex for display (first 8 bytes = 16 hex chars)
    pub fn public_key_short(&self) -> String {
        hex::encode(&self.public_key_bytes()[..8])
    }

    /// Full hex of public key
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.public_key_bytes())
    }

    /// 32-byte X25519 public key for encryption
    pub fn encryption_pubkey_bytes(&self) -> [u8; 32] {
        X25519Public::from(&self.encryption_secret).to_bytes()
    }

    pub fn encryption_pubkey_short(&self) -> String {
        hex::encode(&self.encryption_pubkey_bytes()[..8])
    }

    /// Sign a 32-byte nonce. Returns 64-byte signature as Vec<u8>.
    pub fn sign_nonce(&self, nonce: &[u8; 32]) -> Vec<u8> {
        self.signing_key.sign(nonce).to_bytes().to_vec()
    }

    /// Verify a signature against a public key and nonce.
    pub fn verify(public_key_bytes: &[u8; 32], nonce: &[u8; 32], signature_bytes: &[u8]) -> bool {
        let Ok(vk) = VerifyingKey::from_bytes(public_key_bytes) else {
            return false;
        };
        let Ok(arr) = <[u8; 64]>::try_from(signature_bytes) else {
            return false;
        };
        let sig = ed25519_dalek::Signature::from_bytes(&arr);
        use ed25519_dalek::Verifier;
        vk.verify(nonce, &sig).is_ok()
    }

    /// Encrypt plaintext for a recipient X25519 pubkey using ECDH + HKDF + ChaCha20-Poly1305.
    pub fn encrypt_for(recipient_pubkey: &[u8; 32], plaintext: &[u8]) -> EncryptedBlob {
        let ephemeral_secret = X25519Secret::random_from_rng(OsRng);
        let ephemeral_pubkey = X25519Public::from(&ephemeral_secret);
        let recipient_pk = X25519Public::from(*recipient_pubkey);

        let shared = ephemeral_secret.diffie_hellman(&recipient_pk);

        let hk = Hkdf::<Sha256>::new(None, shared.as_bytes());
        let mut key = [0u8; 32];
        hk.expand(RESULT_ENCRYPTION_INFO, &mut key).expect("hkdf");

        let cipher = ChaCha20Poly1305::new(&key.into());
        let nonce = ChaCha20Poly1305::generate_nonce(&mut ChaChaOsRng);
        let ciphertext = cipher.encrypt(&nonce, plaintext).expect("encrypt");

        EncryptedBlob {
            ephemeral_pubkey: ephemeral_pubkey.to_bytes(),
            nonce: nonce.to_vec(),
            ciphertext,
        }
    }

    /// Decrypt with our X25519 secret.
    pub fn decrypt(&self, blob: &EncryptedBlob) -> Result<Vec<u8>, CryptoError> {
        let ephemeral_pk = X25519Public::from(blob.ephemeral_pubkey);
        let shared = self.encryption_secret.diffie_hellman(&ephemeral_pk);

        let hk = Hkdf::<Sha256>::new(None, shared.as_bytes());
        let mut key = [0u8; 32];
        hk.expand(RESULT_ENCRYPTION_INFO, &mut key)
            .map_err(|_| CryptoError::InvalidKey)?;

        let cipher = ChaCha20Poly1305::new(&key.into());
        if blob.nonce.len() != NONCE_BYTES {
            return Err(CryptoError::InvalidKey);
        }
        let nonce_arr: [u8; NONCE_BYTES] = blob
            .nonce
            .as_slice()
            .try_into()
            .map_err(|_| CryptoError::InvalidKey)?;
        cipher
            .decrypt(&nonce_arr.into(), blob.ciphertext.as_slice())
            .map_err(|_| CryptoError::Decrypt)
    }
}

fn decode_fixed_key<const N: usize>(encoded: &str) -> Option<[u8; N]> {
    let bytes = BASE64.decode(encoded).ok()?;
    bytes.as_slice().try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let identity = Identity::generate();
        let nonce = [42u8; 32];
        let sig = identity.sign_nonce(&nonce);
        assert!(Identity::verify(&identity.public_key_bytes(), &nonce, &sig));
    }

    #[test]
    fn wrong_nonce_fails_verification() {
        let identity = Identity::generate();
        let nonce = [1u8; 32];
        let sig = identity.sign_nonce(&nonce);
        let wrong_nonce = [2u8; 32];
        assert!(!Identity::verify(
            &identity.public_key_bytes(),
            &wrong_nonce,
            &sig
        ));
    }

    #[test]
    fn truncated_signature_fails_verification() {
        let identity = Identity::generate();
        let nonce = [7u8; 32];
        let mut sig = identity.sign_nonce(&nonce);
        sig.truncate(32); // corrupt: too short
        assert!(!Identity::verify(
            &identity.public_key_bytes(),
            &nonce,
            &sig
        ));
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let recipient = Identity::generate();
        let plaintext = b"hello, this is a secret task result with some bytes 0123456789";
        let blob = Identity::encrypt_for(&recipient.encryption_pubkey_bytes(), plaintext);
        let decrypted = recipient.decrypt(&blob).expect("decrypt should succeed");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        let recipient = Identity::generate();
        let attacker = Identity::generate();
        let plaintext = b"top secret";
        let blob = Identity::encrypt_for(&recipient.encryption_pubkey_bytes(), plaintext);
        assert!(attacker.decrypt(&blob).is_err());
    }
}
