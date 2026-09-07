//! Sign in with Google for the browser, so the operator token can go back
//! to being a machine credential.
//!
//! The flow is the plain OpenID Connect authorization code exchange: the
//! browser is sent to Google, Google sends it back with a code, the caller
//! trades the code for tokens over TLS with its client secret, asks Google
//! who the user is, and only then checks the email against the allowlist. A
//! signed cookie carries the login from there. No JWT verification is
//! needed because the identity comes straight from Google's userinfo
//! endpoint on a channel the caller opened itself.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

pub const SESSION_COOKIE: &str = "ceilidh_session";
pub const STATE_COOKIE: &str = "ceilidh_oauth_state";
const SESSION_DAYS: u64 = 30;
const STATE_SECONDS: u64 = 600;

type HmacSha256 = Hmac<Sha256>;

/// Everything the caller needs to run the Google flow. Absent means the
/// login screen falls back to the token field.
#[derive(Debug, Clone)]
pub struct GoogleAuth {
    pub client_id: String,
    pub client_secret: String,
    /// Where the caller is reachable, used to build the redirect URI.
    pub public_url: String,
    /// Lower-cased emails allowed in.
    pub allowed_emails: Vec<String>,
}

impl GoogleAuth {
    pub fn from_env_values(
        client_id: Option<String>,
        client_secret: Option<String>,
        public_url: Option<String>,
        allowed_emails: Option<String>,
    ) -> Option<Self> {
        let client_id = client_id.filter(|v| !v.trim().is_empty())?;
        let client_secret = client_secret.filter(|v| !v.trim().is_empty())?;
        let public_url = public_url.filter(|v| !v.trim().is_empty())?;
        let allowed_emails = allowed_emails
            .unwrap_or_default()
            .split(',')
            .map(|e| e.trim().to_ascii_lowercase())
            .filter(|e| !e.is_empty())
            .collect::<Vec<_>>();
        if allowed_emails.is_empty() {
            return None;
        }
        Some(Self {
            client_id,
            client_secret,
            public_url: public_url.trim_end_matches('/').to_string(),
            allowed_emails,
        })
    }

    pub fn redirect_uri(&self) -> String {
        format!("{}/auth/callback", self.public_url)
    }

    pub fn allows(&self, email: &str) -> bool {
        let email = email.trim().to_ascii_lowercase();
        self.allowed_emails.iter().any(|allowed| allowed == &email)
    }

    /// The URL the browser is sent to.
    pub fn authorize_url(&self, state: &str) -> String {
        let query = [
            ("client_id", self.client_id.as_str()),
            ("redirect_uri", &self.redirect_uri()),
            ("response_type", "code"),
            ("scope", "openid email"),
            ("state", state),
            ("prompt", "select_account"),
            ("access_type", "online"),
        ]
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
        format!("https://accounts.google.com/o/oauth2/v2/auth?{query}")
    }
}

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
}

#[derive(Debug, Deserialize)]
pub struct UserInfo {
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: bool,
}

/// Trade the code for an access token, then ask Google who this is.
pub async fn exchange_code(
    client: &reqwest::Client,
    auth: &GoogleAuth,
    code: &str,
) -> anyhow::Result<UserInfo> {
    let token: TokenResponse = client
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("code", code),
            ("client_id", auth.client_id.as_str()),
            ("client_secret", auth.client_secret.as_str()),
            ("redirect_uri", &auth.redirect_uri()),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let info: UserInfo = client
        .get("https://openidconnect.googleapis.com/v1/userinfo")
        .bearer_auth(token.access_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(info)
}

/// Signs and verifies the cookies. The key is whatever the operator gave
/// (`CEILIDH_COOKIE_SECRET`), else derived from the bearer token, so a
/// token rotation logs every browser out, which is the right default.
#[derive(Clone)]
pub struct CookieSigner {
    key: Vec<u8>,
}

impl CookieSigner {
    pub fn new(secret: &str) -> Self {
        Self {
            key: secret.as_bytes().to_vec(),
        }
    }

    /// `<base64(payload)>.<base64(hmac)>`; payload is `<kind>|<subject>|<expiry>`.
    pub fn sign(&self, kind: &str, subject: &str, ttl_seconds: u64) -> String {
        let expiry = now() + ttl_seconds;
        let payload = format!("{kind}|{subject}|{expiry}");
        let encoded = B64.encode(payload.as_bytes());
        let mac = self.mac(&encoded);
        format!("{encoded}.{mac}")
    }

    /// The subject if the cookie is genuine, of the right kind, and unexpired.
    pub fn verify(&self, kind: &str, value: &str) -> Option<String> {
        let (encoded, mac) = value.split_once('.')?;
        if !constant_time_eq(self.mac(encoded).as_bytes(), mac.as_bytes()) {
            return None;
        }
        let payload = String::from_utf8(B64.decode(encoded).ok()?).ok()?;
        let mut parts = payload.splitn(3, '|');
        let got_kind = parts.next()?;
        let subject = parts.next()?;
        let expiry: u64 = parts.next()?.parse().ok()?;
        if got_kind != kind || expiry < now() {
            return None;
        }
        Some(subject.to_string())
    }

    fn mac(&self, data: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac accepts any key length");
        mac.update(data.as_bytes());
        B64.encode(mac.finalize().into_bytes())
    }
}

pub fn session_cookie(signer: &CookieSigner, email: &str) -> String {
    let value = signer.sign("session", email, SESSION_DAYS * 86_400);
    format!(
        "{SESSION_COOKIE}={value}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age={}",
        SESSION_DAYS * 86_400
    )
}

pub fn state_cookie(signer: &CookieSigner, state: &str) -> String {
    let value = signer.sign("state", state, STATE_SECONDS);
    format!("{STATE_COOKIE}={value}; Path=/auth; HttpOnly; Secure; SameSite=Lax; Max-Age={STATE_SECONDS}")
}

pub fn clear_cookie(name: &str, path: &str) -> String {
    format!("{name}=; Path={path}; HttpOnly; Secure; SameSite=Lax; Max-Age=0")
}

/// The named cookie's raw value from a `Cookie:` header.
pub fn cookie_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header.split(';').find_map(|pair| {
        let (k, v) = pair.trim().split_once('=')?;
        (k.trim() == name).then_some(v.trim())
    })
}

pub fn random_token() -> String {
    let mut bytes = [0u8; 24];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
    B64.encode(bytes)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> GoogleAuth {
        GoogleAuth::from_env_values(
            Some("id".into()),
            Some("secret".into()),
            Some("https://caller.example/".into()),
            Some(" Someone@Example.com , other@example.com".into()),
        )
        .unwrap()
    }

    #[test]
    fn config_needs_every_piece_and_normalizes_emails() {
        assert!(GoogleAuth::from_env_values(None, Some("s".into()), Some("u".into()), Some("a@b".into())).is_none());
        assert!(GoogleAuth::from_env_values(Some("i".into()), Some("s".into()), Some("u".into()), Some(" ".into())).is_none());
        let a = auth();
        assert_eq!(a.redirect_uri(), "https://caller.example/auth/callback");
        assert!(a.allows("SOMEONE@example.com"));
        assert!(!a.allows("stranger@example.com"));
    }

    #[test]
    fn authorize_url_carries_state_and_redirect() {
        let url = auth().authorize_url("st ate");
        assert!(url.starts_with("https://accounts.google.com/o/oauth2/v2/auth?"));
        assert!(url.contains("state=st%20ate"));
        assert!(url.contains("redirect_uri=https%3A%2F%2Fcaller.example%2Fauth%2Fcallback"));
        assert!(url.contains("scope=openid%20email"));
    }

    #[test]
    fn cookies_round_trip_and_reject_tampering() {
        let signer = CookieSigner::new("k");
        let value = signer.sign("session", "someone@example.com", 60);
        assert_eq!(signer.verify("session", &value).as_deref(), Some("someone@example.com"));
        assert!(signer.verify("state", &value).is_none(), "wrong kind");
        let mut forged = value.clone();
        forged.replace_range(0..1, if value.starts_with('A') { "B" } else { "A" });
        assert!(signer.verify("session", &forged).is_none());
        assert!(CookieSigner::new("other").verify("session", &value).is_none());
        let expired = signer.sign("session", "someone@example.com", 0);
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(signer.verify("session", &expired).is_none());
    }

    #[test]
    fn cookie_header_lookup() {
        let header = "a=1; ceilidh_session=abc.def; b=2";
        assert_eq!(cookie_value(header, SESSION_COOKIE), Some("abc.def"));
        assert_eq!(cookie_value(header, "missing"), None);
    }
}
