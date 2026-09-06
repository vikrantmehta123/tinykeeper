use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};

#[derive(Debug, PartialEq, Eq)]
pub struct AuthId {
    pub scheme: String,
    pub id: String,
}

fn digest_identity(credentials: &str) -> AuthId {
    let username = match credentials.split_once(":") {
        Some((username, _password)) => username,
        None => credentials,
    };

    // "id" is constructed in ZooKeeper as: username + ":" + Base64(SHA1(username + ":" + password))
    let hash = Sha1::digest(credentials.as_bytes());
    let encoded_hash = STANDARD.encode(hash);

    AuthId {
        scheme: String::from("digest"),
        id: format!("{username}:{encoded_hash}"),
    }
}

#[derive(Debug)]
pub enum AuthError {
    UnsupportedScheme,
    InvalidCredentials,
}

/// Public endpoint
pub fn authenticate(scheme: &str, credentials: &[u8]) -> Result<AuthId, AuthError> {
    match scheme {
        "digest" => {
            let credentials =
                std::str::from_utf8(credentials).map_err(|_| AuthError::InvalidCredentials)?;

            Ok(digest_identity(credentials))
        }
        _ => Err(AuthError::UnsupportedScheme),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_zookeeper_format() {
        let identity = authenticate("digest", b"super:test").unwrap();

        assert_eq!(identity.scheme, "digest");
        assert_eq!(
            identity.id,
            "super:D/InIHSb7yEEbrWz8b9l71RjZJU="
        );
    }
}
