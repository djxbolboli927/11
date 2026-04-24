//! Jito Block-Engine gRPC client (SearcherService).
//!
//! Auth is attempted on startup using the whitelisted keypair. If auth fails
//! (keypair not activated, endpoint mismatch, network error) the client falls
//! back to no-auth mode and sends bundles without an Authorization header.
//! This mirrors the Jito "NewNoAuth" pattern in their Go SDK.

use anyhow::{anyhow, Context, Result};
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;
use tracing::{debug, error, info, warn};

// Generated from proto/*.proto via build.rs + tonic_build.
mod pb {
    pub mod auth {
        tonic::include_proto!("auth");
    }
    pub mod bundle {
        tonic::include_proto!("bundle");
    }
    pub mod searcher {
        tonic::include_proto!("searcher");
    }
    pub mod packet {
        tonic::include_proto!("packet");
    }
    pub mod shared {
        tonic::include_proto!("shared");
    }
}

use pb::auth::{
    auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role,
};
use pb::bundle::Bundle;
use pb::packet::Packet;
use pb::searcher::{searcher_service_client::SearcherServiceClient, SendBundleRequest};

/// Active tokens returned by AuthService. Times are unix seconds.
#[derive(Clone)]
struct Tokens {
    access: String,
    access_expires_at: u64,
    refresh: String,
    refresh_expires_at: u64,
}

pub struct JitoGrpcClient {
    channel: Channel,
    /// None = no-auth mode (auth failed or skipped at startup).
    tokens: Arc<RwLock<Option<Tokens>>>,
}

impl JitoGrpcClient {
    /// Connect to the Jito gRPC endpoint and attempt authentication.
    ///
    /// If auth fails for any reason the client starts in **no-auth mode**:
    /// bundles are sent without an Authorization header. Many Jito endpoints
    /// accept unauthenticated gRPC bundles (equivalent to the REST UUID path).
    pub async fn new(endpoint: &str, keypair_path: &str) -> Result<Self> {
        let keypair = Arc::new(
            crate::wallet::read_keypair(keypair_path)
                .with_context(|| format!("failed to load gRPC auth keypair from {keypair_path}"))?,
        );
        let auth_pubkey = keypair.pubkey();

        let tls = ClientTlsConfig::new().with_webpki_roots();
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("invalid Jito gRPC endpoint {endpoint}"))?
            .tls_config(tls)
            .context("failed to configure TLS for Jito gRPC")?
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .with_context(|| format!("failed to connect to Jito gRPC at {endpoint}"))?;

        // Attempt auth. On failure fall through to no-auth mode instead of
        // returning Err — this lets the gRPC path stay alive even when the
        // keypair isn't whitelisted by Jito.
        let tokens = match authenticate(&channel, &keypair).await {
            Ok(initial) => {
                info!(
                    auth_pubkey = %auth_pubkey,
                    access_expires_at = initial.access_expires_at,
                    "Jito gRPC authenticated"
                );
                let t = Arc::new(RwLock::new(Some(initial)));

                // Background refresh loop.
                let refresh_channel = channel.clone();
                let refresh_tokens = t.clone();
                let refresh_keypair = keypair.clone();
                tokio::spawn(async move {
                    token_refresh_loop(refresh_channel, refresh_tokens, refresh_keypair).await;
                });

                t
            }
            Err(e) => {
                warn!(
                    error = %e,
                    auth_pubkey = %auth_pubkey,
                    "Jito gRPC auth failed -- running in no-auth mode (bundles sent without token)"
                );
                Arc::new(RwLock::new(None))
            }
        };

        Ok(Self { channel, tokens })
    }

    /// Serialize `tx`, wrap in a single-packet Bundle, and call
    /// SearcherService.SendBundle. Uses Bearer token when available,
    /// otherwise sends unauthenticated (no-auth mode).
    /// Returns the bundle UUID on success.
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;

        let packet = Packet {
            data: tx_bytes,
            meta: None,
        };
        let bundle = Bundle {
            header: None,
            packets: vec![packet],
        };

        let mut client = SearcherServiceClient::new(self.channel.clone());
        let mut req = Request::new(SendBundleRequest {
            bundle: Some(bundle),
        });

        // Attach Authorization header only when we have a valid token.
        if let Some(ref t) = *self.tokens.read().await {
            let auth_value: MetadataValue<_> = format!("Bearer {}", t.access)
                .parse()
                .map_err(|e| anyhow!("invalid access token: {e:?}"))?;
            req.metadata_mut().insert("authorization", auth_value);
        }

        let resp = client
            .send_bundle(req)
            .await
            .context("Jito SearcherService.SendBundle failed")?;
        let uuid = resp.into_inner().uuid;
        debug!(uuid = %uuid, "Jito gRPC bundle accepted");
        Ok(uuid)
    }
}

/// Run the AuthService challenge/sign/exchange dance and return fresh tokens.
async fn authenticate(channel: &Channel, keypair: &Keypair) -> Result<Tokens> {
    let mut auth = AuthServiceClient::new(channel.clone());
    let pubkey = keypair.pubkey();

    let challenge_resp = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await
        .context("GenerateAuthChallenge failed")?
        .into_inner();
    let challenge = challenge_resp.challenge;

    // Jito verifies the signature over "{pubkey_base58}-{challenge}".
    let to_sign = format!("{}-{}", pubkey, challenge);
    let signed = keypair.sign_message(to_sign.as_bytes());
    let signed_bytes = signed.as_ref().to_vec();

    let token_resp = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_bytes().to_vec(),
            signed_challenge: signed_bytes,
        })
        .await
        .context("GenerateAuthTokens failed")?
        .into_inner();

    let access = token_resp
        .access_token
        .ok_or_else(|| anyhow!("no access_token in response"))?;
    let refresh = token_resp
        .refresh_token
        .ok_or_else(|| anyhow!("no refresh_token in response"))?;

    Ok(Tokens {
        access_expires_at: timestamp_secs(access.expires_at_utc.as_ref()),
        access: access.value,
        refresh_expires_at: timestamp_secs(refresh.expires_at_utc.as_ref()),
        refresh: refresh.value,
    })
}

/// Background loop that refreshes the access token well before it expires.
/// Falls back to a full re-auth if RefreshAccessToken fails or the refresh
/// token itself is close to expiry.
async fn token_refresh_loop(
    channel: Channel,
    tokens: Arc<RwLock<Option<Tokens>>>,
    keypair: Arc<Keypair>,
) {
    loop {
        let sleep_secs = {
            let t = tokens.read().await;
            if let Some(ref t) = *t {
                let now = now_secs();
                t.access_expires_at
                    .saturating_sub(now)
                    .saturating_sub(60)
                    .max(10)
                    .min(300)
            } else {
                300
            }
        };
        tokio::time::sleep(Duration::from_secs(sleep_secs)).await;

        let needs_full_reauth = {
            let t = tokens.read().await;
            match *t {
                Some(ref t) => t.refresh_expires_at.saturating_sub(now_secs()) < 120,
                None => true,
            }
        };

        if needs_full_reauth {
            match authenticate(&channel, &keypair).await {
                Ok(new) => {
                    *tokens.write().await = Some(new);
                    info!("Jito gRPC tokens re-issued via full auth");
                }
                Err(e) => {
                    error!(error = %e, "Jito gRPC re-auth failed, retrying in 30s");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
            continue;
        }

        let refresh_token = {
            let t = tokens.read().await;
            t.as_ref().map(|t| t.refresh.clone())
        };
        let Some(refresh_token) = refresh_token else {
            continue;
        };

        let mut auth = AuthServiceClient::new(channel.clone());
        match auth
            .refresh_access_token(RefreshAccessTokenRequest { refresh_token })
            .await
        {
            Ok(resp) => {
                let inner = resp.into_inner();
                if let Some(new_access) = inner.access_token {
                    let mut w = tokens.write().await;
                    if let Some(ref mut t) = *w {
                        t.access_expires_at = timestamp_secs(new_access.expires_at_utc.as_ref());
                        t.access = new_access.value;
                    }
                    debug!("Jito gRPC access token refreshed");
                } else {
                    warn!("RefreshAccessToken returned empty access token, will re-auth");
                }
            }
            Err(e) => {
                warn!(error = %e, "RefreshAccessToken failed, falling back to full auth");
                match authenticate(&channel, &keypair).await {
                    Ok(new) => {
                        *tokens.write().await = Some(new);
                    }
                    Err(e2) => {
                        error!(error = %e2, "Jito gRPC re-auth also failed");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
            }
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn timestamp_secs(ts: Option<&prost_types::Timestamp>) -> u64 {
    match ts {
        Some(t) => t.seconds.max(0) as u64,
        None => 0,
    }
}
