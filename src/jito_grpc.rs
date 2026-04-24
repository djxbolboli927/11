//! Jito Block-Engine gRPC client (SearcherService) — multi-region, NoAuth.
//!
//! Mirrors the REST client design in `jito.rs`: each `send_bundle` call
//! broadcasts the same bundle to ALL configured regional endpoints
//! concurrently. The first regional success is returned to the caller.
//!
//! NoAuth mode: per Jito's 2025 default-send policy, searcher pubkeys no
//! longer need to be whitelisted. We open a TLS gRPC channel per region
//! and call `SearcherService.SendBundle` without any `Authorization`
//! header. Jito enforces a 1 req/s/IP/region default rate limit, which
//! the dispatcher in `arbitrage.rs` plus the per-path RateLimiter respect.
//!
//! Duplicate prevention is the dispatcher's job: each profitable
//! opportunity goes to EITHER the REST path OR the gRPC path (REST first,
//! gRPC fallback when REST limiter is empty).

use anyhow::{Context, Result};
use futures::future::join_all;
use solana_sdk::transaction::VersionedTransaction;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;
use tracing::{debug, info, warn};

// Generated from proto/*.proto via build.rs + tonic_build.
// (auth/shared modules are compiled but unused at runtime in NoAuth mode.)
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

use pb::bundle::Bundle;
use pb::packet::Packet;
use pb::searcher::{searcher_service_client::SearcherServiceClient, SendBundleRequest};

/// One Jito Block Engine region: a single open TLS gRPC channel.
struct Region {
    endpoint: String,
    channel: Channel,
}

pub struct JitoGrpcClient {
    regions: Vec<Arc<Region>>,
}

impl JitoGrpcClient {
    /// Connect to every endpoint with TLS. Regions that fail to connect
    /// (DNS, TLS, network) are skipped; the client is usable as long as
    /// at least one region succeeds. No authentication is performed.
    pub async fn new(endpoints: &[String]) -> Result<Self> {
        let mut regions: Vec<Arc<Region>> = Vec::new();

        for endpoint in endpoints {
            match Self::connect_region(endpoint).await {
                Ok(region) => {
                    info!(endpoint = %endpoint, "Jito gRPC region connected (no-auth)");
                    regions.push(Arc::new(region));
                }
                Err(e) => {
                    warn!(
                        endpoint = %endpoint,
                        error = %e,
                        "Jito gRPC region failed to connect, skipping"
                    );
                }
            }
        }

        if regions.is_empty() {
            anyhow::bail!("no Jito gRPC regions could be reached");
        }

        info!(
            regions_total = regions.len(),
            "Jito gRPC multi-region client initialized (NoAuth mode)"
        );

        Ok(Self { regions })
    }

    /// Open one regional TLS channel with HTTP/2 keepalive.
    async fn connect_region(endpoint: &str) -> Result<Region> {
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

        Ok(Region {
            endpoint: endpoint.to_string(),
            channel,
        })
    }

    /// Serialize `tx` once, broadcast to ALL regions concurrently.
    /// Returns the first regional success, or the last error if every
    /// region failed.
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;

        let futures: Vec<_> = self
            .regions
            .iter()
            .map(|r| {
                let region = r.clone();
                let tx_bytes = tx_bytes.clone();
                async move { send_to_region(&region, tx_bytes).await }
            })
            .collect();

        let results = join_all(futures).await;

        let mut last_err = None;
        for result in results {
            match result {
                Ok(uuid) => return Ok(uuid),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no Jito gRPC regions configured")))
    }
}

/// SendBundle to one region without any Authorization metadata.
async fn send_to_region(region: &Region, tx_bytes: Vec<u8>) -> Result<String> {
    let bundle = Bundle {
        header: None,
        packets: vec![Packet {
            data: tx_bytes,
            meta: None,
        }],
    };

    let mut client = SearcherServiceClient::new(region.channel.clone());
    let req = Request::new(SendBundleRequest {
        bundle: Some(bundle),
    });

    let resp = client
        .send_bundle(req)
        .await
        .with_context(|| format!("Jito gRPC SendBundle failed at {}", region.endpoint))?;
    let uuid = resp.into_inner().uuid;
    debug!(endpoint = %region.endpoint, uuid = %uuid, "gRPC bundle accepted");
    Ok(uuid)
}
