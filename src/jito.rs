use anyhow::{Context, Result};
use futures::future::join_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::VersionedTransaction;
use tracing::{debug, info, warn};

/// Jito JSON-RPC client for bundle submission.
/// Sends bundles to MULTIPLE block engine endpoints concurrently.
pub struct JitoClient {
    http: Client,
    bundle_urls: Vec<String>,
}

#[derive(Serialize)]
struct SendBundleRpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: (Vec<String>,),
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<String>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl JitoClient {
    /// Create a Jito client that sends bundles to multiple endpoints concurrently.
    pub fn new(base_urls: &[String], uuid: &str) -> Self {
        let bundle_urls: Vec<String> = base_urls
            .iter()
            .map(|url| {
                format!(
                    "{}/api/v1/bundles?uuid={}",
                    url.trim_end_matches('/'),
                    uuid
                )
            })
            .collect();

        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build http client");

        info!(
            endpoints = bundle_urls.len(),
            "Jito multi-region client initialized"
        );

        Self { http, bundle_urls }
    }

    /// Send a single-transaction bundle to ALL Jito endpoints concurrently.
    /// Returns the first successful bundle ID.
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;
        let tx_base58 = bs58::encode(&tx_bytes).into_string();

        // Send to ALL endpoints concurrently
        let futures: Vec<_> = self
            .bundle_urls
            .iter()
            .map(|url| self.send_to_endpoint(url, &tx_base58))
            .collect();

        let results = join_all(futures).await;

        // Return first success, or the last error
        let mut last_err = None;
        for result in results {
            match result {
                Ok(bundle_id) => return Ok(bundle_id),
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no Jito endpoints configured")))
    }

    /// Send bundle to a single endpoint.
    async fn send_to_endpoint(&self, url: &str, tx_base58: &str) -> Result<String> {
        let request = SendBundleRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (vec![tx_base58.to_string()],),
        };

        let resp = self
            .http
            .post(url)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("Jito sendBundle to {} failed", url))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!(endpoint = url, http_status = %status, body = %body, "Jito HTTP error");
            anyhow::bail!("Jito HTTP error at {}: {} -- {}", url, status, body);
        }

        let rpc_resp: RpcResponse = resp
            .json()
            .await
            .context("failed to parse Jito response")?;

        if let Some(err) = rpc_resp.error {
            warn!(endpoint = url, code = err.code, message = %err.message, "Jito RPC error");
            anyhow::bail!(
                "Jito RPC error at {}: code={}, message={}",
                url,
                err.code,
                err.message
            );
        }

        let bundle_id = rpc_resp
            .result
            .ok_or_else(|| anyhow::anyhow!("Jito returned no result and no error"))?;

        debug!(endpoint = url, bundle_id = %bundle_id, "bundle accepted");
        Ok(bundle_id)
    }
}
