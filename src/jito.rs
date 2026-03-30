use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::VersionedTransaction;
use tracing::{info, warn};

/// Jito JSON-RPC client for bundle submission.
/// Uses the Jito Block Engine sendBundle API with UUID-based auth.
pub struct JitoClient {
    http: Client,
    bundle_url: String,
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
    /// Create a Jito client using JSON-RPC bundle API with UUID auth.
    ///
    /// URL format: https://<region>.mainnet.block-engine.jito.wtf
    /// UUID: provided by Jito for bundle submission auth
    pub fn new(base_url: &str, uuid: &str) -> Self {
        let bundle_url = format!(
            "{}/api/v1/bundles?uuid={}",
            base_url.trim_end_matches('/'),
            uuid
        );
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build http client");

        info!("Jito JSON-RPC client initialized");

        Self { http, bundle_url }
    }

    /// Send a single-transaction bundle via Jito JSON-RPC sendBundle.
    ///
    /// The transaction is serialized with bincode, then base58-encoded.
    /// Returns the bundle ID on success.
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;
        let tx_base58 = bs58::encode(&tx_bytes).into_string();

        let request = SendBundleRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (vec![tx_base58],),
        };

        let resp = self
            .http
            .post(&self.bundle_url)
            .json(&request)
            .send()
            .await
            .context("Jito sendBundle request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, body = %body, "Jito HTTP error");
            anyhow::bail!("Jito HTTP error: {} — {}", status, body);
        }

        let rpc_resp: RpcResponse = resp
            .json()
            .await
            .context("failed to parse Jito response")?;

        if let Some(err) = rpc_resp.error {
            warn!(code = err.code, message = %err.message, "Jito RPC error");
            anyhow::bail!("Jito RPC error: code={}, message={}", err.code, err.message);
        }

        let bundle_id = rpc_resp
            .result
            .ok_or_else(|| anyhow::anyhow!("Jito returned no result and no error"))?;

        Ok(bundle_id)
    }
}
