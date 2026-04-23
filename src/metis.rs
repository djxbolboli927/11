use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Client for the Metis (Jupiter self-hosted) routing engine.
pub struct MetisClient {
    base_url: String,
    http: Client,
}

// ---------- Quote types ----------

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    pub input_mint: String,
    pub in_amount: String,
    pub output_mint: String,
    pub out_amount: String,
    pub other_amount_threshold: String,
    pub swap_mode: String,
    pub price_impact_pct: String,
    pub route_plan: serde_json::Value,
    #[serde(default)]
    pub context_slot: Option<u64>,
    /// Returned by Metis v7.0.5+ when the /quote call uses `instructionVersion=V2`.
    /// Will be `Some("V2")` on an up-to-date server, `None` on older binaries
    /// (in which case the bot silently falls back to the legacy `route` instruction).
    #[serde(default)]
    pub instruction_version: Option<String>,
    /// Catch-all for extra fields returned by Metis.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

// ---------- Swap-instructions types ----------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsRequest {
    pub user_public_key: String,
    pub quote_response: serde_json::Value,
    pub wrap_and_unwrap_sol: bool,
    pub use_shared_accounts: bool,
    pub dynamic_compute_unit_limit: bool,
    pub skip_user_accounts_rpc_calls: bool,
    pub as_legacy_transaction: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsResponse {
    #[serde(default)]
    pub compute_budget_instructions: Vec<InstructionData>,
    #[serde(default)]
    pub setup_instructions: Vec<InstructionData>,
    pub swap_instruction: InstructionData,
    #[serde(default)]
    pub cleanup_instruction: Option<InstructionData>,
    #[serde(default)]
    pub address_lookup_table_addresses: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InstructionData {
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountMeta {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
}

impl MetisClient {
    pub fn new(base_url: &str, timeout_ms: u64) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .pool_max_idle_per_host(10)
            .tcp_nodelay(true)
            .build()
            .expect("failed to build http client");
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    /// Get a quote from Metis.
    ///
    /// Parameters:
    /// - slippageBps=0: zero slippage at quote layer; the real on-chain floor
    ///   is set via `other_amount_threshold` in `merge_quotes`.
    /// - onlyDirectRoutes=false: allow multi-hop for better routes
    /// - maxAccounts=50: leave room for tip account in final tx
    /// - forJitoBundle=true: excludes Jito-incompatible DEXes
    /// - swapMode=ExactIn: exact input amount
    /// - restrictIntermediateTokens=false: allow all intermediate tokens
    /// - instructionVersion=V2: tells Metis to produce a route_plan that uses
    ///   `bps: 10000` (instead of legacy `percent: 100`). When this QuoteResponse
    ///   is later sent to /swap-instructions, Metis builds a `route_v2` instruction
    ///   which costs fewer compute units and is what competing arb bots use.
    /// - excludeDexes: none. Every DEX registered in program_registry is
    ///   allowed to route. PMM DEXes (Tessera, SolFi, ZeroFi) bypass the
    ///   local simulator when it is enabled; when disabled they go to Jito
    ///   directly like any other route.
    pub async fn get_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_lamports: u64,
    ) -> Result<QuoteResponse> {
        let url = format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}\
             &slippageBps=0\
             &onlyDirectRoutes=false\
             &maxAccounts=50\
             &swapMode=ExactIn\
             &forJitoBundle=true\
             &restrictIntermediateTokens=false\
             &instructionVersion=V2",
            self.base_url, input_mint, output_mint, amount_lamports
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("quote request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("quote failed: {} -- {}", status, body);
        }

        let quote: QuoteResponse = resp.json().await.context("failed to parse quote response")?;
        Ok(quote)
    }

    /// Merge two quotes into a single circular quote via Route Concatenation.
    ///
    /// Takes quote1 (WSOL->Token) and quote2 (Token->WSOL),
    /// concatenates their routePlans, and produces a single combined quote
    /// that represents the full circular path WSOL->Token->WSOL.
    ///
    /// The combined quote is then sent to /swap-instructions to get
    /// a SINGLE route_v2 instruction that handles the entire circular arb.
    ///
    /// `min_acceptable_out` is the minimum output lamports we will tolerate
    /// on-chain. It becomes the `other_amount_threshold` of the merged quote
    /// and is embedded as `slippage_bps` floor in the route_v2 instruction.
    /// Setting it to `amount + tip + base_fee` means the tx reverts ONLY if
    /// the trade would lose lamports -- any price jitter that still leaves us
    /// at break-even or better will land on-chain (even if profit shrinks).
    pub fn merge_quotes(
        quote1: &QuoteResponse,
        quote2: &QuoteResponse,
        min_acceptable_out: u64,
    ) -> Result<QuoteResponse> {
        // Concatenate routePlans: q1.routePlan + q2.routePlan
        let route_plan1 = quote1
            .route_plan
            .as_array()
            .context("quote1 routePlan is not an array")?;
        let route_plan2 = quote2
            .route_plan
            .as_array()
            .context("quote2 routePlan is not an array")?;

        let mut combined_route_plan = route_plan1.clone();
        combined_route_plan.extend(route_plan2.iter().cloned());

        // Build merged quote:
        // - inputMint, inAmount from quote1 (WSOL input)
        // - outputMint, outAmount from quote2 (WSOL output)
        // - routePlan = concatenated
        // - otherAmountThreshold = min_acceptable_out (break-even floor, NOT quote2.outAmount)
        // - instructionVersion propagates from quote1 (must be "V2" for route_v2)
        Ok(QuoteResponse {
            input_mint: quote1.input_mint.clone(),
            in_amount: quote1.in_amount.clone(),
            output_mint: quote2.output_mint.clone(),
            out_amount: quote2.out_amount.clone(),
            other_amount_threshold: min_acceptable_out.to_string(),
            swap_mode: quote1.swap_mode.clone(),
            price_impact_pct: "0".to_string(),
            route_plan: serde_json::Value::Array(combined_route_plan),
            context_slot: quote2.context_slot,
            instruction_version: quote1.instruction_version.clone(),
            extra: quote1.extra.clone(),
        })
    }

    /// Get swap instructions for a merged circular quote.
    ///
    /// CRITICAL for circular arbitrage:
    /// - useSharedAccounts=false (shared accounts cause memory conflicts in circular swaps)
    /// - dynamicComputeUnitLimit=false (avoid extra RPC simulation call by Metis -- we set CU manually)
    /// - wrapAndUnwrapSol=false (WSOL ATA must pre-exist)
    /// - asLegacyTransaction=false (v0 for ALT support)
    pub async fn get_swap_instructions(
        &self,
        user_pubkey: &str,
        quote_response: &QuoteResponse,
    ) -> Result<SwapInstructionsResponse> {
        let quote_value = serde_json::to_value(quote_response)?;

        let body = SwapInstructionsRequest {
            user_public_key: user_pubkey.to_string(),
            quote_response: quote_value,
            wrap_and_unwrap_sol: false,
            use_shared_accounts: false,
            dynamic_compute_unit_limit: false,
            skip_user_accounts_rpc_calls: true,
            as_legacy_transaction: false,
        };

        let url = format!("{}/swap-instructions", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .context("swap-instructions request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("swap-instructions failed: {} -- {}", status, body);
        }

        let swap_ixs: SwapInstructionsResponse = resp
            .json()
            .await
            .context("failed to parse swap-instructions response")?;
        Ok(swap_ixs)
    }
}
