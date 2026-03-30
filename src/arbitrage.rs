use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer, transaction::VersionedTransaction};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::jito::JitoClient;
use crate::metis::{MetisClient, QuoteResponse};
use crate::rate_limiter::RateLimiter;
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// Look up CU limit from config based on hop count.
/// Index 0 = 2 hops, index 1 = 3 hops, etc.
/// If hops exceed the array length, the last value is used.
fn lookup_cu_limit(hop_count: usize, cu_limits: &[u32]) -> u32 {
    if cu_limits.is_empty() {
        return 200_000; // safe default
    }
    let index = hop_count.saturating_sub(2);
    let clamped = index.min(cu_limits.len() - 1);
    cu_limits[clamped]
}

/// Represents a profitable circular arbitrage opportunity.
struct Opportunity {
    token_mint: String,
    tip_lamports: u64,
    /// Merged quote (route concatenation): WSOL→Token→WSOL in one route.
    merged_quote: QuoteResponse,
}

/// Scan ALL tokens at each amount step before moving to the next step.
pub async fn scan_all_tokens(
    metis: &MetisClient,
    token_mints: &[String],
    config: &Config,
    jito: &JitoClient,
    trading_keypair: &Keypair,
    rpc_client: &RpcClient,
    sim_rpc_client: Option<&RpcClient>,
    jito_limiter: &mut RateLimiter,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let base_fee = config.trading.base_fee_lamports;

    let mut amount = min_lamports;
    while amount <= max_lamports {
        for token_mint in token_mints {
            // Leg 1: WSOL → Token
            let quote1 = match metis.get_quote(WSOL_MINT, token_mint, amount).await {
                Ok(q) => q,
                Err(_) => continue,
            };

            let token_amount: u64 = match quote1.out_amount.parse() {
                Ok(v) if v > 0 => v,
                _ => continue,
            };

            // Leg 2: Token → WSOL
            let quote2 = match metis.get_quote(token_mint, WSOL_MINT, token_amount).await {
                Ok(q) => q,
                Err(_) => continue,
            };

            let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);

            if output_wsol <= amount {
                continue;
            }

            let raw_profit = output_wsol - amount;
            let tip = transaction::calculate_tip(
                raw_profit,
                config.jito.tip_profit_percent,
                config.jito.tip_min_lamports,
                config.jito.tip_max_lamports,
            );

            // Total costs = Jito tip + base network fee (from config)
            let total_costs = tip + base_fee;

            if raw_profit <= total_costs + config.trading.min_profit_lamports {
                continue;
            }

            let net_profit = raw_profit - total_costs;

            if !jito_limiter.try_acquire() {
                debug!(token = token_mint.as_str(), "jito rate limit hit, dropping");
                continue;
            }

            // Merge quotes via Route Concatenation → single route instruction
            let merged_quote = match MetisClient::merge_quotes(&quote1, &quote2) {
                Ok(q) => q,
                Err(e) => {
                    warn!(error = %e, token = token_mint.as_str(), "quote merge failed");
                    continue;
                }
            };

            info!(
                token = token_mint.as_str(),
                input_sol = amount as f64 / LAMPORTS_PER_SOL,
                output_sol = output_wsol as f64 / LAMPORTS_PER_SOL,
                profit_lamports = net_profit,
                tip_lamports = tip,
                "PROFITABLE — executing"
            );

            let opp = Opportunity {
                token_mint: token_mint.clone(),
                tip_lamports: tip,
                merged_quote,
            };

            // Count hops from merged routePlan and look up CU limit
            let hop_count = opp
                .merged_quote
                .route_plan
                .as_array()
                .map(|a| a.len())
                .unwrap_or(2);
            let cu_limit = lookup_cu_limit(hop_count, &config.performance.cu_limits);
            debug!(
                token = token_mint.as_str(),
                hops = hop_count,
                cu_limit = cu_limit,
                "CU limit selected"
            );

            match execute_opportunity(
                &opp,
                metis,
                jito,
                trading_keypair,
                rpc_client,
                sim_rpc_client,
                cu_limit,
            )
            .await
            {
                Ok(uuid) => {
                    info!(
                        uuid = %uuid,
                        token = opp.token_mint.as_str(),
                        profit = net_profit,
                        "bundle sent to Jito"
                    );
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        token = opp.token_mint.as_str(),
                        "execution failed"
                    );
                }
            }
        }

        amount += step_lamports;
    }

    Ok(())
}

/// Simulate a transaction via RPC before sending to Jito.
/// Returns Ok(()) if simulation succeeds, Err if it fails.
fn simulate_transaction(
    sim_rpc: &RpcClient,
    tx: &VersionedTransaction,
) -> Result<()> {
    let result = sim_rpc.simulate_transaction(tx)
        .context("simulation RPC call failed")?;

    if let Some(err) = result.value.err {
        anyhow::bail!("simulation failed: {:?}", err);
    }

    Ok(())
}

/// Execute a circular arbitrage opportunity.
///
/// Flow: merge quote → swap-instructions → build tx → simulate → send to Jito
async fn execute_opportunity(
    opp: &Opportunity,
    metis: &MetisClient,
    jito: &JitoClient,
    trading_keypair: &Keypair,
    rpc_client: &RpcClient,
    sim_rpc_client: Option<&RpcClient>,
    cu_limit: u32,
) -> Result<String> {
    let user_pubkey = trading_keypair.pubkey().to_string();

    // Get swap instructions for the MERGED circular quote → single route instruction
    let swap_ixs = metis
        .get_swap_instructions(&user_pubkey, &opp.merged_quote)
        .await?;

    let recent_blockhash = rpc_client.get_latest_blockhash()?;

    // Build 3-instruction tx: CU limit (manual) + route + Jito tip
    let tx = transaction::build_arb_transaction(
        &swap_ixs,
        trading_keypair,
        opp.tip_lamports,
        cu_limit,
        recent_blockhash,
        rpc_client,
    )?;

    // Verify transaction size (max 1232 bytes for Solana MTU)
    let tx_bytes = bincode::serialize(&tx)?;
    if tx_bytes.len() > 1232 {
        anyhow::bail!("tx too large: {} bytes", tx_bytes.len());
    }

    // Simulate via eRPC before wasting a Jito rate limit slot
    if let Some(sim_rpc) = sim_rpc_client {
        simulate_transaction(sim_rpc, &tx)?;
        debug!("simulation passed");
    }

    let uuid = jito.send_bundle(&tx).await?;
    Ok(uuid)
}
