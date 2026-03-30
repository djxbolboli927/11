use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use tracing::{debug, info, warn};

use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::Config;
use crate::jito::JitoClient;
use crate::metis::{MetisClient, QuoteResponse};
use crate::rate_limiter::RateLimiter;
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

fn lookup_cu_limit(hop_count: usize, cu_limits: &[u32]) -> u32 {
    if cu_limits.is_empty() {
        return 200_000;
    }
    let index = hop_count.saturating_sub(2);
    let clamped = index.min(cu_limits.len() - 1);
    cu_limits[clamped]
}

struct Opportunity {
    token_mint: String,
    amount: u64,
    output_wsol: u64,
    tip_lamports: u64,
    net_profit: u64,
    merged_quote: QuoteResponse,
    hop_count: usize,
}

async fn check_opportunity(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    base_fee: u64,
    tip_percent: f64,
    tip_min: u64,
    tip_max: u64,
    min_profit: u64,
) -> Option<Opportunity> {
    let quote1 = metis.get_quote(WSOL_MINT, token_mint, amount).await.ok()?;
    let token_amount: u64 = quote1.out_amount.parse().ok().filter(|&v: &u64| v > 0)?;

    let quote2 = metis.get_quote(token_mint, WSOL_MINT, token_amount).await.ok()?;
    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);

    if output_wsol <= amount {
        return None;
    }

    let raw_profit = output_wsol - amount;
    let tip = transaction::calculate_tip(raw_profit, tip_percent, tip_min, tip_max);
    let total_costs = tip + base_fee;

    if raw_profit <= total_costs + min_profit {
        return None;
    }

    let merged_quote = MetisClient::merge_quotes(&quote1, &quote2).ok()?;
    let hop_count = merged_quote
        .route_plan
        .as_array()
        .map(|a| a.len())
        .unwrap_or(2);

    Some(Opportunity {
        token_mint: token_mint.to_string(),
        amount,
        output_wsol,
        tip_lamports: tip,
        net_profit: raw_profit - total_costs,
        merged_quote,
        hop_count,
    })
}

/// Scan ALL (amount × token) pairs concurrently.
/// Execute IMMEDIATELY on the first profitable result — send to ALL Jito endpoints at once.
pub async fn scan_all_tokens(
    metis: &MetisClient,
    token_mints: &[String],
    config: &Config,
    jito: &JitoClient,
    trading_keypair: &Keypair,
    rpc_client: &RpcClient,
    jito_limiter: &mut RateLimiter,
    blockhash_cache: &BlockhashCache,
    alt_cache: &AltCache,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let base_fee = config.trading.base_fee_lamports;

    let mut futs = FuturesUnordered::new();

    let mut amount = min_lamports;
    while amount <= max_lamports {
        for token_mint in token_mints {
            futs.push(check_opportunity(
                metis,
                token_mint,
                amount,
                base_fee,
                config.jito.tip_profit_percent,
                config.jito.tip_min_lamports,
                config.jito.tip_max_lamports,
                config.trading.min_profit_lamports,
            ));
        }
        amount += step_lamports;
    }

    while let Some(result) = futs.next().await {
        let opp = match result {
            Some(opp) => opp,
            None => continue,
        };

        if !jito_limiter.try_acquire() {
            debug!(token = opp.token_mint.as_str(), "jito rate limit hit, dropping");
            continue;
        }

        let cu_limit = lookup_cu_limit(opp.hop_count, &config.performance.cu_limits);

        info!(
            token = opp.token_mint.as_str(),
            input_sol = opp.amount as f64 / LAMPORTS_PER_SOL,
            output_sol = opp.output_wsol as f64 / LAMPORTS_PER_SOL,
            profit_lamports = opp.net_profit,
            tip_lamports = opp.tip_lamports,
            hops = opp.hop_count,
            cu_limit = cu_limit,
            "PROFITABLE — executing immediately"
        );

        match execute_opportunity(
            &opp,
            metis,
            jito,
            trading_keypair,
            rpc_client,
            cu_limit,
            blockhash_cache,
            alt_cache,
        )
        .await
        {
            Ok(uuid) => {
                info!(
                    uuid = %uuid,
                    token = opp.token_mint.as_str(),
                    profit = opp.net_profit,
                    "bundle sent to all Jito endpoints"
                );
            }
            Err(e) => {
                warn!(error = %e, token = opp.token_mint.as_str(), "execution failed");
            }
        }

        // Break after first execution — restart scan with fresh quotes.
        break;
    }

    Ok(())
}

/// Execute: get swap instructions → build tx → send directly to ALL Jito endpoints (no simulation).
async fn execute_opportunity(
    opp: &Opportunity,
    metis: &MetisClient,
    jito: &JitoClient,
    trading_keypair: &Keypair,
    rpc_client: &RpcClient,
    cu_limit: u32,
    blockhash_cache: &BlockhashCache,
    alt_cache: &AltCache,
) -> Result<String> {
    let user_pubkey = trading_keypair.pubkey().to_string();

    let swap_ixs = metis
        .get_swap_instructions(&user_pubkey, &opp.merged_quote)
        .await?;

    // Cached blockhash — zero RPC calls
    let recent_blockhash = blockhash_cache.get();

    // Build tx with ALT cache
    let tx = transaction::build_arb_transaction(
        &swap_ixs,
        trading_keypair,
        opp.tip_lamports,
        cu_limit,
        recent_blockhash,
        alt_cache,
        rpc_client,
    )?;

    let tx_bytes = bincode::serialize(&tx)?;
    if tx_bytes.len() > 1232 {
        anyhow::bail!("tx too large: {} bytes", tx_bytes.len());
    }

    // Send directly to ALL Jito endpoints concurrently — no simulation
    let uuid = jito.send_bundle(&tx).await?;
    Ok(uuid)
}
