use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::account_cache::AccountCache;
use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::Config;
use crate::jito::JitoClient;
use crate::litesvm_sim::{self, Simulator};
use crate::metis::{MetisClient, SwapInstructionsResponse};
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

/// A profitable opportunity with PRE-FETCHED swap instructions.
/// By the time we build the tx, no more Metis calls are needed.
struct Opportunity {
    token_mint: String,
    amount: u64,
    output_wsol: u64,
    tip_lamports: u64,
    net_profit: u64,
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    cu_limit: u32,
}

/// Check a single (amount, token) pair for profitability.
/// If profitable, ALSO fetch swap-instructions in the same concurrent task.
/// This moves the 3rd Metis call OUT of the critical path.
async fn check_opportunity(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    base_fee: u64,
    tip_percent: f64,
    tip_min: u64,
    tip_max: u64,
    min_profit: u64,
    user_pubkey: &str,
    cu_limits: &[u32],
) -> Option<Opportunity> {
    // Leg 1: WSOL → Token
    let quote1 = metis.get_quote(WSOL_MINT, token_mint, amount).await.ok()?;
    let token_amount: u64 = quote1.out_amount.parse().ok().filter(|&v: &u64| v > 0)?;

    // Leg 2: Token → WSOL (input = output of leg 1)
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

    // On-chain break-even floor. The tx reverts ONLY if the final output would
    // be less than input + tip + base_fee (i.e. an actual net loss). Any positive
    // slippage — or even a shrunk-but-still-profitable outcome — still lands.
    // This is what competing arb bots do; locking threshold to quote2.out_amount
    // (zero negative slippage) was the root cause of frequent reverts.
    let min_acceptable_out = amount + total_costs;

    let merged_quote =
        MetisClient::merge_quotes(&quote1, &quote2, min_acceptable_out).ok()?;

    // Sanity check: if the Metis binary is older than v7.0.5, it ignores
    // instructionVersion=V2 and still emits legacy `route`. Log (don't abort)
    // so a stale server is visible in production traces.
    if merged_quote.instruction_version.as_deref() != Some("V2") {
        debug!(
            token = token_mint,
            got = ?merged_quote.instruction_version,
            "quote did NOT report instructionVersion=V2 — Metis binary may be outdated"
        );
    }
    let hop_count = merged_quote
        .route_plan
        .as_array()
        .map(|a| a.len())
        .unwrap_or(2);

    // CRITICAL: fetch swap-instructions HERE (concurrent with other scans).
    // This removes the 3-5ms gap between "opportunity found" and "tx sent".
    let swap_ixs = metis
        .get_swap_instructions(user_pubkey, &merged_quote)
        .await
        .ok()?;

    let cu_limit = lookup_cu_limit(hop_count, cu_limits);

    Some(Opportunity {
        token_mint: token_mint.to_string(),
        amount,
        output_wsol,
        tip_lamports: tip,
        net_profit: raw_profit - total_costs,
        swap_ixs,
        hop_count,
        cu_limit,
    })
}

/// Scan ALL (amount × token) pairs concurrently.
/// For each profitable pair, swap-instructions is pre-fetched in the same task.
/// First ready Opportunity triggers immediate tx build + send — NO more Metis calls.
pub async fn scan_all_tokens(
    metis: &MetisClient,
    token_mints: &[String],
    config: &Config,
    jito: &Arc<JitoClient>,
    trading_keypair: &Keypair,
    rpc_client: &RpcClient,
    jito_limiter: &mut RateLimiter,
    blockhash_cache: &BlockhashCache,
    alt_cache: &AltCache,
    sim_cache: Option<&Arc<AccountCache>>,
    simulator: Option<&Arc<Simulator>>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let base_fee = config.trading.base_fee_lamports;

    let user_pubkey = trading_keypair.pubkey().to_string();

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
                &user_pubkey,
                &config.performance.cu_limits,
            ));
        }
        amount += step_lamports;
    }

    // PARALLEL EXECUTION: as each profitable opportunity is ready, we IMMEDIATELY
    // build its tx AND kick off the Jito HTTP send on the tokio runtime via
    // `tokio::spawn`. UNLIKE `FuturesUnordered::push` (which only queues a future
    // and never polls it until the owner calls `.next().await`), `tokio::spawn`
    // starts executing the future on a worker thread the moment it is spawned.
    // That is crucial here: we do NOT want to wait for all quote tasks to finish
    // before the FIRST send hits the wire. Every ~100ms of delay against a fresh
    // quote is enough for the pool to move and the tx to revert on slippage.
    while let Some(result) = futs.next().await {
        let opp = match result {
            Some(opp) => opp,
            None => continue,
        };

        if !jito_limiter.try_acquire() {
            debug!(token = opp.token_mint.as_str(), "jito rate limit hit, dropping");
            continue;
        }

        info!(
            token = opp.token_mint.as_str(),
            input_sol = opp.amount as f64 / LAMPORTS_PER_SOL,
            output_sol = opp.output_wsol as f64 / LAMPORTS_PER_SOL,
            profit_lamports = opp.net_profit,
            tip_lamports = opp.tip_lamports,
            hops = opp.hop_count,
            cu_limit = opp.cu_limit,
            "PROFITABLE — instructions ready, building tx"
        );

        // Build the tx synchronously (CPU-bound, ~0.5ms; ALTs served from cache).
        let recent_blockhash = blockhash_cache.get();
        let tx = match transaction::build_arb_transaction(
            &opp.swap_ixs,
            trading_keypair,
            opp.tip_lamports,
            opp.cu_limit,
            recent_blockhash,
            alt_cache,
            rpc_client,
        ) {
            Ok(tx) => tx,
            Err(e) => {
                warn!(error = %e, token = opp.token_mint.as_str(), "tx build failed");
                continue;
            }
        };

        // Tx size check here so we don't consume a Jito send for a doomed tx.
        match bincode::serialize(&tx) {
            Ok(bytes) if bytes.len() > 1232 => {
                warn!(
                    token = opp.token_mint.as_str(),
                    bytes = bytes.len(),
                    "tx too large, dropping"
                );
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, token = opp.token_mint.as_str(), "tx serialize failed");
                continue;
            }
        }

        // ── LiteSVM pre-flight gate ──
        // If simulation is wired in, run the tx locally against the hot
        // Yellowstone-fed account cache. This catches CU overruns, CPI
        // constraint failures, and AMM math divergence between Metis's
        // Rust model and the real on-chain bytecode — all BEFORE we spend
        // a base fee on Jito. A sim pass doesn't guarantee landing (the
        // pool can still move before the slot lands) but a sim failure is
        // a near-certain revert, so dropping them is pure savings.
        if let (Some(cache), Some(sim)) = (sim_cache, simulator) {
            let min_acceptable_out = opp.amount + opp.tip_lamports + base_fee;
            // Resolve ALTs the same way build_arb_transaction did so the
            // simulator can inject state for every account the tx touches.
            let alts = match litesvm_sim::resolve_alts(
                &opp.swap_ixs.address_lookup_table_addresses,
                alt_cache,
                rpc_client,
            ) {
                Ok(a) => a,
                Err(e) => {
                    warn!(error = %e, token = opp.token_mint.as_str(), "sim ALT resolve failed");
                    continue;
                }
            };
            match sim.simulate(&tx, &alts, cache, min_acceptable_out) {
                Ok(outcome) => {
                    debug!(
                        token = opp.token_mint.as_str(),
                        cu = outcome.compute_units,
                        wsol_after = outcome.wsol_after,
                        "sim OK"
                    );
                }
                Err(e) => {
                    debug!(error = %e, token = opp.token_mint.as_str(), "sim rejected, skipping");
                    continue;
                }
            }
        }

        // Fire-and-forget: spawn the send on the runtime so it starts IMMEDIATELY,
        // while this loop keeps draining more opportunities from `futs`. Jito
        // bundles that revert on-chain pay no tip, so redundant shots are free.
        let jito_clone = jito.clone();
        let token_for_log = opp.token_mint.clone();
        let profit_for_log = opp.net_profit;
        tokio::spawn(async move {
            match jito_clone.send_bundle(&tx).await {
                Ok(uuid) => info!(
                    uuid = %uuid,
                    token = token_for_log.as_str(),
                    profit = profit_for_log,
                    "bundle sent to all Jito endpoints"
                ),
                Err(e) => warn!(
                    error = %e,
                    token = token_for_log.as_str(),
                    "execution failed"
                ),
            }
        });
    }

    Ok(())
}

