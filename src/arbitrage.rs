use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use crate::account_cache::AccountCache;
use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::Config;
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::litesvm_sim::{self, SimulatorPool};
use crate::metis::{MetisClient, QuoteResponse, SwapInstructionsResponse};
use crate::metrics::Metrics;
use crate::program_registry::{FORBIDDEN_DEX_LABELS, FORBIDDEN_DEX_PROGRAM_IDS, PMM_PROGRAM_IDS};
use crate::rate_limiter::RateLimiter;
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

fn extract_route_program_ids(quote: &QuoteResponse) -> Vec<String> {
    let arr = match quote.route_plan.as_array() {
        Some(a) => a,
        None => return vec![],
    };
    let mut ids = Vec::new();
    for hop in arr {
        if let Some(swap_info) = hop.get("swapInfo").and_then(|s| s.as_object()) {
            for v in swap_info.values() {
                if let Some(s) = v.as_str() {
                    if s.len() >= 32 && s.len() <= 44 {
                        ids.push(s.to_string());
                    }
                }
            }
        }
    }
    ids
}

fn route_uses_forbidden_dex(quote: &QuoteResponse) -> bool {
    let arr = match quote.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr {
        let swap_info = match hop.get("swapInfo") {
            Some(s) => s,
            None => continue,
        };
        if let Some(label) = swap_info.get("label").and_then(|v| v.as_str()) {
            for banned in FORBIDDEN_DEX_LABELS {
                if label.eq_ignore_ascii_case(banned)
                    || label.to_ascii_lowercase().contains(&banned.to_ascii_lowercase())
                {
                    return true;
                }
            }
        }
        if let Some(obj) = swap_info.as_object() {
            for v in obj.values() {
                if let Some(s) = v.as_str() {
                    if FORBIDDEN_DEX_PROGRAM_IDS.iter().any(|p| *p == s) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn route_uses_pmm(quote: &QuoteResponse) -> bool {
    let ids = extract_route_program_ids(quote);
    ids.iter().any(|id| PMM_PROGRAM_IDS.iter().any(|p| *p == id.as_str()))
}

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
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    cu_limit: u32,
    is_pmm: bool,
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
    user_pubkey: &str,
    cu_limits: &[u32],
    metrics: &Metrics,
) -> Option<Opportunity> {
    metrics.metis_quotes.fetch_add(1, Ordering::Relaxed);

    let quote1 = metis.get_quote(WSOL_MINT, token_mint, amount).await.ok()?;
    let token_amount: u64 = quote1.out_amount.parse().ok().filter(|&v: &u64| v > 0)?;

    let quote2 = metis.get_quote(token_mint, WSOL_MINT, token_amount).await.ok()?;
    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);

    if output_wsol <= amount {
        return None;
    }

    if route_uses_forbidden_dex(&quote1) || route_uses_forbidden_dex(&quote2) {
        debug!(token = token_mint, "skipping route through forbidden DEX");
        return None;
    }

    let is_pmm = route_uses_pmm(&quote1) || route_uses_pmm(&quote2);

    let raw_profit = output_wsol - amount;
    let tip = transaction::calculate_tip(raw_profit, tip_percent, tip_min, tip_max);
    let total_costs = tip + base_fee;

    if raw_profit <= total_costs + min_profit {
        return None;
    }

    let min_acceptable_out = amount + total_costs;

    let merged_quote =
        MetisClient::merge_quotes(&quote1, &quote2, min_acceptable_out).ok()?;

    if merged_quote.instruction_version.as_deref() != Some("V2") {
        debug!(
            token = token_mint,
            got = ?merged_quote.instruction_version,
            "quote did NOT report instructionVersion=V2 -- Metis binary may be outdated"
        );
    }
    let hop_count = merged_quote
        .route_plan
        .as_array()
        .map(|a| a.len())
        .unwrap_or(2);

    let swap_ixs = metis
        .get_swap_instructions(user_pubkey, &merged_quote)
        .await
        .ok()?;

    let cu_limit = lookup_cu_limit(hop_count, cu_limits);

    metrics.metis_profitable.fetch_add(1, Ordering::Relaxed);

    Some(Opportunity {
        token_mint: token_mint.to_string(),
        amount,
        output_wsol,
        tip_lamports: tip,
        net_profit: raw_profit - total_costs,
        swap_ixs,
        hop_count,
        cu_limit,
        is_pmm,
    })
}

/// Scan ALL (amount x token) pairs concurrently.
///
/// AMM routes: simulate → if pass → rate limit → send to Jito
/// PMM routes: BYPASS simulation → rate limit → send directly to Jito
///   (PMMs rely on same-slot oracle freshness that local sim can't provide)
///
/// All per-opportunity work (tx build, serialization, simulation, dispatch)
/// is spawned immediately so the scan loop is never blocked.
pub async fn scan_all_tokens(
    metis: &MetisClient,
    token_mints: &[String],
    config: &Config,
    jito: &Arc<JitoClient>,
    trading_keypair: &Arc<Keypair>,
    rpc_client: &Arc<RpcClient>,
    jito_limiter: &Arc<Mutex<RateLimiter>>,
    blockhash_cache: &BlockhashCache,
    alt_cache: &AltCache,
    sim_cache: Option<&Arc<AccountCache>>,
    sim_pool: Option<&Arc<SimulatorPool>>,
    jito_grpc: Option<&Arc<JitoGrpcClient>>,
    jito_grpc_limiter: Option<&Arc<Mutex<RateLimiter>>>,
    metrics: &Arc<Metrics>,
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
                metrics,
            ));
        }
        amount += step_lamports;
    }

    while let Some(result) = futs.next().await {
        let opp = match result {
            Some(opp) => opp,
            None => continue,
        };

        info!(
            token = opp.token_mint.as_str(),
            input_sol = opp.amount as f64 / LAMPORTS_PER_SOL,
            output_sol = opp.output_wsol as f64 / LAMPORTS_PER_SOL,
            profit_lamports = opp.net_profit,
            tip_lamports = opp.tip_lamports,
            hops = opp.hop_count,
            cu_limit = opp.cu_limit,
            pmm = opp.is_pmm,
            "PROFITABLE -- building tx"
        );

        // Capture everything the spawned task needs. The main loop returns
        // immediately so the next opportunity is processed without waiting
        // for tx build, serialization, simulation, or Jito network I/O.
        let recent_blockhash = blockhash_cache.get();
        let keypair_clone = trading_keypair.clone();
        let rpc_clone = rpc_client.clone();
        let alt_clone = alt_cache.clone();
        let jito_clone = jito.clone();
        let jito_limiter_clone = jito_limiter.clone();
        let jito_grpc_clone = jito_grpc.cloned();
        let jito_grpc_limiter_clone = jito_grpc_limiter.cloned();
        let metrics_clone = metrics.clone();
        let sim_cache_clone = sim_cache.cloned();
        let sim_pool_clone = sim_pool.cloned();

        let token_for_log = opp.token_mint.clone();
        let profit_for_log = opp.net_profit;
        let amount_for_log = opp.amount;
        let expected_out_for_log = opp.output_wsol;
        let tip_lamports = opp.tip_lamports;
        let cu_limit = opp.cu_limit;
        let min_acceptable_out = opp.amount + opp.tip_lamports + base_fee;
        let is_pmm = opp.is_pmm;
        let swap_ixs = opp.swap_ixs;

        tokio::spawn(async move {
            // ── Step 1: build versioned transaction ───────────────────────
            let tx = match transaction::build_arb_transaction(
                &swap_ixs,
                &keypair_clone,
                tip_lamports,
                cu_limit,
                recent_blockhash,
                &alt_clone,
                &*rpc_clone,
            ) {
                Ok(tx) => tx,
                Err(e) => {
                    metrics_clone.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(error = %e, token = token_for_log.as_str(), "tx build failed");
                    return;
                }
            };

            // ── Step 2: size guard (Solana hard limit 1232 bytes) ─────────
            match bincode::serialize(&tx) {
                Ok(bytes) if bytes.len() > 1232 => {
                    metrics_clone.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        token = token_for_log.as_str(),
                        bytes = bytes.len(),
                        "tx too large, dropping"
                    );
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    metrics_clone.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(error = %e, token = token_for_log.as_str(), "tx serialize failed");
                    return;
                }
            }

            // ── Step 3: PMM bypass OR AMM simulation gate ─────────────────
            if is_pmm {
                metrics_clone.pmm_bypass.fetch_add(1, Ordering::Relaxed);
                info!(
                    token = token_for_log.as_str(),
                    "PMM route -- bypassing sim, sending directly to Jito"
                );
            } else if let (Some(cache), Some(pool)) = (&sim_cache_clone, &sim_pool_clone) {
                let alts = match litesvm_sim::resolve_alts(
                    &swap_ixs.address_lookup_table_addresses,
                    &alt_clone,
                    &*rpc_clone,
                ) {
                    Ok(alts) => {
                        for alt in &alts {
                            if cache.get(&alt.key).is_none() {
                                if let Err(e) = cache.get_or_fetch(&alt.key) {
                                    warn!(alt = %alt.key, error = %e, "ALT raw account fetch failed");
                                }
                            }
                        }
                        alts
                    }
                    Err(e) => {
                        metrics_clone.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %e, token = token_for_log.as_str(), "sim ALT resolve failed");
                        return;
                    }
                };

                let sim = pool.acquire();
                metrics_clone.sim_submitted.fetch_add(1, Ordering::Relaxed);
                match sim.simulate(&tx, &alts, cache, min_acceptable_out, &metrics_clone) {
                    Ok(outcome) => {
                        info!(
                            token = token_for_log.as_str(),
                            cu = outcome.compute_units,
                            wsol_after = outcome.wsol_after,
                            "sim PASSED"
                        );
                    }
                    Err(e) => {
                        info!(
                            error = %e,
                            token = token_for_log.as_str(),
                            amount = amount_for_log,
                            expected_out = expected_out_for_log,
                            "sim REJECTED, dropping"
                        );
                        return;
                    }
                }
            }

            // ── Step 4: dispatch — REST first, gRPC fallback ──────────────
            let use_rest = jito_limiter_clone.lock().unwrap().try_acquire();
            let use_grpc = if use_rest {
                false
            } else {
                match (&jito_grpc_clone, &jito_grpc_limiter_clone) {
                    (Some(_), Some(lim)) => lim.lock().unwrap().try_acquire(),
                    _ => false,
                }
            };

            if use_rest {
                match jito_clone.send_bundle(&tx).await {
                    Ok(uuid) => {
                        metrics_clone.jito_sent.fetch_add(1, Ordering::Relaxed);
                        info!(
                            uuid = %uuid,
                            token = token_for_log.as_str(),
                            profit = profit_for_log,
                            pmm = is_pmm,
                            path = "rest",
                            "bundle sent to Jito"
                        );
                    }
                    Err(e) => warn!(
                        error = %e,
                        token = token_for_log.as_str(),
                        path = "rest",
                        "execution failed"
                    ),
                }
            } else if use_grpc {
                let grpc = jito_grpc_clone.as_ref().expect("grpc client set when use_grpc");
                match grpc.send_bundle(&tx).await {
                    Ok(uuid) => {
                        metrics_clone.jito_grpc_sent.fetch_add(1, Ordering::Relaxed);
                        info!(
                            uuid = %uuid,
                            token = token_for_log.as_str(),
                            profit = profit_for_log,
                            pmm = is_pmm,
                            path = "grpc",
                            "bundle sent to Jito"
                        );
                    }
                    Err(e) => warn!(
                        error = %e,
                        token = token_for_log.as_str(),
                        path = "grpc",
                        "execution failed"
                    ),
                }
            } else {
                metrics_clone.jito_rate_limited.fetch_add(1, Ordering::Relaxed);
                debug!(token = token_for_log.as_str(), "jito rate limit hit on both paths");
            }
        });
    }

    Ok(())
}
