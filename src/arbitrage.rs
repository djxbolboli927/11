use anyhow::Result;
use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::account_cache::AccountCache;
use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::Config;
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::litesvm_sim::SimulatorPool;
use crate::metis::{MetisClient, QuoteResponse, SwapInstructionsResponse};
use crate::metrics::Metrics;
use crate::program_registry::{FORBIDDEN_DEX_LABELS, FORBIDDEN_DEX_PROGRAM_IDS, PMM_PROGRAM_IDS};
use crate::rate_limiter::RateLimiter;
use crate::token_metrics::TokenMetrics;
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const JITO_TIP_LAMPORTS: u64 = 1_600;
const NETWORK_FEE_LAMPORTS: u64 = 5_000;

// ─── Route helpers ───────────────────────────────────────────────────────────

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
    let arr = match quote.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr {
        if let Some(swap_info) = hop.get("swapInfo").and_then(|s| s.as_object()) {
            for v in swap_info.values() {
                if let Some(s) = v.as_str() {
                    if PMM_PROGRAM_IDS.iter().any(|p| *p == s) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn lookup_cu_limit(hop_count: usize, cu_limits: &[u32]) -> u32 {
    if cu_limits.is_empty() {
        return 200_000;
    }
    let index = hop_count.saturating_sub(2);
    cu_limits[index.min(cu_limits.len() - 1)]
}

// ─── Stage 1 output ──────────────────────────────────────────────────────────

struct QuotePair {
    token_mint: String,
    amount: u64,
    output_wsol: u64,
    net_profit: i64,
    quote1: QuoteResponse,
    quote2: QuoteResponse,
    hop_count: usize,
    #[allow(dead_code)]
    is_pmm: bool,
}

// ─── LIFO queue item ──────────────────────────────────────────────────────────

struct ReadyInstruction {
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    arrived_at: Instant,
}

// ─── Shared context ───────────────────────────────────────────────────────────

pub struct CalcCtx {
    pub metis: Arc<MetisClient>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub trading_keypair: Arc<Keypair>,
    pub rpc_client: Arc<RpcClient>,
    pub alt_cache: AltCache,
    pub jito: Arc<JitoClient>,
    pub jito_grpc: Option<Arc<JitoGrpcClient>>,
    pub jito_limiter: Arc<Mutex<RateLimiter>>,
    pub jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
    pub cu_limits: Vec<u32>,
    pub user_pubkey: String,
    pub sim_cache: Option<Arc<AccountCache>>,
    pub sim_pool: Option<Arc<SimulatorPool>>,
}

// ─── Pipeline handle ──────────────────────────────────────────────────────────

/// Shared LIFO queue + semaphore. Pass to scan_all_tokens.
pub struct Pipeline {
    lifo: Arc<Mutex<Vec<ReadyInstruction>>>,
    lifo_sem: Arc<tokio::sync::Semaphore>,
}

// ─── Stage 1: Quote scanner ───────────────────────────────────────────────────

async fn quote_check(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    min_profit_lamports: u64,
    metrics: &Metrics,
    token_metrics: &TokenMetrics,
) -> Option<QuotePair> {
    // 2 HTTP requests per scan (quote1 + quote2).
    metrics.metis_req_sent.fetch_add(2, Ordering::Relaxed);
    let ts = token_metrics.get(token_mint);
    if let Some(ts) = ts {
        ts.q_sent.fetch_add(2, Ordering::Relaxed);
    }

    // quote1: WSOL → token
    let quote1 = match metis.get_quote(WSOL_MINT, token_mint, amount).await {
        Ok(q) => q,
        Err(_) => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let token_amount: u64 = match quote1.out_amount.parse::<u64>().ok().filter(|&v| v > 0) {
        Some(v) => v,
        None => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    // quote2: token → WSOL
    let quote2 = match metis.get_quote(token_mint, WSOL_MINT, token_amount).await {
        Ok(q) => q,
        Err(_) => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);
    metrics.metis_resp_total.fetch_add(1, Ordering::Relaxed);
    if let Some(ts) = ts {
        ts.route_ok.fetch_add(1, Ordering::Relaxed);
    }

    let stage1_threshold = amount.saturating_add(min_profit_lamports);
    if output_wsol <= stage1_threshold {
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    if route_uses_forbidden_dex(&quote1) || route_uses_forbidden_dex(&quote2) {
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    // Profitable.
    if let Some(ts) = ts {
        ts.profitable.fetch_add(1, Ordering::Relaxed);
    }

    let is_pmm = route_uses_pmm(&quote1) || route_uses_pmm(&quote2);
    let hop_count = {
        let n1 = quote1.route_plan.as_array().map(|a| a.len()).unwrap_or(1);
        let n2 = quote2.route_plan.as_array().map(|a| a.len()).unwrap_or(1);
        n1 + n2
    };

    metrics.metis_resp_ok.fetch_add(1, Ordering::Relaxed);

    let on_chain_floor = amount
        .saturating_add(JITO_TIP_LAMPORTS)
        .saturating_add(NETWORK_FEE_LAMPORTS);
    let net_profit = output_wsol as i64 - on_chain_floor as i64;
    Some(QuotePair {
        token_mint: token_mint.to_string(),
        amount,
        output_wsol,
        net_profit,
        quote1,
        quote2,
        hop_count,
        is_pmm,
    })
}

// ─── Worker pool ──────────────────────────────────────────────────────────────

/// Spawn `worker_count` persistent pipeline workers and return the Pipeline handle.
///
/// Workers wait on the semaphore for a ReadyInstruction to arrive in the LIFO
/// queue. When one arrives they pop the **newest** item (LIFO order), discard it
/// if older than `queue_max_age_ms`, then build a versioned transaction and
/// submit it to Jito.
///
/// Workers build the transaction first, then claim a Jito rate-limit slot right
/// before `send_bundle`. This keeps slow/failed swap-instructions or tx builds
/// from burning one of the 10 per-second Jito slots.
pub fn spawn_workers(
    ctx: Arc<CalcCtx>,
    metrics: Arc<Metrics>,
    worker_count: usize,
    queue_max_age_ms: u64,
) -> Pipeline {
    let lifo: Arc<Mutex<Vec<ReadyInstruction>>> = Arc::new(Mutex::new(Vec::new()));
    let lifo_sem = Arc::new(tokio::sync::Semaphore::new(0));

    for _ in 0..worker_count {
        let lifo_c = lifo.clone();
        let sem_c = lifo_sem.clone();
        let ctx_c = ctx.clone();
        let met_c = metrics.clone();
        tokio::spawn(async move {
            loop {
                // Block until at least one item is available.
                sem_c.acquire().await.unwrap().forget();

                let item = lifo_c.lock().unwrap().pop();
                let item = match item {
                    Some(i) => i,
                    None => continue,
                };

                // Drop stale items immediately.
                if item.arrived_at.elapsed().as_millis() as u64 > queue_max_age_ms {
                    met_c.dropped_stale.fetch_add(1, Ordering::Relaxed);
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Build versioned transaction.
                let cu_limit = lookup_cu_limit(item.hop_count, &ctx_c.cu_limits);
                let recent_blockhash = ctx_c.blockhash_cache.get();
                let keypair = ctx_c.trading_keypair.clone();
                let alt = ctx_c.alt_cache.clone();
                let rpc = ctx_c.rpc_client.clone();
                let swap_ixs = item.swap_ixs;

                let tx = match tokio::task::spawn_blocking(move || {
                    transaction::build_arb_transaction(
                        &swap_ixs,
                        &keypair,
                        JITO_TIP_LAMPORTS,
                        cu_limit,
                        recent_blockhash,
                        &alt,
                        &rpc,
                    )
                })
                .await
                {
                    Ok(Ok(tx)) => tx,
                    _ => {
                        met_c.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                // Size guard (Solana hard limit: 1232 bytes).
                match bincode::serialize(&tx) {
                    Ok(bytes) if bytes.len() > 1232 => {
                        met_c.tx_too_large.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(_) => {
                        met_c.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Ok(_) => {}
                }

                met_c.calc_done.fetch_add(1, Ordering::Relaxed);

                // Claim a Jito slot only after the tx is fully built and sized.
                let use_grpc = if ctx_c.jito_limiter.lock().unwrap().try_acquire() {
                    false
                } else if let Some(gl) = &ctx_c.jito_grpc_limiter {
                    if gl.lock().unwrap().try_acquire() {
                        true
                    } else {
                        met_c.dropped_rate_limit.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                } else {
                    met_c.dropped_rate_limit.fetch_add(1, Ordering::Relaxed);
                    continue;
                };

                // Send bundle to Jito via the slot we just claimed.
                let result = if use_grpc {
                    match &ctx_c.jito_grpc {
                        Some(grpc) => grpc.send_bundle(&tx).await,
                        None => ctx_c.jito.send_bundle(&tx).await,
                    }
                } else {
                    ctx_c.jito.send_bundle(&tx).await
                };

                match result {
                    Ok(_) => {
                        met_c.jito_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        met_c.jito_send_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    Pipeline { lifo, lifo_sem }
}

// ─── Main scan entry ─────────────────────────────────────────────────────────

/// One scan cycle over all (token × amount) pairs.
///
/// Stage 1: buffer_unordered quote scanner (`performance.max_concurrent_quotes`).
/// For each profitable pair:
///   - Merge quotes and fire a detached tokio task for swap_instructions.
///   - The task pushes a ReadyInstruction into the Pipeline's LIFO queue.
/// Stage 2: pre-spawned workers drain the LIFO queue newest-first, build the tx,
/// then claim a Jito rate-limit slot only immediately before send_bundle.
pub async fn scan_all_tokens(
    token_mints: &[String],
    config: &Config,
    ctx: &Arc<CalcCtx>,
    pipeline: &Pipeline,
    metrics: &Arc<Metrics>,
    token_metrics: &Arc<TokenMetrics>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let min_profit_lamports = config.trading.min_profit_lamports;

    let all_pairs: Vec<(u64, String)> = {
        let mut pairs = Vec::new();
        let mut amount = min_lamports;
        while amount <= max_lamports {
            for token_mint in token_mints {
                pairs.push((amount, token_mint.clone()));
            }
            amount += step_lamports;
        }
        pairs
    };
    let max_concurrent = config
        .performance
        .max_concurrent_quotes
        .max(1)
        .min(all_pairs.len().max(1));

    let metis_ref: &MetisClient = &ctx.metis;
    let met_ref: &Metrics = metrics;
    let tok_met_ref: &TokenMetrics = token_metrics;

    let mut opps = stream::iter(all_pairs)
        .map(move |(amt, tok)| async move {
            quote_check(metis_ref, &tok, amt, min_profit_lamports, met_ref, tok_met_ref).await
        })
        .buffer_unordered(max_concurrent);

    while let Some(result) = opps.next().await {
        let pair = match result {
            Some(p) => p,
            None => continue,
        };

        let on_chain_floor = pair.amount + JITO_TIP_LAMPORTS + NETWORK_FEE_LAMPORTS;
        tracing::debug!(
            token = %pair.token_mint,
            amount = pair.amount,
            output = pair.output_wsol,
            quoted_edge = pair.output_wsol as i64 - pair.amount as i64,
            floor_edge = pair.net_profit,
            "send_candidate"
        );

        let merged = match MetisClient::merge_quotes(&pair.quote1, &pair.quote2, on_chain_floor) {
            Ok(m) => m,
            Err(_) => {
                metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        // Fire swap_instructions asynchronously; push result into the LIFO queue.
        let ctx_c = ctx.clone();
        let met_c = metrics.clone();
        let lifo_c = pipeline.lifo.clone();
        let sem_c = pipeline.lifo_sem.clone();
        let hop_count = pair.hop_count;

        metrics.metis_req_sent.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let swap_ixs =
                match ctx_c.metis.get_swap_instructions(&ctx_c.user_pubkey, &merged).await {
                    Ok(s) => s,
                    Err(_) => {
                        met_c.swap_ix_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

            let item = ReadyInstruction {
                swap_ixs,
                hop_count,
                arrived_at: Instant::now(),
            };
            lifo_c.lock().unwrap().push(item);
            sem_c.add_permits(1);
        });
    }

    Ok(())
}
