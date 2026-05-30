use anyhow::Result;
use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::account_cache::AccountCache;
use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::Config;
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::litesvm_sim::SimulatorPool;
use crate::metis::{MetisClient, QuoteResponse};
use crate::metrics::Metrics;
use crate::program_registry::{FORBIDDEN_DEX_LABELS, FORBIDDEN_DEX_PROGRAM_IDS, PMM_PROGRAM_IDS};
use crate::rate_limiter::RateLimiter;
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

// ─── Worker sender handle ─────────────────────────────────────────────────────

/// Opaque handle to the pre-spawned worker pool. Pass to scan_all_tokens.
pub struct WorkSender(tokio::sync::mpsc::Sender<QuotePair>);

// ─── Stage 1: Quote scanner ───────────────────────────────────────────────────

async fn quote_check(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    min_profit_lamports: u64,
    metrics: &Metrics,
) -> Option<QuotePair> {
    metrics.metis_req_sent.fetch_add(2, Ordering::Relaxed); // quote1 + quote2

    let quote1 = metis.get_quote(WSOL_MINT, token_mint, amount).await.ok()?;
    let token_amount: u64 = quote1.out_amount.parse().ok().filter(|&v: &u64| v > 0)?;

    let quote2 = metis.get_quote(token_mint, WSOL_MINT, token_amount).await.ok()?;
    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);

    metrics.metis_resp_total.fetch_add(1, Ordering::Relaxed);

    let stage1_threshold = amount.saturating_add(min_profit_lamports);
    if output_wsol <= stage1_threshold {
        return None;
    }

    if route_uses_forbidden_dex(&quote1) || route_uses_forbidden_dex(&quote2) {
        return None;
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

// ─── Per-opportunity pipeline ─────────────────────────────────────────────────

async fn process_and_send(pair: QuotePair, ctx: Arc<CalcCtx>, metrics: Arc<Metrics>) {
    // 1. Claim a Jito rate-limit slot first.
    let use_grpc = if ctx.jito_limiter.lock().unwrap().try_acquire() {
        false
    } else if let Some(gl) = &ctx.jito_grpc_limiter {
        if gl.lock().unwrap().try_acquire() {
            true
        } else {
            metrics.dropped_rate_limit.fetch_add(1, Ordering::Relaxed);
            return;
        }
    } else {
        metrics.dropped_rate_limit.fetch_add(1, Ordering::Relaxed);
        return;
    };

    // 2. Merge quotes: embed on-chain minimum output.
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
            return;
        }
    };

    // 3. Get swap instructions (only called after a rate-limit slot is secured).
    metrics.metis_req_sent.fetch_add(1, Ordering::Relaxed);
    let swap_ixs = match ctx.metis.get_swap_instructions(&ctx.user_pubkey, &merged).await {
        Ok(s) => s,
        Err(_) => {
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    // 4. Build versioned transaction.
    let cu_limit = lookup_cu_limit(pair.hop_count, &ctx.cu_limits);
    let recent_blockhash = ctx.blockhash_cache.get();
    let keypair = ctx.trading_keypair.clone();
    let alt = ctx.alt_cache.clone();
    let rpc = ctx.rpc_client.clone();

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
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    // 5. Size guard (Solana hard limit: 1232 bytes).
    match bincode::serialize(&tx) {
        Ok(bytes) if bytes.len() > 1232 => {
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Err(_) => {
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Ok(_) => {}
    }

    metrics.calc_done.fetch_add(1, Ordering::Relaxed);

    // 6. Send bundle to Jito.
    let result = if use_grpc {
        match &ctx.jito_grpc {
            Some(grpc) => grpc.send_bundle(&tx).await,
            None => ctx.jito.send_bundle(&tx).await,
        }
    } else {
        ctx.jito.send_bundle(&tx).await
    };

    match result {
        Ok(_) => {
            metrics.jito_sent.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ─── Worker pool ──────────────────────────────────────────────────────────────

/// Spawn `worker_count` persistent pipeline workers. Call once at startup.
///
/// Each worker runs a permanent loop: receive a profitable pair from the shared
/// channel, execute the full pipeline (rate-limit → merge → swap_instructions →
/// build tx → Jito send), then immediately wait for the next pair.
///
/// Because workers are already alive and blocked on `recv()`, there is zero
/// spawn latency when a burst arrives. Items are delivered in FIFO order via
/// the channel. If all workers are busy and the channel buffer is full,
/// `scan_all_tokens` drops the item (counted as `dropped_busy`).
pub fn spawn_workers(
    ctx: Arc<CalcCtx>,
    metrics: Arc<Metrics>,
    worker_count: usize,
) -> WorkSender {
    let capacity = worker_count.max(1);
    let (tx, rx) = tokio::sync::mpsc::channel::<QuotePair>(capacity);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    for _ in 0..worker_count {
        let rx_c = rx.clone();
        let ctx_c = ctx.clone();
        let met_c = metrics.clone();
        tokio::spawn(async move {
            loop {
                // Only one worker holds the receiver lock at a time; this
                // ensures FIFO ordering and prevents multiple workers from
                // racing to dequeue the same item.
                let pair = {
                    let mut locked = rx_c.lock().await;
                    match locked.recv().await {
                        Some(p) => p,
                        None => return, // sender dropped = program exiting
                    }
                };
                // Lock is released here; process_and_send runs without
                // holding it, so the next idle worker can dequeue immediately.
                process_and_send(pair, ctx_c.clone(), met_c.clone()).await;
            }
        });
    }

    WorkSender(tx)
}

// ─── Main scan entry ─────────────────────────────────────────────────────────

/// One scan cycle over all (token × amount) pairs.
///
/// Stage 1: buffer_unordered quote scanner (token_count/2 concurrent)
/// Stage 2: profitable pairs are dispatched to pre-spawned workers via channel
pub async fn scan_all_tokens(
    token_mints: &[String],
    config: &Config,
    ctx: &Arc<CalcCtx>,
    work_tx: &WorkSender,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let min_profit_lamports = config.trading.min_profit_lamports;

    // Half the token catalog in flight at once.
    let max_concurrent = (token_mints.len() / 2).max(1);

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

    let metis_ref: &MetisClient = &ctx.metis;
    let met_ref: &Metrics = metrics;

    let mut opps = stream::iter(all_pairs)
        .map(move |(amt, tok)| async move {
            quote_check(metis_ref, &tok, amt, min_profit_lamports, met_ref).await
        })
        .buffer_unordered(max_concurrent);

    while let Some(result) = opps.next().await {
        let pair = match result {
            Some(p) => p,
            None => continue,
        };
        // try_send: non-blocking. Drops if all workers busy + channel full.
        if work_tx.0.try_send(pair).is_err() {
            metrics.dropped_busy.fetch_add(1, Ordering::Relaxed);
        }
    }

    Ok(())
}
