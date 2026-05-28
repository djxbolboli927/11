use anyhow::Result;
use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, transaction::VersionedTransaction};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

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
const JITO_TIP_LAMPORTS: u64 = 5_000;
/// Bundles older than this are stale and dropped before sending to Jito.
const BUNDLE_MAX_AGE_MS: u64 = 15;

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
    quote1: QuoteResponse,
    quote2: QuoteResponse,
    hop_count: usize,
    #[allow(dead_code)]
    is_pmm: bool,
}

// ─── Stage 2 output ──────────────────────────────────────────────────────────

pub struct ReadyBundle {
    pub tx: VersionedTransaction,
    pub use_grpc: bool,
    pub built_at: Instant,
}

// ─── Dependencies for Stage-2 calc workers ───────────────────────────────────

pub struct CalcCtx {
    pub metis: Arc<MetisClient>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub trading_keypair: Arc<Keypair>,
    pub rpc_client: Arc<RpcClient>,
    pub alt_cache: AltCache,
    pub jito_limiter: Arc<Mutex<RateLimiter>>,
    pub jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
    pub cu_limits: Vec<u32>,
    pub user_pubkey: String,
    pub sim_cache: Option<Arc<AccountCache>>,
    pub sim_pool: Option<Arc<SimulatorPool>>,
}

// ─── Stage 1: Quote scanner ───────────────────────────────────────────────────

async fn quote_check(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    metrics: &Metrics,
) -> Option<QuotePair> {
    metrics.metis_req_sent.fetch_add(2, Ordering::Relaxed); // quote1 + quote2

    let quote1 = metis.get_quote(WSOL_MINT, token_mint, amount).await.ok()?;
    let token_amount: u64 = quote1.out_amount.parse().ok().filter(|&v: &u64| v > 0)?;

    let quote2 = metis.get_quote(token_mint, WSOL_MINT, token_amount).await.ok()?;
    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);

    if output_wsol <= amount {
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

    Some(QuotePair { token_mint: token_mint.to_string(), amount, output_wsol, quote1, quote2, hop_count, is_pmm })
}

// ─── Stage 2: Calc + tx build ─────────────────────────────────────────────────

async fn calc_and_build(
    pair: QuotePair,
    ctx: Arc<CalcCtx>,
    jito_tx: mpsc::Sender<ReadyBundle>,
    metrics: Arc<Metrics>,
    _permit: OwnedSemaphorePermit, // released on drop → frees calc slot
) {
    // 1. Reserve a Jito rate-limit slot first (cheap; avoids building tx we can't send).
    let use_grpc = if ctx.jito_limiter.lock().unwrap().try_acquire() {
        false
    } else if let Some(gl) = &ctx.jito_grpc_limiter {
        if gl.lock().unwrap().try_acquire() {
            true
        } else {
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
    } else {
        metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
        return;
    };

    // 2. Merge quotes + get swap instructions.
    let merged = match MetisClient::merge_quotes(&pair.quote1, &pair.quote2, pair.amount) {
        Ok(m) => m,
        Err(_) => { metrics.tx_dropped.fetch_add(1, Ordering::Relaxed); return; }
    };

    metrics.metis_req_sent.fetch_add(1, Ordering::Relaxed); // swap_instructions HTTP call
    let swap_ixs = match ctx.metis.get_swap_instructions(&ctx.user_pubkey, &merged).await {
        Ok(s) => s,
        Err(_) => { metrics.tx_dropped.fetch_add(1, Ordering::Relaxed); return; }
    };

    // 3. Build versioned transaction (CPU-bound + possible ALT RPC → spawn_blocking).
    let cu_limit = lookup_cu_limit(pair.hop_count, &ctx.cu_limits);
    let recent_blockhash = ctx.blockhash_cache.get();
    let keypair = ctx.trading_keypair.clone();
    let alt = ctx.alt_cache.clone();
    let rpc = ctx.rpc_client.clone();

    let tx = match tokio::task::spawn_blocking(move || {
        transaction::build_arb_transaction(
            &swap_ixs, &keypair, JITO_TIP_LAMPORTS, cu_limit, recent_blockhash, &alt, &rpc,
        )
    })
    .await
    {
        Ok(Ok(tx)) => tx,
        _ => { metrics.tx_dropped.fetch_add(1, Ordering::Relaxed); return; }
    };

    // 4. Size guard (Solana hard limit: 1232 bytes).
    match bincode::serialize(&tx) {
        Ok(bytes) if bytes.len() > 1232 => { metrics.tx_dropped.fetch_add(1, Ordering::Relaxed); return; }
        Err(_) => { metrics.tx_dropped.fetch_add(1, Ordering::Relaxed); return; }
        Ok(_) => {}
    }

    // 5. Send to Stage-3 Jito worker (non-blocking; drop if channel full).
    let bundle = ReadyBundle { tx, use_grpc, built_at: Instant::now() };
    if jito_tx.try_send(bundle).is_ok() {
        metrics.calc_done.fetch_add(1, Ordering::Relaxed);
    } else {
        metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
    }
}

// ─── Stage 3: Persistent Jito dispatcher ────────────────────────────────────

/// Receives ready bundles from Stage 2 and dispatches them to Jito.
/// Stale bundles (older than BUNDLE_MAX_AGE_MS) are dropped.
/// Each Jito HTTP send is spawned independently to avoid blocking on network I/O.
pub async fn jito_dispatch_task(
    mut rx: mpsc::Receiver<ReadyBundle>,
    jito: Arc<JitoClient>,
    jito_grpc: Option<Arc<JitoGrpcClient>>,
    metrics: Arc<Metrics>,
) {
    while let Some(bundle) = rx.recv().await {
        if bundle.built_at.elapsed().as_millis() as u64 > BUNDLE_MAX_AGE_MS {
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let jito_c = jito.clone();
        let grpc_c = jito_grpc.clone();
        let met = metrics.clone();

        tokio::spawn(async move {
            if bundle.use_grpc {
                if let Some(grpc) = grpc_c {
                    match grpc.send_bundle(&bundle.tx).await {
                        Ok(_) => { met.jito_sent.fetch_add(1, Ordering::Relaxed); }
                        Err(_) => { met.tx_dropped.fetch_add(1, Ordering::Relaxed); }
                    }
                }
            } else {
                match jito_c.send_bundle(&bundle.tx).await {
                    Ok(_) => { met.jito_sent.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { met.tx_dropped.fetch_add(1, Ordering::Relaxed); }
                }
            }
        });
    }
}

// ─── Main scan entry ─────────────────────────────────────────────────────────

/// One scan cycle over all (token × amount) pairs.
///
/// Stage 1 (here): buffer_unordered quote scanner
/// Stage 2 (spawned tasks, max CALC_WORKERS concurrent): swap_instructions + tx build
/// Stage 3 (jito_dispatch_task, persistent): Jito send with age-check
pub async fn scan_all_tokens(
    token_mints: &[String],
    config: &Config,
    ctx: &Arc<CalcCtx>,
    jito_tx: &mpsc::Sender<ReadyBundle>,
    calc_sem: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let max_concurrent = config.performance.max_concurrent_quotes;

    // Pairs interleaved across tokens: (0.001,A),(0.001,B),...,(0.001,Q),(0.0011,A),...
    // This ensures all tokens get equal opportunity regardless of which complete first.
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

    // Stage 1: stream quotes with bounded concurrency.
    let mut opps = stream::iter(all_pairs)
        .map(move |(amt, tok)| async move {
            quote_check(metis_ref, &tok, amt, met_ref).await
        })
        .buffer_unordered(max_concurrent);

    while let Some(result) = opps.next().await {
        let pair = match result {
            Some(p) => p,
            None => continue,
        };

        // Stage 2: try calc slot immediately (no queue — drop if all workers busy).
        let permit = match calc_sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let ctx_c = ctx.clone();
        let jito_tx_c = jito_tx.clone();
        let met_c = metrics.clone();

        tokio::spawn(async move {
            calc_and_build(pair, ctx_c, jito_tx_c, met_c, permit).await;
        });
    }

    Ok(())
}
