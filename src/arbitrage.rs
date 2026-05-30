use anyhow::Result;
use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

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
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const JITO_TIP_LAMPORTS: u64 = 1_600;
const NETWORK_FEE_LAMPORTS: u64 = 5_000;
/// Total time budget from swap_instructions fire to Jito send (milliseconds).
const CALC_MAX_AGE_MS: u64 = 30;
/// Maximum time a ReadyInstruction may wait in the LIFO queue (milliseconds).
const INSTRUCTION_QUEUE_MAX_AGE_MS: u64 = 15;
/// Pause this many milliseconds after every N swap_instructions spawns to
/// give Metis time to drain its queue between bursts.
const SWAP_IX_BURST: usize = 6;
const SWAP_IX_PAUSE_MS: u64 = 2;

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
    net_profit: i64, // output_wsol - (amount + on_chain_floor); negative = unprofitable after fees
    quote1: QuoteResponse,
    quote2: QuoteResponse,
    hop_count: usize,
    #[allow(dead_code)]
    is_pmm: bool,
}

// ─── Stage 2 output (LIFO queue element) ─────────────────────────────────────

pub struct ReadyInstruction {
    pub swap_ixs: SwapInstructionsResponse,
    pub amount: u64,
    pub hop_count: usize,
    pub use_grpc: bool,
    /// When swap_instructions HTTP request was fired (30 ms total budget).
    pub fired_at: Instant,
    /// When the response arrived in the LIFO queue (15 ms pickup budget).
    pub arrived_at: Instant,
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
    min_profit_lamports: u64,
    metrics: &Metrics,
) -> Option<QuotePair> {
    metrics.metis_req_sent.fetch_add(2, Ordering::Relaxed); // quote1 + quote2

    let quote1 = metis.get_quote(WSOL_MINT, token_mint, amount).await.ok()?;
    let token_amount: u64 = quote1.out_amount.parse().ok().filter(|&v: &u64| v > 0)?;

    let quote2 = metis.get_quote(token_mint, WSOL_MINT, token_amount).await.ok()?;
    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);

    // Both quotes returned — count Metis throughput regardless of profitability.
    metrics.metis_resp_total.fetch_add(1, Ordering::Relaxed);

    // Stage-1 pre-filter uses min_profit_lamports from config (gross profit check).
    // This controls which quotes are counted as "profitable" and proceed to
    // swap_instructions. It is independent of the on-chain floor used in Stage 3.
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

    // net_profit relative to the on-chain floor (tip + network fee), not the config threshold.
    let on_chain_floor = amount
        .saturating_add(JITO_TIP_LAMPORTS)
        .saturating_add(NETWORK_FEE_LAMPORTS);
    let net_profit = output_wsol as i64 - on_chain_floor as i64;
    Some(QuotePair { token_mint: token_mint.to_string(), amount, output_wsol, net_profit, quote1, quote2, hop_count, is_pmm })
}

// ─── Stage 2: Merge quotes + fire swap_instructions (fire-and-forget) ────────

async fn calc_and_build(
    pair: QuotePair,
    ctx: Arc<CalcCtx>,
    lifo: Arc<Mutex<Vec<ReadyInstruction>>>,
    notify: Arc<Notify>,
    metrics: Arc<Metrics>,
    _permit: OwnedSemaphorePermit, // released on return → frees calc slot immediately
) {
    // 1. Reserve a Jito rate-limit slot first (cheap; avoids firing swap_ixs we can't send).
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

    // 2. Merge quotes.
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

    // 3. Spawn the swap_instructions HTTP call and return immediately.
    //    The spawned task pushes the result into the LIFO queue; jito_send_task picks it up.
    let fired_at = Instant::now();
    let amount = pair.amount;
    let hop_count = pair.hop_count;
    let metis = ctx.metis.clone();
    let user_pubkey = ctx.user_pubkey.clone();
    let met_c = metrics.clone();

    metrics.metis_req_sent.fetch_add(1, Ordering::Relaxed); // swap_instructions HTTP call

    tokio::spawn(async move {
        let swap_ixs = match metis.get_swap_instructions(&user_pubkey, &merged).await {
            Ok(s) => s,
            Err(_) => {
                met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        // Drop response if total budget already exceeded.
        if fired_at.elapsed().as_millis() as u64 > CALC_MAX_AGE_MS {
            met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let arrived_at = Instant::now();
        lifo.lock().unwrap().push(ReadyInstruction {
            swap_ixs,
            amount,
            hop_count,
            use_grpc,
            fired_at,
            arrived_at,
        });
        notify.notify_one();
    });
    // _permit drops here: Stage-2 slot is freed while the HTTP call is still in flight.
}

// ─── Stage 3: Persistent Jito sender (LIFO, newest-first) ────────────────────

/// Waits for swap instruction results on the LIFO queue (newest first), builds
/// versioned transactions, and dispatches them to Jito.
/// Items older than INSTRUCTION_QUEUE_MAX_AGE_MS in queue, or CALC_MAX_AGE_MS
/// from fire, are dropped without sending.
/// Each Jito HTTP/gRPC send is spawned independently to avoid blocking.
pub async fn jito_send_task(
    lifo: Arc<Mutex<Vec<ReadyInstruction>>>,
    notify: Arc<Notify>,
    ctx: Arc<CalcCtx>,
    jito: Arc<JitoClient>,
    jito_grpc: Option<Arc<JitoGrpcClient>>,
    metrics: Arc<Metrics>,
) {
    loop {
        notify.notified().await;
        loop {
            // Pop newest item (LIFO: push to end, pop from end).
            let item = { lifo.lock().unwrap().pop() };
            let item = match item {
                None => break,
                Some(i) => i,
            };

            // Drop stale items.
            if item.arrived_at.elapsed().as_millis() as u64 > INSTRUCTION_QUEUE_MAX_AGE_MS
                || item.fired_at.elapsed().as_millis() as u64 > CALC_MAX_AGE_MS
            {
                metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // Build versioned transaction (CPU-bound + possible ALT RPC → spawn_blocking).
            let cu_limit = lookup_cu_limit(item.hop_count, &ctx.cu_limits);
            let recent_blockhash = ctx.blockhash_cache.get();
            let keypair = ctx.trading_keypair.clone();
            let alt = ctx.alt_cache.clone();
            let rpc = ctx.rpc_client.clone();
            let swap_ixs = item.swap_ixs;

            let tx = match tokio::task::spawn_blocking(move || {
                transaction::build_arb_transaction(
                    &swap_ixs, &keypair, JITO_TIP_LAMPORTS, cu_limit, recent_blockhash, &alt, &rpc,
                )
            })
            .await
            {
                Ok(Ok(tx)) => tx,
                _ => {
                    metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Size guard (Solana hard limit: 1232 bytes).
            match bincode::serialize(&tx) {
                Ok(bytes) if bytes.len() > 1232 => {
                    metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(_) => {
                    metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(_) => {}
            }

            // Fire Jito send independently (non-blocking).
            let jito_c = jito.clone();
            let grpc_c = jito_grpc.clone();
            let met = metrics.clone();
            let use_grpc = item.use_grpc;

            tokio::spawn(async move {
                if use_grpc {
                    if let Some(grpc) = grpc_c {
                        match grpc.send_bundle(&tx).await {
                            Ok(_) => {
                                met.jito_sent.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                met.tx_dropped.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                } else {
                    match jito_c.send_bundle(&tx).await {
                        Ok(_) => {
                            met.jito_sent.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            met.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });

            metrics.calc_done.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ─── Main scan entry ─────────────────────────────────────────────────────────

/// One scan cycle over all (token × amount) pairs.
///
/// Stage 1 (here): buffer_unordered quote scanner
/// Stage 2 (spawned tasks, max calc_sem concurrent): merge quotes + fire swap_instructions
/// Stage 3 (jito_send_task, persistent): LIFO drain → tx build → Jito send
pub async fn scan_all_tokens(
    token_mints: &[String],
    config: &Config,
    ctx: &Arc<CalcCtx>,
    lifo: &Arc<Mutex<Vec<ReadyInstruction>>>,
    notify: &Arc<Notify>,
    calc_sem: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let max_concurrent = config.performance.max_concurrent_quotes;
    let min_profit_lamports = config.trading.min_profit_lamports;

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
            quote_check(metis_ref, &tok, amt, min_profit_lamports, met_ref).await
        })
        .buffer_unordered(max_concurrent);

    let mut spawn_count: usize = 0;
    while let Some(result) = opps.next().await {
        let pair = match result {
            Some(p) => p,
            None => continue,
        };

        // Stage 2: try calc slot immediately (no queue — drop if all workers busy).
        let permit = match calc_sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                metrics.dropped_busy.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        spawn_count += 1;
        if spawn_count % SWAP_IX_BURST == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(SWAP_IX_PAUSE_MS)).await;
        }

        let ctx_c = ctx.clone();
        let lifo_c = lifo.clone();
        let notify_c = notify.clone();
        let met_c = metrics.clone();

        tokio::spawn(async move {
            calc_and_build(pair, ctx_c, lifo_c, notify_c, met_c, permit).await;
        });
    }

    Ok(())
}
