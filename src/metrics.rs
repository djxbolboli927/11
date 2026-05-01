use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const WINDOW_SECS: u64 = 120;

/// Pipeline throughput counters. All fields are atomics so arbitrage tasks
/// and simulator workers can increment them without locking.
///
/// A background task (spawned via `spawn_reporter`) swaps every counter to
/// zero every 120 seconds and logs the window totals so the operator can see
/// exactly which stage of the pipeline is the bottleneck.
pub struct Metrics {
    /// 1. (amount, token) pairs screened — i.e. total Metis quote round-trips
    pub metis_quotes: AtomicU64,
    /// 2. Profitable opportunities identified (gross profit > tip + base_fee)
    pub metis_profitable: AtomicU64,
    /// 2a. Sim-passed opportunities dropped by the Jito per-second rate
    ///     limiter. This counter is bumped AFTER simulation passes, right
    ///     before `send_bundle`. A non-zero value means we found and
    ///     validated more profitable arbs than `max_bundles_per_second`
    ///     allows — consider raising the config value.
    pub jito_rate_limited: AtomicU64,
    /// 2b. Tx build / serialize / size-check failed (too large, >1232 bytes, or
    ///     ALT resolve error). Also contributes to the profitable -> submitted
    ///     gap, but usually tiny.
    pub tx_build_failed: AtomicU64,
    /// 3. Opportunities submitted to the simulator (sim enabled path only)
    pub sim_submitted: AtomicU64,
    /// 4. Simulations actually executed inside LiteSVM
    pub sim_executed: AtomicU64,
    /// 5. Sim rejected: WSOL output < min_acceptable (slippage / unprofitable)
    pub sim_slippage_rejected: AtomicU64,
    /// 6. Sim rejected: transaction reverted inside LiteSVM (program error)
    pub sim_revert_rejected: AtomicU64,
    /// 7. Sim passed — forwarded to Jito
    pub sim_passed: AtomicU64,
    /// 7a. PMM routes that bypassed simulation entirely
    pub pmm_bypass: AtomicU64,
    /// 8. Bundles successfully dispatched via the REST (UUID) Jito path.
    pub jito_sent: AtomicU64,
    /// 8b. Bundles successfully dispatched via the SearcherService gRPC path.
    ///     `jito_sent + jito_grpc_sent` is the real submission throughput.
    pub jito_grpc_sent: AtomicU64,

    // ── Latency tracking ────────────────────────────────────────────────────
    /// Sum of total Metis interaction time per opportunity (µs).
    /// Covers: get_quote×2 + get_swap_instructions.
    pub metis_latency_sum_us: AtomicU64,
    pub metis_latency_count: AtomicU64,
    /// Sum of time from Metis response to the moment the bundle is dispatched
    /// to the network (tx build + sim + queue wait), in µs.
    pub dispatch_latency_sum_us: AtomicU64,
    pub dispatch_latency_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_quotes: AtomicU64::new(0),
            metis_profitable: AtomicU64::new(0),
            jito_rate_limited: AtomicU64::new(0),
            tx_build_failed: AtomicU64::new(0),
            sim_submitted: AtomicU64::new(0),
            sim_executed: AtomicU64::new(0),
            sim_slippage_rejected: AtomicU64::new(0),
            sim_revert_rejected: AtomicU64::new(0),
            sim_passed: AtomicU64::new(0),
            pmm_bypass: AtomicU64::new(0),
            jito_sent: AtomicU64::new(0),
            jito_grpc_sent: AtomicU64::new(0),
            metis_latency_sum_us: AtomicU64::new(0),
            metis_latency_count: AtomicU64::new(0),
            dispatch_latency_sum_us: AtomicU64::new(0),
            dispatch_latency_count: AtomicU64::new(0),
        })
    }

    /// Spawn a background task that logs all counters every 120 s and resets
    /// them so each log line represents exactly one rolling window.
    pub fn spawn_reporter(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await; // discard the immediate first tick
            loop {
                interval.tick().await;
                let quotes     = m.metis_quotes.swap(0, Ordering::Relaxed);
                let profit     = m.metis_profitable.swap(0, Ordering::Relaxed);
                let ratelim    = m.jito_rate_limited.swap(0, Ordering::Relaxed);
                let build_fail = m.tx_build_failed.swap(0, Ordering::Relaxed);
                let submitted  = m.sim_submitted.swap(0, Ordering::Relaxed);
                let executed   = m.sim_executed.swap(0, Ordering::Relaxed);
                let slippage   = m.sim_slippage_rejected.swap(0, Ordering::Relaxed);
                let revert     = m.sim_revert_rejected.swap(0, Ordering::Relaxed);
                let passed     = m.sim_passed.swap(0, Ordering::Relaxed);
                let pmm_byp    = m.pmm_bypass.swap(0, Ordering::Relaxed);
                let sent       = m.jito_sent.swap(0, Ordering::Relaxed);
                let grpc_sent  = m.jito_grpc_sent.swap(0, Ordering::Relaxed);

                let metis_lat_sum  = m.metis_latency_sum_us.swap(0, Ordering::Relaxed);
                let metis_lat_cnt  = m.metis_latency_count.swap(0, Ordering::Relaxed);
                let disp_lat_sum   = m.dispatch_latency_sum_us.swap(0, Ordering::Relaxed);
                let disp_lat_cnt   = m.dispatch_latency_count.swap(0, Ordering::Relaxed);

                let avg_metis_us    = if metis_lat_cnt > 0 { metis_lat_sum / metis_lat_cnt } else { 0 };
                let avg_dispatch_us = if disp_lat_cnt  > 0 { disp_lat_sum  / disp_lat_cnt  } else { 0 };

                let coverage_pct = if profit > 0 {
                    submitted * 100 / profit
                } else {
                    100
                };
                info!(
                    window_secs        = WINDOW_SECS,
                    metis_quotes       = quotes,
                    metis_profitable   = profit,
                    tx_build_failed    = build_fail,
                    sim_submitted      = submitted,
                    sim_coverage_pct   = coverage_pct,
                    sim_executed       = executed,
                    sim_slippage_rej   = slippage,
                    sim_revert_rej     = revert,
                    sim_passed         = passed,
                    pmm_bypass         = pmm_byp,
                    jito_rate_limited  = ratelim,
                    jito_sent          = sent,
                    jito_grpc_sent     = grpc_sent,
                    "==[PIPELINE METRICS]==",
                );

                // Yellow (33m) for Metis round-trip, blue (34m) for build-to-dispatch.
                eprintln!(
                    "\x1b[33m[LATENCY] metis_quote_avg={}ms (n={})  |  \x1b[34mbuild_to_dispatch_avg={}ms (n={})\x1b[0m",
                    avg_metis_us / 1000,
                    metis_lat_cnt,
                    avg_dispatch_us / 1000,
                    disp_lat_cnt,
                );
            }
        });
    }
}
