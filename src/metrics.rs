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
    /// 2a. Profitable opportunities dropped by the Jito per-second rate limiter
    ///     BEFORE they reach the simulator. This is usually the biggest gap
    ///     between `metis_profitable` and `sim_submitted`: Metis returns many
    ///     profitable quotes in a burst, the rate limiter only lets
    ///     `max_bundles_per_second` through per rolling 1-second window, and
    ///     the rest are dropped.
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
    /// 8. Bundles successfully dispatched to Jito (all regions)
    pub jito_sent: AtomicU64,
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
            jito_sent: AtomicU64::new(0),
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
                let sent       = m.jito_sent.swap(0, Ordering::Relaxed);
                info!(
                    window_secs        = WINDOW_SECS,
                    metis_quotes       = quotes,
                    metis_profitable   = profit,
                    jito_rate_limited  = ratelim,
                    tx_build_failed    = build_fail,
                    sim_submitted      = submitted,
                    sim_executed       = executed,
                    sim_slippage_rej   = slippage,
                    sim_revert_rej     = revert,
                    sim_passed         = passed,
                    jito_sent          = sent,
                    "==[PIPELINE METRICS]==",
                );
            }
        });
    }
}
