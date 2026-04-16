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
                let quotes    = m.metis_quotes.swap(0, Ordering::Relaxed);
                let profit    = m.metis_profitable.swap(0, Ordering::Relaxed);
                let submitted = m.sim_submitted.swap(0, Ordering::Relaxed);
                let executed  = m.sim_executed.swap(0, Ordering::Relaxed);
                let slippage  = m.sim_slippage_rejected.swap(0, Ordering::Relaxed);
                let revert    = m.sim_revert_rejected.swap(0, Ordering::Relaxed);
                let passed    = m.sim_passed.swap(0, Ordering::Relaxed);
                let sent      = m.jito_sent.swap(0, Ordering::Relaxed);
                info!(
                    window_secs        = WINDOW_SECS,
                    metis_quotes       = quotes,
                    metis_profitable   = profit,
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
