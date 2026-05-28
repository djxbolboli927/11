use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 60;

pub struct Metrics {
    /// Stage 1: HTTP requests sent to Metis (2 per pair for quotes, +1 per calc for swap_instructions)
    pub metis_req_sent: AtomicU64,
    /// Stage 1: complete quote pairs where BOTH quote1+quote2 returned (regardless of profit)
    /// Divide metis_req_sent by 2 vs this number to see Metis drop rate.
    pub metis_resp_total: AtomicU64,
    /// Stage 1: profitable quote pairs (output_wsol > input_wsol, no forbidden DEX)
    pub metis_resp_ok: AtomicU64,
    /// Stage 1→2: dropped because all 6 calc slots were busy (semaphore full)
    pub dropped_busy: AtomicU64,
    /// Stage 2: dropped because both Jito REST+gRPC rate limiters were full
    pub dropped_rate_limit: AtomicU64,
    /// Stage 2: calc workers that successfully built a tx and put it on the Jito queue
    pub calc_done: AtomicU64,
    /// Stage 2+3: other drops (tx build fail, size limit, stale bundle, Jito send error)
    pub tx_dropped: AtomicU64,
    /// Stage 3: bundles successfully submitted to Jito (REST + gRPC combined)
    pub jito_sent: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_resp_total: AtomicU64::new(0),
            metis_resp_ok: AtomicU64::new(0),
            dropped_busy: AtomicU64::new(0),
            dropped_rate_limit: AtomicU64::new(0),
            calc_done: AtomicU64::new(0),
            tx_dropped: AtomicU64::new(0),
            jito_sent: AtomicU64::new(0),
        })
    }

    pub fn spawn_reporter(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await;
            loop {
                interval.tick().await;
                let sent      = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let resp      = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let ok        = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let busy      = m.dropped_busy.swap(0, Ordering::Relaxed);
                let ratelim   = m.dropped_rate_limit.swap(0, Ordering::Relaxed);
                let calc      = m.calc_done.swap(0, Ordering::Relaxed);
                let drop      = m.tx_dropped.swap(0, Ordering::Relaxed);
                let jito      = m.jito_sent.swap(0, Ordering::Relaxed);
                eprintln!(
                    "[{WINDOW_SECS}s] metis_sent={sent} | metis_resp={resp} | profitable={ok} | busy={busy} | rate_lim={ratelim} | calc_done={calc} | dropped={drop} | jito_sent={jito}"
                );
            }
        });
    }
}
