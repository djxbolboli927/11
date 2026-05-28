use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 60;

pub struct Metrics {
    /// Stage 1: total HTTP requests sent to Metis (2 per pair for quotes, +1 per profitable for swap_instructions)
    pub metis_req_sent: AtomicU64,
    /// Stage 1: profitable quote pairs found (output_wsol > input_wsol)
    pub metis_resp_ok: AtomicU64,
    /// Stage 2: calc workers that successfully built a tx and put it on the Jito queue
    pub calc_done: AtomicU64,
    /// Any stage: opportunities dropped (no calc slot, tx build fail, stale, rate limited, etc.)
    pub tx_dropped: AtomicU64,
    /// Stage 3: bundles successfully submitted to Jito (REST + gRPC combined)
    pub jito_sent: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_resp_ok: AtomicU64::new(0),
            calc_done: AtomicU64::new(0),
            tx_dropped: AtomicU64::new(0),
            jito_sent: AtomicU64::new(0),
        })
    }

    /// Spawn a background task that prints a one-line report every 60s and resets counters.
    /// Uses eprintln! directly so output is always visible regardless of RUST_LOG level.
    pub fn spawn_reporter(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                let sent  = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let ok    = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let calc  = m.calc_done.swap(0, Ordering::Relaxed);
                let drop  = m.tx_dropped.swap(0, Ordering::Relaxed);
                let jito  = m.jito_sent.swap(0, Ordering::Relaxed);
                eprintln!(
                    "[{WINDOW_SECS}s] metis_sent={sent} | profitable={ok} | calc_done={calc} | dropped={drop} | jito_sent={jito}"
                );
            }
        });
    }
}
