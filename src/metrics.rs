use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 30;

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
            // Tick once per second. Each second we print a per-second probe line
            // (how many profitable opportunities + sends happened in THAT second)
            // and accumulate into WINDOW_SECS totals. This makes the burst
            // hypothesis falsifiable: if profitable spikes in 1-2 seconds and is
            // near-zero otherwise, opportunities arrive in bursts; if it is spread
            // evenly yet jito_sent stays low, the bottleneck is elsewhere.
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await; // discard the immediate first tick

            let mut acc_sent = 0u64;
            let mut acc_resp = 0u64;
            let mut acc_ok = 0u64;
            let mut acc_busy = 0u64;
            let mut acc_ratelim = 0u64;
            let mut acc_calc = 0u64;
            let mut acc_drop = 0u64;
            let mut acc_jito = 0u64;
            let mut second: u64 = 0;

            loop {
                interval.tick().await;
                second += 1;

                let sent    = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let resp    = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let ok      = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let busy    = m.dropped_busy.swap(0, Ordering::Relaxed);
                let ratelim = m.dropped_rate_limit.swap(0, Ordering::Relaxed);
                let calc    = m.calc_done.swap(0, Ordering::Relaxed);
                let drop    = m.tx_dropped.swap(0, Ordering::Relaxed);
                let jito    = m.jito_sent.swap(0, Ordering::Relaxed);

                // Per-second probe line.
                eprintln!(
                    "  [s{second:02}] profitable={ok} | rate_lim={ratelim} | calc_done={calc} | jito_sent={jito}"
                );

                acc_sent    += sent;
                acc_resp    += resp;
                acc_ok      += ok;
                acc_busy    += busy;
                acc_ratelim += ratelim;
                acc_calc    += calc;
                acc_drop    += drop;
                acc_jito    += jito;

                if second >= WINDOW_SECS {
                    eprintln!(
                        "[{WINDOW_SECS}s TOTAL] metis_sent={acc_sent} | metis_resp={acc_resp} | profitable={acc_ok} | busy={acc_busy} | rate_lim={acc_ratelim} | calc_done={acc_calc} | dropped={acc_drop} | jito_sent={acc_jito}"
                    );
                    acc_sent = 0;
                    acc_resp = 0;
                    acc_ok = 0;
                    acc_busy = 0;
                    acc_ratelim = 0;
                    acc_calc = 0;
                    acc_drop = 0;
                    acc_jito = 0;
                    second = 0;
                }
            }
        });
    }
}
