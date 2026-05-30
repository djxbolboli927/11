use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 30;

pub struct Metrics {
    /// Stage 1: HTTP requests sent to Metis for quotes (2 per pair: quote1 + quote2).
    pub metis_req_sent: AtomicU64,
    /// Stage 1: individual quote HTTP responses that returned successfully.
    /// Each pair fires 2 quote requests; this increments once per successful response.
    /// timeout_count = metis_req_sent - metis_quote_ok
    pub metis_quote_ok: AtomicU64,
    /// Stage 1: complete quote pairs where BOTH quote1+quote2 returned (regardless of profit).
    pub metis_resp_total: AtomicU64,
    /// Stage 1: profitable quote pairs (output_wsol > input + min_profit_lamports from config).
    pub metis_resp_ok: AtomicU64,
    /// Stage 1→2: dropped because all workers were busy and channel was full.
    pub dropped_busy: AtomicU64,
    /// Stage 2: dropped because both Jito REST+gRPC rate limiters were full.
    pub dropped_rate_limit: AtomicU64,
    /// Stage 2: workers that successfully built a tx and sent to Jito.
    pub calc_done: AtomicU64,
    /// All stages: drops (swap_ixs fail, tx build fail, size limit, stale, Jito error).
    pub tx_dropped: AtomicU64,
    /// Stage 3: bundles successfully submitted to Jito (REST + gRPC combined).
    pub jito_sent: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_quote_ok: AtomicU64::new(0),
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
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await; // discard immediate first tick

            let mut acc_sent    = 0u64;
            let mut acc_qok     = 0u64;
            let mut acc_resp    = 0u64;
            let mut acc_ok      = 0u64;
            let mut acc_busy    = 0u64;
            let mut acc_ratelim = 0u64;
            let mut acc_calc    = 0u64;
            let mut acc_drop    = 0u64;
            let mut acc_jito    = 0u64;
            let mut second: u64 = 0;

            loop {
                interval.tick().await;
                second += 1;

                let sent    = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let qok     = m.metis_quote_ok.swap(0, Ordering::Relaxed);
                let resp    = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let ok      = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let busy    = m.dropped_busy.swap(0, Ordering::Relaxed);
                let ratelim = m.dropped_rate_limit.swap(0, Ordering::Relaxed);
                let calc    = m.calc_done.swap(0, Ordering::Relaxed);
                let drop    = m.tx_dropped.swap(0, Ordering::Relaxed);
                let jito    = m.jito_sent.swap(0, Ordering::Relaxed);

                // timeout = quote requests sent - quote responses received
                let timeout = sent.saturating_sub(qok);

                // Per-second probe line.
                // q_ok/timeout: how many quote responses returned vs timed out
                // profitable: pairs where output > input + min_profit_lamports (from config)
                // drop: swap_ixs failures + tx build failures + stale queue drops
                eprintln!(
                    "  [s{second:02}] q_ok={qok} timeout={timeout} | profitable={ok} | rate_lim={ratelim} | calc={calc} drop={drop} | jito={jito}"
                );

                acc_sent    += sent;
                acc_qok     += qok;
                acc_resp    += resp;
                acc_ok      += ok;
                acc_busy    += busy;
                acc_ratelim += ratelim;
                acc_calc    += calc;
                acc_drop    += drop;
                acc_jito    += jito;

                if second >= WINDOW_SECS {
                    let total_timeout = acc_sent.saturating_sub(acc_qok);
                    eprintln!(
                        "[{WINDOW_SECS}s TOTAL] q_sent={acc_sent} q_ok={acc_qok} q_timeout={total_timeout} | resp_both={acc_resp} profitable={acc_ok} | busy={acc_busy} rate_lim={acc_ratelim} | calc={acc_calc} drop={acc_drop} | jito={acc_jito}"
                    );
                    acc_sent    = 0;
                    acc_qok     = 0;
                    acc_resp    = 0;
                    acc_ok      = 0;
                    acc_busy    = 0;
                    acc_ratelim = 0;
                    acc_calc    = 0;
                    acc_drop    = 0;
                    acc_jito    = 0;
                    second = 0;
                }
            }
        });
    }
}
