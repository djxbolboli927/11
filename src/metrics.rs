use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 30;

pub struct Metrics {
    // ── Stage 1: quoting ─────────────────────────────────────────────────────
    /// Total HTTP requests sent to Metis (quotes + swap_instructions)
    pub metis_req_sent: AtomicU64,
    /// Round-trips where both quote1+quote2 returned successfully
    pub metis_resp_total: AtomicU64,
    /// Quote pairs that passed the profitability check
    pub metis_resp_ok: AtomicU64,

    // ── Stage 1.5: swap_instructions + queue entry ────────────────────────────
    /// /swap-instructions returned an error (pre-queue drop — never enters LIFO)
    pub swap_ix_failed: AtomicU64,
    /// Items successfully pushed into the LIFO queue (= profitable - swap_ix_fail)
    pub queue_in: AtomicU64,
    /// Current LIFO queue depth (gauge: +1 on push, -1 on pop; read with load)
    pub queue_depth: AtomicI64,

    // ── Stage 2: worker processing ────────────────────────────────────────────
    /// Popped from queue but waited longer than queue_max_age_ms (in-queue drop)
    pub dropped_stale: AtomicU64,
    /// Transaction serialization / signing failed
    pub tx_build_failed: AtomicU64,
    /// Built tx exceeds Solana's 1232-byte limit
    pub tx_too_large: AtomicU64,
    /// Tx fully built and sized; attempted to claim a Jito rate-limit slot
    pub calc_done: AtomicU64,

    // ── Stage 3: Jito send ────────────────────────────────────────────────────
    /// Both REST and gRPC Jito limiters were full — tx discarded after build
    pub dropped_rate_limit: AtomicU64,
    /// Claimed a slot but Jito API returned an error
    pub jito_send_failed: AtomicU64,
    /// Bundle successfully accepted by Jito
    pub jito_sent: AtomicU64,

    // ── Legacy aggregates (kept for compatibility) ────────────────────────────
    pub dropped_busy: AtomicU64,
    pub tx_dropped: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_resp_total: AtomicU64::new(0),
            metis_resp_ok: AtomicU64::new(0),
            swap_ix_failed: AtomicU64::new(0),
            queue_in: AtomicU64::new(0),
            queue_depth: AtomicI64::new(0),
            dropped_stale: AtomicU64::new(0),
            tx_build_failed: AtomicU64::new(0),
            tx_too_large: AtomicU64::new(0),
            calc_done: AtomicU64::new(0),
            dropped_rate_limit: AtomicU64::new(0),
            jito_send_failed: AtomicU64::new(0),
            jito_sent: AtomicU64::new(0),
            dropped_busy: AtomicU64::new(0),
            tx_dropped: AtomicU64::new(0),
        })
    }

    /// Prints a funnel-style report every 30 s so every drop reason is visible.
    ///
    /// Pipeline:
    ///   profitable → [swap_ix_fail?] → QUEUE → [stale?] → TX build → [rate_lim?] → Jito
    pub fn spawn_reporter(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await; // discard the immediate first tick

            loop {
                interval.tick().await;

                // Counters: swap and reset.
                let sent      = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let routes    = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let profit    = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let swap_fail = m.swap_ix_failed.swap(0, Ordering::Relaxed);
                let q_in      = m.queue_in.swap(0, Ordering::Relaxed);
                let stale     = m.dropped_stale.swap(0, Ordering::Relaxed);
                let build     = m.tx_build_failed.swap(0, Ordering::Relaxed);
                let too_big   = m.tx_too_large.swap(0, Ordering::Relaxed);
                let calc      = m.calc_done.swap(0, Ordering::Relaxed);
                let rate_lim  = m.dropped_rate_limit.swap(0, Ordering::Relaxed);
                let jfail     = m.jito_send_failed.swap(0, Ordering::Relaxed);
                let jito      = m.jito_sent.swap(0, Ordering::Relaxed);

                // Gauge: read without reset.
                let depth     = m.queue_depth.load(Ordering::Relaxed);

                // Also drain legacy aggregates so they don't overflow.
                let _ = m.tx_dropped.swap(0, Ordering::Relaxed);
                let _ = m.dropped_busy.swap(0, Ordering::Relaxed);

                eprintln!(
                    "[{WINDOW_SECS}s] \
metis_sent={sent} routes={routes} profitable={profit}\n  \
PRE-QUEUE : swap_ix_fail={swap_fail} -> queue_in={q_in}  (depth_now={depth})\n  \
IN-QUEUE  : stale={stale} (expired in queue before worker picked up)\n  \
TX-BUILD  : build_fail={build}  too_large={too_big}  calc_ok={calc}\n  \
JITO      : rate_lim={rate_lim}  send_fail={jfail}  sent={jito}"
                );
            }
        });
    }
}
