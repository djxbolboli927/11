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
    /// Quote pairs that passed the profitability check at quote time.
    /// Fires synchronously inside quote_check — may lead queue_in by one window
    /// because the swap_instructions call runs asynchronously afterwards.
    pub metis_resp_ok: AtomicU64,
    /// Items that successfully entered the LIFO queue (cache hit OR swap_ix success).
    /// This is the definitive "made it to Jito" counter and is always sync with queue_in.
    pub swap_ix_ok: AtomicU64,

    // ── Stage 1.5: swap_instructions + queue entry ────────────────────────────
    /// /swap-instructions returned an error (pre-queue drop). Sum of four below.
    pub swap_ix_failed: AtomicU64,
    /// Breakdown: request exceeded quote_timeout_ms (Metis too slow).
    pub swap_ix_timeout: AtomicU64,
    /// Breakdown: Metis returned non-2xx (no route / rejected merged quote).
    pub swap_ix_http: AtomicU64,
    /// Breakdown: connection-level failure (TCP reset, pool exhausted, etc.).
    pub swap_ix_network: AtomicU64,
    /// Breakdown: 2xx body could not be parsed as SwapInstructionsResponse.
    pub swap_ix_parse: AtomicU64,
    /// Items pushed into the LIFO queue (profitable minus swap_ix_fail).
    pub queue_in: AtomicU64,
    /// Current LIFO queue depth (gauge: +1 push / -1 pop — use load, not swap).
    pub queue_depth: AtomicI64,

    // ── Stage 2: worker processing ────────────────────────────────────────────
    /// Popped from queue but aged past queue_max_age_ms — dropped (only drop reason).
    pub dropped_stale: AtomicU64,
    /// Transaction serialization / signing failed.
    pub tx_build_failed: AtomicU64,
    /// Built tx exceeds Solana's 1232-byte limit.
    pub tx_too_large: AtomicU64,
    /// Tx fully built and sized; proceeded to claim a Jito slot.
    pub calc_done: AtomicU64,

    // ── Stage 3: Jito send ────────────────────────────────────────────────────
    /// Distinct txs that waited at least once for a Jito rate-limit slot
    /// (bounded by queue_in — one count per tx, not per 20ms poll).
    pub rate_requeued: AtomicU64,
    /// Jito API returned an error after a slot was claimed.
    pub jito_send_failed: AtomicU64,
    /// Bundle successfully accepted by Jito.
    pub jito_sent: AtomicU64,

    // ── Legacy ────────────────────────────────────────────────────────────────
    pub dropped_busy: AtomicU64,
    pub tx_dropped: AtomicU64,

    // ── swap_instructions latency ─────────────────────────────────────────────
    /// Milliseconds spent waiting for all Metis swap_instructions responses.
    pub metis_fetch_ms_total: AtomicU64,
    /// Sample count for metis_fetch_ms_total.
    pub metis_fetch_samples: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_resp_total: AtomicU64::new(0),
            metis_resp_ok: AtomicU64::new(0),
            swap_ix_ok: AtomicU64::new(0),
            swap_ix_failed: AtomicU64::new(0),
            swap_ix_timeout: AtomicU64::new(0),
            swap_ix_http: AtomicU64::new(0),
            swap_ix_network: AtomicU64::new(0),
            swap_ix_parse: AtomicU64::new(0),
            queue_in: AtomicU64::new(0),
            queue_depth: AtomicI64::new(0),
            dropped_stale: AtomicU64::new(0),
            tx_build_failed: AtomicU64::new(0),
            tx_too_large: AtomicU64::new(0),
            calc_done: AtomicU64::new(0),
            rate_requeued: AtomicU64::new(0),
            jito_send_failed: AtomicU64::new(0),
            jito_sent: AtomicU64::new(0),
            dropped_busy: AtomicU64::new(0),
            tx_dropped: AtomicU64::new(0),
            metis_fetch_ms_total: AtomicU64::new(0),
            metis_fetch_samples: AtomicU64::new(0),
        })
    }

    /// Prints a funnel-style report every 30 s so every drop reason is visible.
    pub fn spawn_reporter(self: &Arc<Self>, queue_max_age_ms: u64) {
        let m = self.clone();
        let ttl_secs = queue_max_age_ms as f64 / 1000.0;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await;

            loop {
                interval.tick().await;

                // ── Quoting ───────────────────────────────────────────────────
                let sent      = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let routes    = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let profit    = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let sw_ok     = m.swap_ix_ok.swap(0, Ordering::Relaxed);

                // ── swap_instructions ────────────────────────────────────────
                let swap_fail = m.swap_ix_failed.swap(0, Ordering::Relaxed);
                let sf_to     = m.swap_ix_timeout.swap(0, Ordering::Relaxed);
                let sf_http   = m.swap_ix_http.swap(0, Ordering::Relaxed);
                let sf_net    = m.swap_ix_network.swap(0, Ordering::Relaxed);
                let sf_parse  = m.swap_ix_parse.swap(0, Ordering::Relaxed);
                let q_in      = m.queue_in.swap(0, Ordering::Relaxed);

                // ── Worker / Jito ────────────────────────────────────────────
                let stale     = m.dropped_stale.swap(0, Ordering::Relaxed);
                let build     = m.tx_build_failed.swap(0, Ordering::Relaxed);
                let too_big   = m.tx_too_large.swap(0, Ordering::Relaxed);
                let calc      = m.calc_done.swap(0, Ordering::Relaxed);
                let requeued  = m.rate_requeued.swap(0, Ordering::Relaxed);
                let jfail     = m.jito_send_failed.swap(0, Ordering::Relaxed);
                let jito      = m.jito_sent.swap(0, Ordering::Relaxed);

                // ── swap_instructions latency ────────────────────────────────
                let ms_ms     = m.metis_fetch_ms_total.swap(0, Ordering::Relaxed);
                let ms_n      = m.metis_fetch_samples.swap(0, Ordering::Relaxed);

                // ── Gauges (read without reset) ───────────────────────────────
                let depth      = m.queue_depth.load(Ordering::Relaxed);

                // ── Drain legacy aggregates ───────────────────────────────────
                let _ = m.tx_dropped.swap(0, Ordering::Relaxed);
                let _ = m.dropped_busy.swap(0, Ordering::Relaxed);

                let avg_metis_ms = if ms_n > 0 { ms_ms / ms_n } else { 0 };

                eprintln!(
                    "[{WINDOW_SECS}s] \
metis_sent={sent} routes={routes} quoted_profitable={profit} (async lag: swap results may appear in next window)\n  \
PRE-QUEUE : swap_ix_ok={sw_ok}  swap_ix_fail={swap_fail} [timeout={sf_to} http={sf_http} net={sf_net} parse={sf_parse}] -> queue_in={q_in}  (depth_now={depth})\n  \
IN-QUEUE  : stale={stale} (ONLY drop reason: waited >{ttl_secs}s for a send slot)\n  \
TX-BUILD  : build_fail={build}  too_large={too_big}  calc_ok={calc}\n  \
JITO      : sent={jito}  send_fail={jfail}  waited_for_slot={requeued}\n  \
SWAP-IX   : avg_metis={avg_metis_ms}ms"
                );
            }
        });
    }
}
