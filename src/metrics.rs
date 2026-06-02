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
    pub metis_resp_ok: AtomicU64,
    /// Items that successfully entered the LIFO queue (template hit OR swap_ix success).
    pub swap_ix_ok: AtomicU64,

    // ── Stage 1.5: template lookup + swap_instructions ────────────────────────
    /// Instructions served from RouteTemplate in RAM (Tier-1 exact + Tier-2 hop-pair).
    /// No /swap-instructions call was made for these.
    pub ix_from_ram: AtomicU64,
    /// Instructions obtained from Metis /swap-instructions (Tier-3 fallback).
    pub ix_from_metis: AtomicU64,
    /// RouteTemplate hit: served from RAM with amount patching (no Metis call).
    pub route_template_hit: AtomicU64,
    /// All hops in the route had a HopTemplate (metrics only; no composer yet).
    pub hop_template_all_hit: AtomicU64,
    /// At least one hop in the route was missing a HopTemplate.
    pub hop_template_missing: AtomicU64,
    /// /swap-instructions returned an error. Sum of four below.
    pub swap_ix_failed: AtomicU64,
    /// Breakdown: request exceeded quote_timeout_ms (Metis too slow).
    pub swap_ix_timeout: AtomicU64,
    /// Breakdown: Metis returned non-2xx (no route / rejected merged quote).
    pub swap_ix_http: AtomicU64,
    /// Breakdown: connection-level failure (TCP reset, pool exhausted, etc.).
    pub swap_ix_network: AtomicU64,
    /// Breakdown: 2xx body could not be parsed as SwapInstructionsResponse.
    pub swap_ix_parse: AtomicU64,
    /// Items pushed into the LIFO queue.
    pub queue_in: AtomicU64,
    /// Current LIFO queue depth (gauge).
    pub queue_depth: AtomicI64,

    // ── Stage 2: worker processing ────────────────────────────────────────────
    pub dropped_stale: AtomicU64,
    pub tx_build_failed: AtomicU64,
    pub tx_too_large: AtomicU64,
    pub calc_done: AtomicU64,

    // ── Stage 3: Jito send ────────────────────────────────────────────────────
    pub rate_requeued: AtomicU64,
    pub jito_send_failed: AtomicU64,
    pub jito_sent: AtomicU64,

    // ── Legacy aggregates (drained each window, not shown) ────────────────────
    pub dropped_busy: AtomicU64,
    pub tx_dropped: AtomicU64,

    // ── swap_instructions latency ─────────────────────────────────────────────
    pub metis_fetch_ms_total: AtomicU64,
    pub metis_fetch_samples: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_resp_total: AtomicU64::new(0),
            metis_resp_ok: AtomicU64::new(0),
            swap_ix_ok: AtomicU64::new(0),
            ix_from_ram: AtomicU64::new(0),
            ix_from_metis: AtomicU64::new(0),
            route_template_hit: AtomicU64::new(0),
            hop_template_all_hit: AtomicU64::new(0),
            hop_template_missing: AtomicU64::new(0),
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

    pub fn spawn_reporter(
        self: &Arc<Self>,
        queue_max_age_ms: u64,
        store: Arc<crate::template_cache::TemplateStore>,
    ) {
        let m = self.clone();
        let ttl_secs = queue_max_age_ms as f64 / 1000.0;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await;

            loop {
                interval.tick().await;

                let sent      = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let routes    = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let profit    = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let sw_ok     = m.swap_ix_ok.swap(0, Ordering::Relaxed);

                let from_ram   = m.ix_from_ram.swap(0, Ordering::Relaxed);
                let from_metis = m.ix_from_metis.swap(0, Ordering::Relaxed);
                let rt_hit    = m.route_template_hit.swap(0, Ordering::Relaxed);
                let ht_all    = m.hop_template_all_hit.swap(0, Ordering::Relaxed);
                let ht_miss   = m.hop_template_missing.swap(0, Ordering::Relaxed);

                let swap_fail = m.swap_ix_failed.swap(0, Ordering::Relaxed);
                let sf_to     = m.swap_ix_timeout.swap(0, Ordering::Relaxed);
                let sf_http   = m.swap_ix_http.swap(0, Ordering::Relaxed);
                let sf_net    = m.swap_ix_network.swap(0, Ordering::Relaxed);
                let sf_parse  = m.swap_ix_parse.swap(0, Ordering::Relaxed);
                let q_in      = m.queue_in.swap(0, Ordering::Relaxed);

                let stale     = m.dropped_stale.swap(0, Ordering::Relaxed);
                let build     = m.tx_build_failed.swap(0, Ordering::Relaxed);
                let too_big   = m.tx_too_large.swap(0, Ordering::Relaxed);
                let calc      = m.calc_done.swap(0, Ordering::Relaxed);
                let requeued  = m.rate_requeued.swap(0, Ordering::Relaxed);
                let jfail     = m.jito_send_failed.swap(0, Ordering::Relaxed);
                let jito      = m.jito_sent.swap(0, Ordering::Relaxed);

                let ms_ms     = m.metis_fetch_ms_total.swap(0, Ordering::Relaxed);
                let ms_n      = m.metis_fetch_samples.swap(0, Ordering::Relaxed);

                let depth     = m.queue_depth.load(Ordering::Relaxed);
                let _         = m.tx_dropped.swap(0, Ordering::Relaxed);
                let _         = m.dropped_busy.swap(0, Ordering::Relaxed);

                let avg_ms    = if ms_n > 0 { ms_ms / ms_n } else { 0 };
                let n_routes  = store.route_count();
                let n_hops    = store.hop_count();

                let ram_pct = if sw_ok > 0 { from_ram * 100 / sw_ok } else { 0 };

                eprintln!(
                    "[{WINDOW_SECS}s] \
metis_sent={sent} routes={routes} quoted_profitable={profit}\n  \
TEMPLATE  : route_hit={rt_hit}  hop_all_hit={ht_all}  hop_miss={ht_miss}  routes={n_routes}  hops={n_hops}\n  \
IX-SOURCE : from_ram={from_ram}  from_metis={from_metis}  ram_pct={ram_pct}%\n  \
PRE-QUEUE : swap_ix_ok={sw_ok}  swap_ix_fail={swap_fail} [timeout={sf_to} http={sf_http} net={sf_net} parse={sf_parse}] -> queue_in={q_in}  (depth_now={depth})\n  \
IN-QUEUE  : stale={stale} (waited >{ttl_secs}s)\n  \
TX-BUILD  : build_fail={build}  too_large={too_big}  calc_ok={calc}\n  \
JITO      : sent={jito}  send_fail={jfail}  waited_for_slot={requeued}\n  \
SWAP-IX   : avg_metis={avg_ms}ms"
                );
            }
        });
    }
}
