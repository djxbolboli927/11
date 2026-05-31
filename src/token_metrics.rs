use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct TokenStat {
    pub mint: String,
    /// Quote HTTP requests sent to Metis (2 per scan: quote1 + quote2).
    pub q_sent: AtomicU64,
    /// Scan pairs where Metis returned a valid route (both quotes succeeded).
    pub route_ok: AtomicU64,
    /// Scan pairs where Metis failed (HTTP error, timeout, or zero output).
    pub route_fail: AtomicU64,
    /// Profitable opportunities: output > input + min_profit_lamports.
    pub profitable: AtomicU64,
    /// Route returned but not profitable (output ≤ threshold, or forbidden DEX).
    pub not_profitable: AtomicU64,
}

pub struct TokenMetrics {
    pub stats: Vec<TokenStat>,
    index: HashMap<String, usize>,
}

impl TokenMetrics {
    pub fn new(token_mints: &[String]) -> Arc<Self> {
        let stats: Vec<TokenStat> = token_mints
            .iter()
            .map(|m| TokenStat {
                mint: m.clone(),
                q_sent: AtomicU64::new(0),
                route_ok: AtomicU64::new(0),
                route_fail: AtomicU64::new(0),
                profitable: AtomicU64::new(0),
                not_profitable: AtomicU64::new(0),
            })
            .collect();
        let index: HashMap<String, usize> = stats
            .iter()
            .enumerate()
            .map(|(i, s)| (s.mint.clone(), i))
            .collect();
        Arc::new(Self { stats, index })
    }

    /// Look up per-token stats by mint address. O(1).
    pub fn get(&self, mint: &str) -> Option<&TokenStat> {
        self.index.get(mint).and_then(|&i| self.stats.get(i))
    }

    /// Print a per-token breakdown every 30 s, sorted by profitable desc.
    pub fn spawn_reporter(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await; // discard first immediate tick

            loop {
                interval.tick().await;

                // Snapshot and reset all counters atomically.
                struct Row {
                    mint: String,
                    q_sent: u64,
                    route_ok: u64,
                    route_fail: u64,
                    profitable: u64,
                    not_profitable: u64,
                }

                let mut rows: Vec<Row> = m
                    .stats
                    .iter()
                    .map(|s| Row {
                        mint: s.mint.clone(),
                        q_sent: s.q_sent.swap(0, Ordering::Relaxed),
                        route_ok: s.route_ok.swap(0, Ordering::Relaxed),
                        route_fail: s.route_fail.swap(0, Ordering::Relaxed),
                        profitable: s.profitable.swap(0, Ordering::Relaxed),
                        not_profitable: s.not_profitable.swap(0, Ordering::Relaxed),
                    })
                    .filter(|r| r.q_sent > 0 || r.route_ok > 0)
                    .collect();

                if rows.is_empty() {
                    continue;
                }

                // Sort: profitable desc, then route_ok desc.
                rows.sort_by(|a, b| {
                    b.profitable
                        .cmp(&a.profitable)
                        .then(b.route_ok.cmp(&a.route_ok))
                });

                let t_q: u64 = rows.iter().map(|r| r.q_sent).sum();
                let t_ok: u64 = rows.iter().map(|r| r.route_ok).sum();
                let t_fail: u64 = rows.iter().map(|r| r.route_fail).sum();
                let t_prof: u64 = rows.iter().map(|r| r.profitable).sum();
                let t_noprof: u64 = rows.iter().map(|r| r.not_profitable).sum();

                eprintln!(
                    "\n[30s TOKEN REPORT] tokens={} | q_sent={} | route_ok={} | route_fail={} | profitable={} | no_profit={}",
                    rows.len(), t_q, t_ok, t_fail, t_prof, t_noprof,
                );
                eprintln!(
                    "  {:<16}  {:>8}  {:>10}  {:>12}  {:>12}  {:>10}",
                    "TOKEN", "q_sent", "route_ok", "route_fail", "profitable", "no_profit"
                );
                for r in &rows {
                    eprintln!(
                        "  {:<16}  {:>8}  {:>10}  {:>12}  {:>12}  {:>10}",
                        abbrev(&r.mint),
                        r.q_sent,
                        r.route_ok,
                        r.route_fail,
                        r.profitable,
                        r.not_profitable,
                    );
                }
                eprintln!();
            }
        });
    }
}

fn abbrev(mint: &str) -> String {
    if mint.len() <= 16 {
        return mint.to_string();
    }
    format!("{}..{}", &mint[..8], &mint[mint.len() - 4..])
}
