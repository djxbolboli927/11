use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type RouteSignature = u64;

/// How long a confirmed cache entry is considered valid within a single run.
/// After this interval the entry is treated as a miss and Metis is called again
/// to pick up any pool-state changes. Set to 0 to always re-fetch.
const INTRA_RUN_TTL_SECS: u64 = 60;

/// Hard limit on in-RAM entries. When the cache is full the oldest entry
/// (by last_seen_ts) is evicted on every new insert. This bounds steady-state
/// RAM to roughly MAX_CACHE_ENTRIES × ~3 KB ≈ 6 MB at the default.
/// It also caps how much JSON we write/read at each flush cycle.
/// Keep this small: Metis runs on the same machine and shares physical RAM.
/// With 17 tokens × 246 amounts × 2 modes = ~8k pairs, a few hundred are
/// profitable at any one time — 2000 slots is 5–10× headroom.
const MAX_CACHE_ENTRIES: usize = 2_000;

/// Cache key: route structure + exact input amount.
///
/// Including the amount means a cache hit guarantees the same instruction bytes
/// Metis would produce — the `in_amount` / `out_amount` / `other_amount_threshold`
/// fields inside the instruction data are all determined by this pair.
type CacheKey = (RouteSignature, u64);

/// One cached entry per (route_signature, amount) pair.
#[derive(Clone, Serialize, Deserialize)]
pub struct CachedRoute {
    /// Hex of the RouteSignature (stable across restarts, amount-independent).
    pub sig: String,
    /// The exact input lamports this instruction was built for.
    pub amount: u64,
    /// DEX labels in hop order (e.g. ["Orca", "Raydium"]).
    pub dex_path: Vec<String>,
    /// Full Metis /swap-instructions response; refreshed on every new hit.
    pub swap_ixs: SwapInstructionsResponse,
    /// How many times we've served this exact (route, amount) pair.
    pub hit_count: u64,
    /// Unix timestamp (seconds) of the most-recent update.
    pub last_seen_ts: u64,
    /// Monotonic timestamp of the last Metis confirmation in this process run.
    /// `None` means the entry was loaded from disk and has not been re-confirmed
    /// yet; those entries are NOT served until Metis validates them.
    /// Entries older than `INTRA_RUN_TTL_SECS` are also treated as misses,
    /// forcing a re-fetch so pool-state changes are picked up.
    /// Never persisted to disk — always `None` on load.
    #[serde(skip, default)]
    pub confirmed_at: Option<std::time::Instant>,
}

struct CacheInner {
    entries: HashMap<CacheKey, CachedRoute>,
    dirty: bool,
}

pub struct InstructionCache {
    inner: RwLock<CacheInner>,
}

impl InstructionCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(CacheInner {
                entries: HashMap::new(),
                dirty: false,
            }),
        })
    }

    /// Stable, amount-independent route signature.
    /// Hashes: hop count + per-hop (ammKey | label | inputMint | outputMint).
    pub fn compute_signature(route_plan: &serde_json::Value) -> RouteSignature {
        let mut key = String::new();
        if let Some(hops) = route_plan.as_array() {
            use std::fmt::Write as _;
            let _ = write!(key, "{}", hops.len());
            for hop in hops {
                if let Some(info) = hop.get("swapInfo") {
                    for field in &["ammKey", "label", "inputMint", "outputMint"] {
                        key.push('|');
                        key.push_str(
                            info.get(field).and_then(|v| v.as_str()).unwrap_or(""),
                        );
                    }
                }
            }
        }
        fnv1a(&key)
    }

    /// Exact lookup: returns `Some` only when route + amount match AND the entry
    /// was confirmed by Metis in the current process run within `INTRA_RUN_TTL_SECS`.
    ///
    /// Disk-loaded entries have `confirmed_at = None` → treated as miss until
    /// Metis confirms them. Entries older than the TTL are also treated as miss
    /// so pool-state changes are picked up (re-fetch is demand-driven).
    pub fn lookup(&self, sig: RouteSignature, amount: u64) -> Option<CachedRoute> {
        self.inner
            .read()
            .ok()?
            .entries
            .get(&(sig, amount))
            .filter(|r| {
                r.confirmed_at
                    .map(|t| t.elapsed().as_secs() < INTRA_RUN_TTL_SECS)
                    .unwrap_or(false)
            })
            .cloned()
    }

    /// Store (or refresh) a Metis response for this exact (route, amount) pair.
    /// Returns `true` if this is the first time this combination was seen.
    pub fn record(
        &self,
        sig: RouteSignature,
        amount: u64,
        dex_path: Vec<String>,
        swap_ixs: SwapInstructionsResponse,
    ) -> bool {
        let mut inner = self.inner.write().unwrap();
        let key = (sig, amount);
        let is_new = !inner.entries.contains_key(&key);
        if is_new {
            inner.entries.insert(
                key,
                CachedRoute {
                    sig: format!("{sig:016x}"),
                    amount,
                    dex_path,
                    swap_ixs,
                    hit_count: 1,
                    last_seen_ts: unix_now(),
                    confirmed_at: Some(std::time::Instant::now()),
                },
            );
        } else if let Some(entry) = inner.entries.get_mut(&key) {
            entry.swap_ixs = swap_ixs;
            entry.hit_count += 1;
            entry.last_seen_ts = unix_now();
            entry.confirmed_at = Some(std::time::Instant::now());
        }
        inner.dirty = true;

        // Evict the least-recently-seen entry when the cache is at capacity.
        // Only runs on inserts (is_new), not on updates.
        if is_new && inner.entries.len() > MAX_CACHE_ENTRIES {
            if let Some(oldest) = inner
                .entries
                .iter()
                .min_by_key(|(_, v)| v.last_seen_ts)
                .map(|(k, _)| *k)
            {
                inner.entries.remove(&oldest);
            }
        }

        is_new
    }

    /// Total (route, amount) entries stored.
    pub fn entry_count(&self) -> usize {
        self.inner.read().map(|g| g.entries.len()).unwrap_or(0)
    }

    /// Number of distinct route signatures (unique DEX paths, amount-independent).
    pub fn route_count(&self) -> usize {
        self.inner
            .read()
            .map(|g| {
                let mut seen = std::collections::HashSet::new();
                for (sig, _) in g.entries.keys() {
                    seen.insert(*sig);
                }
                seen.len()
            })
            .unwrap_or(0)
    }

    /// Spawn background flush every `interval_secs` seconds. Never blocks hot path.
    pub fn spawn_flush_task(self: &Arc<Self>, interval_secs: u64) {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(interval_secs));
            interval.tick().await;
            loop {
                interval.tick().await;
                // JSON serialisation + fs::write are blocking; run off the
                // async executor so tokio threads stay free for quote/swap tasks.
                let c = cache.clone();
                match tokio::task::spawn_blocking(move || c.flush_to_disk()).await {
                    Ok(Err(e)) => eprintln!("[cache] flush error: {e}"),
                    Err(e) => eprintln!("[cache] flush panic: {e}"),
                    Ok(Ok(())) => {}
                }
            }
        });
    }

    fn flush_to_disk(&self) -> anyhow::Result<()> {
        let snapshot: Vec<CachedRoute> = {
            let inner = self.inner.read().unwrap();
            if !inner.dirty {
                return Ok(());
            }
            inner.entries.values().cloned().collect()
        };
        self.inner.write().unwrap().dirty = false;

        let cache_dir = std::path::Path::new("/root/c/cache");
        std::fs::create_dir_all(cache_dir.join("routes"))?;
        std::fs::create_dir_all(cache_dir.join("hops"))?;
        std::fs::create_dir_all(cache_dir.join("index"))?;

        // routes/hot_routes.json — full entry per (route, amount) pair.
        // Key format: "<sig_hex>:<amount_lamports>" for human readability.
        let route_map: HashMap<String, &CachedRoute> = snapshot
            .iter()
            .map(|r| (format!("{}:{}", r.sig, r.amount), r))
            .collect();
        std::fs::write(
            cache_dir.join("routes/hot_routes.json"),
            serde_json::to_string(&route_map)?,
        )?;

        // hops/<dex>.json — per-DEX view (all routes touching that DEX).
        let mut by_dex: HashMap<String, Vec<&CachedRoute>> = HashMap::new();
        for r in &snapshot {
            let mut seen_in_route: Vec<&str> = Vec::new();
            for dex in &r.dex_path {
                if !seen_in_route.contains(&dex.as_str()) {
                    seen_in_route.push(dex.as_str());
                    by_dex.entry(dex.clone()).or_default().push(r);
                }
            }
        }
        for (dex, routes) in &by_dex {
            let map: HashMap<String, &CachedRoute> = routes
                .iter()
                .map(|r| (format!("{}:{}", r.sig, r.amount), *r))
                .collect();
            let fname = format!("{}.json", sanitize_filename(dex));
            std::fs::write(
                cache_dir.join("hops").join(fname),
                serde_json::to_string(&map)?,
            )?;
        }

        // index/template_index.json — lightweight summary grouped by route sig.
        let mut by_sig: HashMap<&str, serde_json::Value> = HashMap::new();
        for r in &snapshot {
            let entry = by_sig.entry(r.sig.as_str()).or_insert_with(|| {
                serde_json::json!({
                    "dex_path":     r.dex_path,
                    "amounts":      [],
                    "total_hits":   0u64,
                    "last_seen_ts": r.last_seen_ts,
                })
            });
            if let Some(obj) = entry.as_object_mut() {
                if let Some(arr) = obj.get_mut("amounts").and_then(|a| a.as_array_mut()) {
                    arr.push(serde_json::json!(r.amount));
                }
                if let Some(hits) = obj.get_mut("total_hits").and_then(|v| v.as_u64()) {
                    obj.insert(
                        "total_hits".to_string(),
                        serde_json::json!(hits + r.hit_count),
                    );
                }
                if let Some(ts) = obj.get("last_seen_ts").and_then(|v| v.as_u64()) {
                    if r.last_seen_ts > ts {
                        obj.insert("last_seen_ts".to_string(), serde_json::json!(r.last_seen_ts));
                    }
                }
            }
        }
        std::fs::write(
            cache_dir.join("index/template_index.json"),
            serde_json::to_string(&by_sig)?,
        )?;

        Ok(())
    }

    /// Reload hot_routes.json into RAM on startup. Returns entries loaded.
    ///
    /// Entries whose last_seen_ts is within INTRA_RUN_TTL_SECS of now are
    /// marked confirmed immediately so they can be served from cache on the
    /// very first scan (avoiding a startup flood of swap_instructions calls).
    /// Stale entries are loaded cold and require a Metis round-trip before
    /// they become hot.
    pub fn load_from_disk(&self) -> usize {
        const MAX_LOAD_BYTES: u64 = 20 * 1024 * 1024; // 20 MB — matches new MAX_CACHE_ENTRIES

        let path = std::path::Path::new("/root/c/cache/routes/hot_routes.json");

        // Guard: refuse to load files that would cause a large startup RAM spike.
        match std::fs::metadata(path) {
            Ok(meta) if meta.len() > MAX_LOAD_BYTES => {
                eprintln!(
                    "[cache] hot_routes.json is {} MB — too large to load safely (limit 150 MB). \
                     Cache will rebuild from Metis this run. \
                     Delete the file or wait for the next flush to get the compact format.",
                    meta.len() / 1_048_576
                );
                return 0;
            }
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                // Can't stat the file for some other reason; skip gracefully.
                return 0;
            }
            _ => {}
        }

        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        // Key format: "<sig_hex>:<amount>"
        let raw: HashMap<String, CachedRoute> = match serde_json::from_str(&content) {
            Ok(p) => p,
            Err(_) => return 0,
        };
        // Sort by last_seen_ts descending and keep only the freshest
        // MAX_CACHE_ENTRIES entries so startup RAM usage is bounded even
        // when the on-disk file has grown very large.
        let mut all: Vec<CachedRoute> = raw.into_values().collect();
        all.sort_unstable_by(|a, b| b.last_seen_ts.cmp(&a.last_seen_ts));
        all.truncate(MAX_CACHE_ENTRIES);

        let now_ts = unix_now();
        let mut inner = self.inner.write().unwrap();
        for mut route in all {
            let sig = u64::from_str_radix(&route.sig, 16).unwrap_or(0);
            // Entries last seen within TTL are immediately usable so the first
            // scan can hit the cache rather than flooding Metis with
            // swap_instructions calls for every profitable quote.
            // The route_v2 instruction bytes are safe to reuse: on-chain
            // other_amount_threshold is embedded in the instruction and reverts
            // the tx if profitability is gone.
            let age_secs = now_ts.saturating_sub(route.last_seen_ts);
            if age_secs < INTRA_RUN_TTL_SECS {
                route.confirmed_at = Some(std::time::Instant::now());
            }
            inner.entries.insert((sig, route.amount), route);
        }
        inner.entries.len()
    }
}

/// Record a swap_ix HTTP failure with its DEX path to
/// `/root/c/cache/swap_ix_failures.jsonl`.
///
/// Over time this file reveals which DEX paths Metis consistently refuses to
/// build circular instructions for. Routes that appear here frequently can be
/// added to `FORBIDDEN_DEX_LABELS` so the bot stops wasting Metis requests
/// on them.
pub fn append_swap_ix_failure(dex_path: &[String], http_status: u16) {
    let path = "/root/c/cache/swap_ix_failures.jsonl";
    let entry = serde_json::json!({
        "ts":         unix_now(),
        "dex_path":   dex_path,
        "http_status": http_status,
    });
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{entry}");
    }
}

/// Extract DEX labels from a route_plan in hop order.
pub fn extract_dex_labels(route_plan: &serde_json::Value) -> Vec<String> {
    let mut labels = Vec::new();
    if let Some(hops) = route_plan.as_array() {
        for hop in hops {
            let label = hop
                .get("swapInfo")
                .and_then(|i| i.get("label"))
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");
            labels.push(label.to_string());
        }
    }
    labels
}

// FNV-1a 64-bit — fast, stable across runs, no external dependency.
fn fnv1a(s: &str) -> u64 {
    const PRIME: u64 = 1_099_511_628_211;
    const OFFSET: u64 = 14_695_981_039_346_656_037;
    let mut h = OFFSET;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
