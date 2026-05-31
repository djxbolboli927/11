use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type RouteSignature = u64;

/// One cached entry — the most-recent Metis response for this route structure.
#[derive(Clone, Serialize, Deserialize)]
pub struct CachedRoute {
    /// Hex of the RouteSignature (stable across restarts).
    pub sig: String,
    /// DEX labels in hop order (e.g. ["Orca", "Raydium"]).
    pub dex_path: Vec<String>,
    /// Full Metis /swap-instructions response; refreshed on every new hit.
    pub swap_ixs: SwapInstructionsResponse,
    /// Total number of times this route was seen.
    pub hit_count: u64,
    /// Unix timestamp (seconds) of the most-recent update.
    pub last_seen_ts: u64,
}

struct CacheInner {
    routes: HashMap<RouteSignature, CachedRoute>,
    dirty: bool,
}

pub struct InstructionCache {
    inner: RwLock<CacheInner>,
}

impl InstructionCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(CacheInner {
                routes: HashMap::new(),
                dirty: false,
            }),
        })
    }

    /// Compute a stable, amount-independent route signature from a merged route_plan.
    ///
    /// Hashes: hop count + per-hop (ammKey | label | inputMint | outputMint).
    /// Amounts are deliberately excluded so the same route with a different trade
    /// size still produces the same signature.
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

    /// Look up a cached route. Returns `None` on miss (O(1) read, no write lock).
    pub fn lookup(&self, sig: RouteSignature) -> Option<CachedRoute> {
        self.inner.read().ok()?.routes.get(&sig).cloned()
    }

    /// Store a fresh Metis response for this route.
    ///
    /// Returns `true` if this is the first time we've seen this route signature.
    /// On subsequent calls the stored entry is refreshed with the newest response.
    pub fn record(
        &self,
        sig: RouteSignature,
        dex_path: Vec<String>,
        swap_ixs: SwapInstructionsResponse,
    ) -> bool {
        let mut inner = self.inner.write().unwrap();
        let now = unix_now();
        let is_new = !inner.routes.contains_key(&sig);
        if is_new {
            inner.routes.insert(
                sig,
                CachedRoute {
                    sig: format!("{sig:016x}"),
                    dex_path,
                    swap_ixs,
                    hit_count: 1,
                    last_seen_ts: now,
                },
            );
        } else if let Some(entry) = inner.routes.get_mut(&sig) {
            entry.swap_ixs = swap_ixs;
            entry.hit_count += 1;
            entry.last_seen_ts = now;
        }
        inner.dirty = true;
        is_new
    }

    /// Number of unique routes currently in the cache.
    pub fn route_count(&self) -> usize {
        self.inner.read().map(|g| g.routes.len()).unwrap_or(0)
    }

    /// Spawn a background task that flushes dirty cache entries to disk every
    /// `interval_secs` seconds. Never blocks the hot path.
    pub fn spawn_flush_task(self: &Arc<Self>, interval_secs: u64) {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(interval_secs));
            interval.tick().await; // skip immediate tick
            loop {
                interval.tick().await;
                if let Err(e) = cache.flush_to_disk() {
                    eprintln!("[cache] flush error: {e}");
                }
            }
        });
    }

    fn flush_to_disk(&self) -> anyhow::Result<()> {
        // Snapshot under read lock — hold it as briefly as possible.
        let snapshot: Vec<CachedRoute> = {
            let inner = self.inner.read().unwrap();
            if !inner.dirty {
                return Ok(());
            }
            inner.routes.values().cloned().collect()
        };

        // Clear dirty flag (write lock, but data stays intact).
        self.inner.write().unwrap().dirty = false;

        let cache_dir = std::path::Path::new("/root/c/cache");
        std::fs::create_dir_all(cache_dir.join("routes"))?;
        std::fs::create_dir_all(cache_dir.join("hops"))?;
        std::fs::create_dir_all(cache_dir.join("index"))?;

        // routes/hot_routes.json — full response keyed by hex signature.
        let route_map: HashMap<&str, &CachedRoute> =
            snapshot.iter().map(|r| (r.sig.as_str(), r)).collect();
        std::fs::write(
            cache_dir.join("routes/hot_routes.json"),
            serde_json::to_string_pretty(&route_map)?,
        )?;

        // hops/<dex>.json — one file per DEX, listing every route that touches
        // that DEX (full instructions included so each DEX can be inspected in
        // isolation). A route appears in every DEX file it uses.
        let mut by_dex: HashMap<String, Vec<&CachedRoute>> = HashMap::new();
        for r in &snapshot {
            let mut seen: Vec<&str> = Vec::new();
            for dex in &r.dex_path {
                if seen.contains(&dex.as_str()) {
                    continue; // don't list the same route twice in one DEX file
                }
                seen.push(dex.as_str());
                by_dex.entry(dex.clone()).or_default().push(r);
            }
        }
        for (dex, routes) in &by_dex {
            let map: HashMap<&str, &CachedRoute> =
                routes.iter().map(|r| (r.sig.as_str(), *r)).collect();
            let fname = format!("{}.json", sanitize_filename(dex));
            std::fs::write(
                cache_dir.join("hops").join(fname),
                serde_json::to_string_pretty(&map)?,
            )?;
        }

        // index/template_index.json — lightweight summary (no instruction bytes).
        let index: HashMap<&str, serde_json::Value> = snapshot
            .iter()
            .map(|r| {
                (
                    r.sig.as_str(),
                    serde_json::json!({
                        "dex_path":     r.dex_path,
                        "hit_count":    r.hit_count,
                        "last_seen_ts": r.last_seen_ts,
                    }),
                )
            })
            .collect();
        std::fs::write(
            cache_dir.join("index/template_index.json"),
            serde_json::to_string_pretty(&index)?,
        )?;

        Ok(())
    }

    /// Load a previously-flushed hot_routes.json back into RAM at startup so the
    /// cache survives restarts. Returns the number of routes loaded (0 if the
    /// file is missing — a fresh start). Best-effort: a corrupt file is ignored.
    pub fn load_from_disk(&self) -> usize {
        let path = std::path::Path::new("/root/c/cache/routes/hot_routes.json");
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        let parsed: HashMap<String, CachedRoute> = match serde_json::from_str(&content) {
            Ok(p) => p,
            Err(_) => return 0,
        };
        let mut inner = self.inner.write().unwrap();
        for route in parsed.into_values() {
            let sig = u64::from_str_radix(&route.sig, 16).unwrap_or(0);
            inner.routes.insert(sig, route);
        }
        inner.routes.len()
    }
}

/// Structural comparison between a cached and a fresh `SwapInstructionsResponse`.
///
/// Returns `true` when:
/// - Same Jupiter program_id
/// - Same account list (pubkeys, signer flags, writable flags)
/// - Same base64 data length (structure preserved; actual bytes may differ due to amounts)
/// - Same number of setup instructions
///
/// Data-byte differences are expected and are NOT treated as a mismatch.
pub fn instructions_match_structurally(
    cached: &SwapInstructionsResponse,
    fresh: &SwapInstructionsResponse,
) -> bool {
    let ci = &cached.swap_instruction;
    let fi = &fresh.swap_instruction;

    if ci.program_id != fi.program_id {
        return false;
    }
    if ci.accounts.len() != fi.accounts.len() {
        return false;
    }
    for (ca, fa) in ci.accounts.iter().zip(fi.accounts.iter()) {
        if ca.pubkey != fa.pubkey
            || ca.is_signer != fa.is_signer
            || ca.is_writable != fa.is_writable
        {
            return false;
        }
    }
    if ci.data.len() != fi.data.len() {
        return false;
    }
    cached.setup_instructions.len() == fresh.setup_instructions.len()
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

// FNV-1a 64-bit — fast, stable across runs, no dependency.
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

/// Make a DEX label safe to use as a filename (DEX labels may contain spaces,
/// slashes, etc.). Replaces anything that isn't alphanumeric/-/_ with '_'.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
