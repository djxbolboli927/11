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
        std::fs::create_dir_all(cache_dir.join("index"))?;

        // routes/hot_routes.json — full response keyed by hex signature.
        let route_map: HashMap<&str, &CachedRoute> =
            snapshot.iter().map(|r| (r.sig.as_str(), r)).collect();
        std::fs::write(
            cache_dir.join("routes/hot_routes.json"),
            serde_json::to_string_pretty(&route_map)?,
        )?;

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

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
