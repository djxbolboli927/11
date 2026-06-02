use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Per-segment capacity. Total live entries stay within ~2×SEG_CAP, so the
/// cache never grows unbounded while still refreshing continuously.
const SEG_CAP: usize = 4_000;
const CACHE_FILE: &str = "/root/c/cache/routes/hot_routes.json";

/// In-RAM store for swap_instructions results, with optional disk persistence.
///
/// Generational (two-segment) design so the cache keeps tracking the routes
/// that are CURRENTLY being scanned instead of freezing once full:
///   - lookups check `hot` then `cold`; a hit in `cold` is promoted to `hot`
///   - inserts go to `hot`; when `hot` fills, `cold` is dropped and `hot`
///     becomes the new `cold` (old entries age out, recent ones survive)
///
/// No TTL timers, no per-request disk I/O. One periodic flush task writes the
/// whole map to disk every N seconds — Metis is never touched by the cache.
pub struct InstructionCache {
    inner: RwLock<Inner>,
}

struct Inner {
    hot: HashMap<(u64, u64), SwapInstructionsResponse>,
    cold: HashMap<(u64, u64), SwapInstructionsResponse>,
}

/// Stable, amount-INDEPENDENT signature of a route_plan.
///
/// Only the structural identity of each hop is hashed (ammKey, mints, label,
/// feeMint, …) — the per-quote numeric fields (inAmount, outAmount, feeAmount)
/// are skipped so the same DEX path produces the SAME signature across scans
/// regardless of trade size or market movement.
pub fn route_sig(route_plan: &serde_json::Value) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0001_0000_01b3);
        }
    };
    if let Some(arr) = route_plan.as_array() {
        for hop in arr {
            if let Some(si) = hop.get("swapInfo").and_then(|s| s.as_object()) {
                let mut keys: Vec<&String> = si.keys().collect();
                keys.sort();
                for k in keys {
                    if k == "inAmount" || k == "outAmount" || k == "feeAmount" {
                        continue;
                    }
                    if let Some(s) = si.get(k).and_then(|v| v.as_str()) {
                        feed(k.as_bytes());
                        feed(b"=");
                        feed(s.as_bytes());
                        feed(b";");
                    }
                }
            }
        }
    }
    h
}

#[derive(Serialize)]
struct DiskEntryRef<'a> {
    sig: u64,
    amount: u64,
    swap_ixs: &'a SwapInstructionsResponse,
}

#[derive(Deserialize)]
struct DiskEntry {
    sig: u64,
    amount: u64,
    swap_ixs: SwapInstructionsResponse,
}

impl InstructionCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Inner {
                hot: HashMap::new(),
                cold: HashMap::new(),
            }),
        })
    }

    /// Returns a cloned entry if present (checks hot then cold; promotes cold hits).
    pub fn get(&self, sig: u64, amount: u64) -> Option<SwapInstructionsResponse> {
        let key = (sig, amount);
        // Fast path: read lock, check hot.
        {
            let g = self.inner.read().ok()?;
            if let Some(v) = g.hot.get(&key) {
                return Some(v.clone());
            }
            if !g.cold.contains_key(&key) {
                return None;
            }
        }
        // Cold hit: promote to hot under a write lock.
        let mut g = self.inner.write().ok()?;
        if let Some(v) = g.cold.remove(&key) {
            let out = v.clone();
            Self::insert_hot(&mut g, key, v);
            return Some(out);
        }
        // Someone else moved it; fall back to hot.
        g.hot.get(&key).cloned()
    }

    /// Inserts into the hot segment (always accepted — eviction is generational).
    /// Returns true if the key was not already present in either segment.
    pub fn set(&self, sig: u64, amount: u64, value: SwapInstructionsResponse) -> bool {
        let key = (sig, amount);
        let Ok(mut g) = self.inner.write() else {
            return false;
        };
        if g.hot.contains_key(&key) || g.cold.contains_key(&key) {
            return false;
        }
        Self::insert_hot(&mut g, key, value);
        true
    }

    /// Insert into hot; roll generations when hot is full.
    fn insert_hot(g: &mut Inner, key: (u64, u64), value: SwapInstructionsResponse) {
        if g.hot.len() >= SEG_CAP {
            // Age out: drop the old cold, promote hot to cold, start a fresh hot.
            g.cold = std::mem::take(&mut g.hot);
        }
        g.hot.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.inner
            .read()
            .map(|g| g.hot.len() + g.cold.len())
            .unwrap_or(0)
    }

    /// Load persisted entries from CACHE_FILE. Returns the number loaded.
    pub fn load_from_disk(&self) -> usize {
        let data = match std::fs::read_to_string(CACHE_FILE) {
            Ok(d) => d,
            Err(_) => return 0,
        };
        let entries: Vec<DiskEntry> = match serde_json::from_str(&data) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        let Ok(mut g) = self.inner.write() else {
            return 0;
        };
        for e in entries {
            Self::insert_hot(&mut g, (e.sig, e.amount), e.swap_ixs);
        }
        g.hot.len() + g.cold.len()
    }

    /// Serialize both segments to CACHE_FILE (atomic via temp + rename).
    fn flush_to_disk(&self) {
        let json = {
            let Ok(g) = self.inner.read() else {
                return;
            };
            let refs: Vec<DiskEntryRef> = g
                .hot
                .iter()
                .chain(g.cold.iter())
                .map(|((s, a), v)| DiskEntryRef {
                    sig: *s,
                    amount: *a,
                    swap_ixs: v,
                })
                .collect();
            match serde_json::to_string(&refs) {
                Ok(j) => j,
                Err(_) => return,
            }
        }; // read lock released before disk I/O

        if let Some(dir) = Path::new(CACHE_FILE).parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let tmp = format!("{CACHE_FILE}.tmp");
        if std::fs::write(&tmp, json.as_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp, CACHE_FILE);
        }
    }

    /// Spawn one background task that flushes to disk every `secs` seconds.
    pub fn spawn_flush_task(self: &Arc<Self>, secs: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                let c = this.clone();
                let _ = tokio::task::spawn_blocking(move || c.flush_to_disk()).await;
            }
        });
    }
}
