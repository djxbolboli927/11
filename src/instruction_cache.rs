use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

const MAX_ENTRIES: usize = 2_000;
const CACHE_FILE: &str = "/root/c/cache/routes/hot_routes.json";

/// In-RAM store for swap_instructions results, with optional disk persistence.
///
/// - Plain HashMap capped at MAX_ENTRIES (inserts stop when full).
/// - No TTL, no eviction churn.
/// - Persisted to CACHE_FILE: loaded once at startup, flushed every N seconds
///   by a single background task (one file write per interval — no per-request
///   I/O, so Metis is never affected).
///
/// Key: (route_signature, in_amount_lamports)
pub struct InstructionCache {
    map: RwLock<HashMap<(u64, u64), SwapInstructionsResponse>>,
}

/// Stable, amount-INDEPENDENT signature of a route_plan.
///
/// Only the structural identity of each hop is hashed (ammKey, mints, label,
/// feeMint, …) — the per-quote numeric fields (inAmount, outAmount, feeAmount)
/// are skipped so the same DEX path produces the SAME signature across scans
/// regardless of trade size or market movement. Without this, the signature
/// changed every scan and the cache never produced a hit.
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
                // Sort keys for a deterministic order independent of JSON layout.
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
            map: RwLock::new(HashMap::new()),
        })
    }

    /// Returns a cloned entry if present.
    pub fn get(&self, sig: u64, amount: u64) -> Option<SwapInstructionsResponse> {
        self.map.read().ok()?.get(&(sig, amount)).cloned()
    }

    /// Inserts if absent and under the size cap. Returns true if newly added.
    pub fn set(&self, sig: u64, amount: u64, value: SwapInstructionsResponse) -> bool {
        let Ok(mut m) = self.map.write() else {
            return false;
        };
        if m.len() >= MAX_ENTRIES {
            return false;
        }
        use std::collections::hash_map::Entry;
        match m.entry((sig, amount)) {
            Entry::Vacant(e) => {
                e.insert(value);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        self.map.read().map(|m| m.len()).unwrap_or(0)
    }

    /// Load persisted entries from CACHE_FILE. Returns the number loaded.
    /// Missing or unreadable file is treated as an empty cache (returns 0).
    pub fn load_from_disk(&self) -> usize {
        let data = match std::fs::read_to_string(CACHE_FILE) {
            Ok(d) => d,
            Err(_) => return 0,
        };
        let entries: Vec<DiskEntry> = match serde_json::from_str(&data) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        let Ok(mut m) = self.map.write() else {
            return 0;
        };
        for e in entries.into_iter().take(MAX_ENTRIES) {
            m.insert((e.sig, e.amount), e.swap_ixs);
        }
        m.len()
    }

    /// Serialize the current map to CACHE_FILE (atomic via temp + rename).
    fn flush_to_disk(&self) {
        let json = {
            let Ok(m) = self.map.read() else {
                return;
            };
            let refs: Vec<DiskEntryRef> = m
                .iter()
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
        }; // read lock released before any disk I/O

        if let Some(dir) = Path::new(CACHE_FILE).parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let tmp = format!("{CACHE_FILE}.tmp");
        if std::fs::write(&tmp, json.as_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp, CACHE_FILE);
        }
    }

    /// Spawn one background task that flushes to disk every `secs` seconds.
    /// A single periodic file write — no per-request I/O.
    pub fn spawn_flush_task(self: &Arc<Self>, secs: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                let c = this.clone();
                // Run the (small) file write off the async worker threads.
                let _ = tokio::task::spawn_blocking(move || c.flush_to_disk()).await;
            }
        });
    }
}
