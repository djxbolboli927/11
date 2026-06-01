use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type RouteSignature = u64;

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
    /// True only when this entry was written (or refreshed) in the current
    /// process run. Entries loaded from disk start as `false` and are not
    /// served until Metis confirms them fresh for the first time.
    /// Never persisted to disk (always resets to false on load).
    #[serde(skip, default)]
    pub fresh: bool,
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
    /// has been confirmed fresh in the current process run (`fresh = true`).
    ///
    /// Entries loaded from disk at startup have `fresh = false` until Metis
    /// returns a successful response for them and `record()` is called.
    /// This prevents serving stale instructions whose pool-state data is hours
    /// old and would cause on-chain reverts.
    pub fn lookup(&self, sig: RouteSignature, amount: u64) -> Option<CachedRoute> {
        self.inner
            .read()
            .ok()?
            .entries
            .get(&(sig, amount))
            .filter(|r| r.fresh)
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
                    fresh: true, // confirmed by Metis in this process run
                },
            );
        } else if let Some(entry) = inner.entries.get_mut(&key) {
            entry.swap_ixs = swap_ixs;
            entry.hit_count += 1;
            entry.last_seen_ts = unix_now();
            entry.fresh = true; // re-confirmed fresh
        }
        inner.dirty = true;
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
                if let Err(e) = cache.flush_to_disk() {
                    eprintln!("[cache] flush error: {e}");
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
            serde_json::to_string_pretty(&route_map)?,
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
                serde_json::to_string_pretty(&map)?,
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
            serde_json::to_string_pretty(&by_sig)?,
        )?;

        Ok(())
    }

    /// Reload hot_routes.json into RAM on startup. Returns entries loaded.
    pub fn load_from_disk(&self) -> usize {
        let path = std::path::Path::new("/root/c/cache/routes/hot_routes.json");
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return 0,
        };
        // Key format: "<sig_hex>:<amount>"
        let raw: HashMap<String, CachedRoute> = match serde_json::from_str(&content) {
            Ok(p) => p,
            Err(_) => return 0,
        };
        let mut inner = self.inner.write().unwrap();
        for route in raw.into_values() {
            let sig = u64::from_str_radix(&route.sig, 16).unwrap_or(0);
            inner.entries.insert((sig, route.amount), route);
        }
        inner.entries.len()
    }
}

// ── Comparison helpers ────────────────────────────────────────────────────────

/// Full byte comparison: every account pubkey, every data byte, every setup instruction.
/// Returns `true` only when the cached and fresh instructions are completely identical.
pub fn instructions_match_bytewise(
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
    // base64 strings: equal strings ↔ equal decoded bytes.
    if ci.data != fi.data {
        return false;
    }
    if cached.setup_instructions.len() != fresh.setup_instructions.len() {
        return false;
    }
    for (cs, fs) in cached
        .setup_instructions
        .iter()
        .zip(fresh.setup_instructions.iter())
    {
        if cs.program_id != fs.program_id || cs.data != fs.data {
            return false;
        }
    }
    true
}

/// Human-readable description of what differs between cached and fresh instructions.
/// Used in the mismatch log so the cause is immediately visible.
pub fn diff_description(
    cached: &SwapInstructionsResponse,
    fresh: &SwapInstructionsResponse,
) -> String {
    let mut diffs = Vec::new();
    let ci = &cached.swap_instruction;
    let fi = &fresh.swap_instruction;

    if ci.program_id != fi.program_id {
        diffs.push(format!("program_id({} vs {})", ci.program_id, fi.program_id));
    }
    if ci.accounts.len() != fi.accounts.len() {
        diffs.push(format!(
            "account_count({} vs {})",
            ci.accounts.len(),
            fi.accounts.len()
        ));
    } else {
        let mismatched: Vec<usize> = ci
            .accounts
            .iter()
            .zip(fi.accounts.iter())
            .enumerate()
            .filter(|(_, (ca, fa))| ca.pubkey != fa.pubkey)
            .map(|(i, _)| i)
            .collect();
        if !mismatched.is_empty() {
            diffs.push(format!("account_pubkeys_at({mismatched:?})"));
        }
    }
    if ci.data != fi.data {
        diffs.push(format!(
            "swap_data_bytes(cached_len={} fresh_len={})",
            ci.data.len(),
            fi.data.len()
        ));
    }
    if cached.setup_instructions.len() != fresh.setup_instructions.len() {
        diffs.push(format!(
            "setup_count({} vs {})",
            cached.setup_instructions.len(),
            fresh.setup_instructions.len()
        ));
    }
    if diffs.is_empty() {
        "none_detected".to_string()
    } else {
        diffs.join("; ")
    }
}

/// Append one mismatch event to `/root/c/cache/mismatch_log.jsonl` (JSON Lines).
/// Best-effort: if the file can't be opened, the error is silently dropped.
pub fn append_mismatch_log(sig_hex: &str, amount: u64, dex_path: &[String], diff: &str) {
    let path = "/root/c/cache/mismatch_log.jsonl";
    let entry = serde_json::json!({
        "ts":       unix_now(),
        "sig":      sig_hex,
        "amount":   amount,
        "dex_path": dex_path,
        "diff":     diff,
    });
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{entry}");
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
