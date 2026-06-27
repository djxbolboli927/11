//! Static pool registry loaded from `pools.json`.
//!
//! Each entry describes one fixed pool we want to arbitrage:
//!
//! ```json
//! { "pubkey": "<pool>", "owner": "<dex program>", "params": { ... } }
//! ```
//!
//! `params` holds every account the pool's swap touches — vaults
//! (`tokenAccountA/B`), mints (`tokenmentA/B`), the ALT, plus DEX-specific
//! extras (oracle, authority, global, tickmap, vault sub-accounts, …). Rather
//! than enumerate each DEX's field names, we **recursively collect every string
//! value that parses as a base58 pubkey**. This guarantees that no account a
//! pool needs is ever missing from the simulator, regardless of DEX layout.
//!
//! All collected accounts are:
//!   1. pre-fetched once at startup (warm-up), and
//!   2. added to the Yellowstone gRPC subscription so their live state streams
//!      into the sim cache (vaults change every swap; static ones rarely do —
//!      subscribing both is harmless and removes any "is this fresh?" doubt).
//!
//! This complements the dynamic per-instruction subscription
//! (`AccountCache::ensure_subscribed`): the static file guarantees coverage and
//! warm-up for our known pools before the first trade, while dynamic
//! subscription catches anything new Metis returns at runtime.

use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use std::collections::BTreeSet;
use std::path::Path;
use std::str::FromStr;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct PoolEntry {
    pubkey: String,
    owner: String,
    #[serde(default)]
    params: serde_json::Value,
}

/// Result of loading `pools.json`.
pub struct Pools {
    /// Every unique account referenced by any pool (pool key, vaults, mints,
    /// oracle, ALT, authority, …). Used for both startup prefetch and the live
    /// gRPC subscription.
    pub accounts: Vec<Pubkey>,
    /// Distinct DEX program ids that own the configured pools. Useful to verify
    /// every owner is registered in `program_registry` (so its `.so` loads).
    pub owners: Vec<Pubkey>,
    /// Number of pools parsed.
    pub pool_count: usize,
}

impl Pools {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

/// Recursively walk a JSON value and insert every string that parses as a
/// base58 `Pubkey` into `out`. Numbers, names ("WSOL"), and non-pubkey strings
/// are skipped automatically because they fail `Pubkey::from_str`.
fn collect_pubkeys(value: &serde_json::Value, out: &mut BTreeSet<Pubkey>) {
    match value {
        serde_json::Value::String(s) => {
            if let Ok(pk) = Pubkey::from_str(s) {
                out.insert(pk);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_pubkeys(v, out);
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                collect_pubkeys(v, out);
            }
        }
        _ => {}
    }
}

/// Load `pools.json`. Returns an empty `Pools` (never errors) if the file is
/// missing or unparseable, so a missing file simply disables static warm-up
/// rather than crashing the bot.
pub fn load(path: &str) -> Pools {
    let empty = Pools {
        accounts: vec![],
        owners: vec![],
        pool_count: 0,
    };

    if !Path::new(path).exists() {
        info!(path, "pools.json not found — static pool warm-up disabled");
        return empty;
    }

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            warn!(path, error = %e, "cannot read pools.json");
            return empty;
        }
    };

    let entries: Vec<PoolEntry> = match serde_json::from_str(&content) {
        Ok(e) => e,
        Err(e) => {
            warn!(path, error = %e, "invalid pools.json");
            return empty;
        }
    };

    let mut accounts: BTreeSet<Pubkey> = BTreeSet::new();
    let mut owners: BTreeSet<Pubkey> = BTreeSet::new();
    let mut pool_count = 0usize;

    for entry in &entries {
        match Pubkey::from_str(&entry.pubkey) {
            Ok(pk) => {
                accounts.insert(pk);
            }
            Err(_) => {
                warn!(pubkey = %entry.pubkey, "invalid pool pubkey — skipped");
                continue;
            }
        }
        if let Ok(owner) = Pubkey::from_str(&entry.owner) {
            owners.insert(owner);
        }
        // Grab every account inside params (vaults, mints, oracle, ALT, …).
        collect_pubkeys(&entry.params, &mut accounts);
        pool_count += 1;
    }

    info!(
        path,
        pools = pool_count,
        accounts = accounts.len(),
        owners = owners.len(),
        "static pool registry loaded"
    );

    Pools {
        accounts: accounts.into_iter().collect(),
        owners: owners.into_iter().collect(),
        pool_count,
    }
}
