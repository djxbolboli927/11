//! Pool account registry.
//!
//! Reads `<dex_dir>/<DEX_NAME>/<pool>.toml` files at startup and returns:
//!   - `all_accounts` — every static pubkey to pre-fetch via RPC so the sim
//!     cache is fully populated before the first trade
//!   - `subscribe_accounts` — vault accounts that change on every swap and
//!     must be subscribed individually on Yellowstone for live updates
//!     (the Yellowstone owner-filter already covers accounts owned by DEX
//!     programs; vaults are owned by SPL Token and need a direct subscription)
//!
//! Directory layout:
//!   dex_dir/
//!     Goonfi_V2/
//!       hype_usdc.toml
//!       wsol_usdc.toml
//!     SomeOtherDex/
//!       ...
//!
//! Files starting with `_` (e.g. `_template.toml`) are skipped.

use anyhow::Result;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use std::path::Path;
use tracing::{info, warn};

#[derive(Deserialize)]
struct PoolFile {
    name: Option<String>,
    market: Option<String>,
    vault_a: Option<String>,
    vault_b: Option<String>,
    mint_a: Option<String>,
    mint_b: Option<String>,
    #[serde(default)]
    extra: Vec<String>,
}

pub struct DexPools {
    /// Every static account to pre-fetch via RPC at startup.
    pub all_accounts: Vec<Pubkey>,
    /// Vault accounts to subscribe for live Yellowstone updates.
    /// These change on every swap and must be kept fresh.
    pub subscribe_accounts: Vec<Pubkey>,
}

/// Load all pool files from `dex_dir/<DEX>/<pool>.toml`.
/// Returns an empty `DexPools` if the directory does not exist.
pub fn load(dex_dir: &str) -> DexPools {
    let dex_path = Path::new(dex_dir);
    if !dex_path.exists() {
        return DexPools {
            all_accounts: vec![],
            subscribe_accounts: vec![],
        };
    }

    let mut all: Vec<Pubkey> = Vec::new();
    let mut subs: Vec<Pubkey> = Vec::new();
    let mut pool_count = 0usize;

    let dex_entries = match std::fs::read_dir(dex_path) {
        Ok(e) => e,
        Err(e) => {
            warn!(dex_dir, error = %e, "cannot read dex_dir");
            return DexPools { all_accounts: all, subscribe_accounts: subs };
        }
    };

    for dex_entry in dex_entries.flatten() {
        if !dex_entry.file_type().map_or(false, |t| t.is_dir()) {
            continue;
        }
        let dex_name = dex_entry.file_name();
        let pool_entries = match std::fs::read_dir(dex_entry.path()) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for pool_entry in pool_entries.flatten() {
            let path = pool_entry.path();
            // Skip template files and non-TOML files
            let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if fname.starts_with('_') || path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }

            let content = match std::fs::read_to_string(&path) {
                Ok(s) => s,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "cannot read pool file");
                    continue;
                }
            };

            let pool: PoolFile = match toml::from_str(&content) {
                Ok(p) => p,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "invalid pool TOML");
                    continue;
                }
            };

            let pool_name = pool.name.as_deref().unwrap_or(fname);
            let mut parsed = 0usize;

            let mut push = |s: Option<&str>, is_vault: bool| {
                let s = match s { Some(s) => s, None => return };
                match Pubkey::try_from(s) {
                    Ok(pk) => {
                        all.push(pk);
                        if is_vault { subs.push(pk); }
                        parsed += 1;
                    }
                    Err(_) => warn!(
                        dex = ?dex_name, pool = pool_name, addr = s,
                        "invalid pubkey in pool file — skipped"
                    ),
                }
            };

            push(pool.market.as_deref(), false);
            push(pool.vault_a.as_deref(), true);   // vault → live subscription
            push(pool.vault_b.as_deref(), true);   // vault → live subscription
            push(pool.mint_a.as_deref(), false);
            push(pool.mint_b.as_deref(), false);
            for addr in &pool.extra {
                push(Some(addr.as_str()), false);
            }

            info!(
                dex = ?dex_name, pool = pool_name,
                accounts = parsed,
                "pool loaded"
            );
            pool_count += 1;
        }
    }

    // Deduplicate — mints and global protocol accounts appear across pools
    all.sort_unstable();
    all.dedup();
    subs.sort_unstable();
    subs.dedup();

    info!(
        pools = pool_count,
        prefetch = all.len(),
        live_subs = subs.len(),
        "dex pool registry loaded"
    );

    DexPools { all_accounts: all, subscribe_accounts: subs }
}
