//! Local LiteSVM simulation gate.
//!
//! For every profitable opportunity Metis finds, we build the final
//! VersionedTransaction and hand it here BEFORE it reaches Jito. LiteSVM
//! runs the real on-chain bytecode (the .so files we loaded at startup)
//! against the freshest account state we have (the Yellowstone-fed
//! `AccountCache`) and reports:
//!   * did the tx succeed?
//!   * how much CU did it actually consume?
//!   * what is the user's WSOL balance AFTER execution?
//!
//! If simulation says the arb would net less than `min_acceptable_out`
//! WSOL (i.e. `amount + tip + base_fee`), we drop the send and save the
//! base fee. If simulation says success, we forward to Jito with confidence
//! that the code path works -- the only remaining risk is that the pool
//! state moved between our cached snapshot and the slot the tx lands in,
//! which no local simulator can eliminate.

use anyhow::{anyhow, Context, Result};
use litesvm::LiteSVM;
use solana_account::ReadableAccount;
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    clock::Clock,
    message::VersionedMessage,
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::account_cache::AccountCache;

pub struct SimOutcome {
    pub compute_units: u64,
    pub wsol_after: u64,
}

pub struct Simulator {
    /// LiteSVM is not Sync; a single Mutex serialises sim calls. Each sim is
    /// ~2-5ms so contention is a non-issue at our 5 bundles/sec rate.
    svm: Mutex<LiteSVM>,
    wsol_ata: Pubkey,
    fail_closed: bool,
}

impl Simulator {
    /// Load every program listed in `program_registry::PROGRAMS` from
    /// `so_dir`. Missing files are skipped with a warning so a partial
    /// mapping doesn't block startup.
    pub fn new(so_dir: &str, wsol_ata: Pubkey, fail_closed: bool) -> Result<Self> {
        let mut svm = LiteSVM::new()
            .with_sysvars()
            .with_precompiles()
            .with_sigverify(false)
            .with_blockhash_check(false)
            .with_spl_programs();

        // CRITICAL: realistic Clock is required for two independent reasons.
        //
        // 1. ALT validation: solana-address-lookup-table-interface only
        //    exposes addresses up to `last_extended_slot_start_index` when
        //    `current_slot <= last_extended_slot` (see state.rs:173-177).
        //    LiteSVM defaults to slot 0, smaller than any active mainnet
        //    ALT slot (~3.6e8), so V0 sanitization fails with
        //    InvalidLookupIndex for any index pointing past the ALT's
        //    creation point. Need slot >> last_extended_slot.
        //
        // 2. Oracle / TWAP / pool-staleness checks: many DEXes (Tessera,
        //    GoonFi, etc.) compare `clock.unix_timestamp` against an
        //    on-chain `last_update_ts` and refuse the swap if the gap is
        //    too large or negative. LiteSVM's default unix_timestamp is 0
        //    (= 1970), so EVERY pool looks "infinitely stale" and the
        //    program returns its proprietary error code (0xffff for
        //    Tessera, 0x15 for GoonFi -- both observed in production logs
        //    while the cache and pool state were already correct).
        //
        // Set unix_timestamp to wall-clock now (cheap, no RPC), and slot
        // to a value past every plausible ALT extension. We refresh
        // unix_timestamp on every simulate() call so the bot doesn't drift
        // out of an oracle's freshness window during long runs.
        const FUTURE_SLOT: u64 = 1_000_000_000_000;
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut clock = svm.get_sysvar::<Clock>();
        clock.slot = FUTURE_SLOT;
        clock.unix_timestamp = now_ts;
        clock.epoch_start_timestamp = now_ts;
        svm.set_sysvar::<Clock>(&clock);

        let mut loaded = 0usize;
        let mut missing = 0usize;
        for (pid_str, fname) in crate::program_registry::PROGRAMS {
            if fname.is_empty() {
                continue;
            }
            let path = format!("{}/{}", so_dir.trim_end_matches('/'), fname);
            if !Path::new(&path).exists() {
                warn!(path, "program .so not found, skipping");
                missing += 1;
                continue;
            }
            let pid = Pubkey::try_from(*pid_str)
                .map_err(|e| anyhow!("bad program id {pid_str}: {e:?}"))?;
            match svm.add_program_from_file(pid, &path) {
                Ok(()) => {
                    debug!(program = %pid, path, "program loaded");
                    loaded += 1;
                }
                Err(e) => {
                    warn!(program = %pid, path, error = ?e, "program load failed");
                    missing += 1;
                }
            }
        }
        info!(loaded, missing, "LiteSVM programs loaded");

        Ok(Self {
            svm: Mutex::new(svm),
            wsol_ata,
            fail_closed,
        })
    }

    /// Simulate `tx` against `cache`. Returns `Ok(SimOutcome)` if the tx
    /// would succeed AND leaves at least `min_acceptable_out` lamports in
    /// the user's WSOL ATA. `Err` otherwise -- caller should drop the send.
    ///
    /// `alts` is required to resolve lookup-table indexes into real pubkeys
    /// so every account the tx touches can be injected.
    pub fn simulate(
        &self,
        tx: &VersionedTransaction,
        alts: &[AddressLookupTableAccount],
        cache: &AccountCache,
        min_acceptable_out: u64,
    ) -> Result<SimOutcome> {
        // Collect every pubkey referenced by the tx (static keys + ALT entries).
        let accounts = collect_tx_accounts(tx, alts);

        // CRITICAL: Yellowstone's account subscription only streams UPDATES,
        // not an initial snapshot. Any DEX pool that hasn't traded since
        // bot startup is missing from the cache, and LiteSVM then rejects
        // the tx with InvalidAccountData (DEX side) or Jupiter custom 6025
        // (token-account side). Lazy-fetch every referenced account that
        // is not yet in cache via a single getMultipleAccounts call. After
        // the first sim that touches a given pool, future sims for that
        // pool hit the cache (and Yellowstone keeps the cached entry fresh
        // as updates flow in).
        cache.batch_fetch_missing(&accounts);

        let mut svm = self.svm.lock().unwrap();

        // Bump Clock.unix_timestamp to wall-clock now BEFORE injecting
        // anything else. Oracles inside DEX programs read this to gauge
        // pool freshness; if we leave it at the value set during startup,
        // after a few minutes pools start failing freshness checks and
        // emitting opaque custom errors. SystemTime::now() is sub-microsecond.
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut clock = svm.get_sysvar::<Clock>();
        if clock.unix_timestamp != now_ts {
            clock.unix_timestamp = now_ts;
            svm.set_sysvar::<Clock>(&clock);
        }

        // CRITICAL: the ALT accounts themselves must exist in LiteSVM state,
        // otherwise `simulate_transaction` fails at sanitization time while
        // resolving V0 lookup-table indexes (the error we saw as
        // "Transaction sanitization failed"). Fetch the raw on-chain ALT
        // account -- unfiltered, exactly as Solana runtime sees it -- via the
        // AccountCache: cache hit if Yellowstone already has it, otherwise a
        // one-time RPC fetch that is then memoized.
        for alt in alts {
            match cache.get_or_fetch(&alt.key) {
                Ok(raw) => {
                    if let Err(e) = svm.set_account(alt.key, raw) {
                        warn!(alt = %alt.key, error = ?e, "set_account(ALT) failed");
                    }
                }
                Err(e) => {
                    warn!(alt = %alt.key, error = %e, "ALT raw fetch failed");
                }
            }
        }

        // Inject whatever state we have. Accounts we don't know about keep
        // LiteSVM's default (empty). That is usually fine for read-only
        // sysvars / token program state we already preloaded.
        let mut injected = 0usize;
        let mut missing = 0usize;
        for pk in &accounts {
            match cache.get(pk) {
                Some(acct) => {
                    // Skip executable accounts. Two reasons:
                    //   1. Our DEX programs (and Jupiter v6) were already
                    //      loaded via `add_program_from_file` at startup,
                    //      which sets up the program-data side correctly.
                    //      Overwriting with `set_account` clobbers that
                    //      and the runtime then complains that the matching
                    //      program-data account is missing
                    //      (`Instruction(MissingAccount)` -> Custom(65535)).
                    //   2. Builtins (System, Token, ComputeBudget, ...) are
                    //      pre-registered by LiteSVM and must not be touched.
                    if acct.executable {
                        continue;
                    }
                    if let Err(e) = svm.set_account(*pk, acct) {
                        warn!(pubkey = %pk, error = ?e, "set_account failed");
                    } else {
                        injected += 1;
                    }
                }
                None => {
                    missing += 1;
                }
            }
        }
        debug!(injected, missing, accounts = accounts.len(), "sim prepared");

        // Read the pre-execution WSOL balance from the injected state so the
        // delta afterwards makes sense even if the ATA starts non-empty.
        let wsol_before = parse_wsol_amount(&svm, &self.wsol_ata);

        match svm.simulate_transaction(tx.clone()) {
            Ok(info) => {
                // `post_accounts` holds the final state. Pick out the WSOL ATA.
                let wsol_after = info
                    .post_accounts
                    .iter()
                    .find(|(pk, _)| *pk == self.wsol_ata)
                    .and_then(|(_, acc)| parse_token_amount(acc.data()))
                    .unwrap_or(wsol_before);

                let cu = info.meta.compute_units_consumed;
                if wsol_after < min_acceptable_out {
                    anyhow::bail!(
                        "sim unprofitable: wsol_after={} < min={}",
                        wsol_after,
                        min_acceptable_out
                    );
                }
                Ok(SimOutcome {
                    compute_units: cu,
                    wsol_after,
                })
            }
            Err(meta) => {
                // fail_closed: refuse to send. fail_open: allow the send so a
                // sim bug doesn't silently block every tx.
                if self.fail_closed {
                    anyhow::bail!(
                        "sim reverted: err={:?} logs={:?}",
                        meta.err,
                        meta.meta.logs.iter().rev().take(5).collect::<Vec<_>>()
                    );
                } else {
                    warn!(
                        err = ?meta.err,
                        logs = ?meta.meta.logs.iter().rev().take(3).collect::<Vec<_>>(),
                        "sim reverted but fail_open=true, allowing send"
                    );
                    Ok(SimOutcome {
                        compute_units: meta.meta.compute_units_consumed,
                        wsol_after: 0,
                    })
                }
            }
        }
    }
}

/// Best-effort extraction of the token amount from an SPL token account.
/// Layout (packed, 165 bytes): mint[0..32], owner[32..64], amount[64..72] le.
fn parse_token_amount(data: &[u8]) -> Option<u64> {
    if data.len() < 72 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[64..72]);
    Some(u64::from_le_bytes(buf))
}

fn parse_wsol_amount(svm: &LiteSVM, wsol_ata: &Pubkey) -> u64 {
    svm.get_account(wsol_ata)
        .and_then(|a| parse_token_amount(&a.data))
        .unwrap_or(0)
}

fn collect_tx_accounts(
    tx: &VersionedTransaction,
    alts: &[AddressLookupTableAccount],
) -> Vec<Pubkey> {
    let mut out: Vec<Pubkey> = tx.message.static_account_keys().to_vec();
    if let VersionedMessage::V0(v0) = &tx.message {
        for lookup in &v0.address_table_lookups {
            let alt = match alts.iter().find(|a| a.key == lookup.account_key) {
                Some(a) => a,
                None => continue,
            };
            for &idx in &lookup.writable_indexes {
                if let Some(addr) = alt.addresses.get(idx as usize) {
                    out.push(*addr);
                }
            }
            for &idx in &lookup.readonly_indexes {
                if let Some(addr) = alt.addresses.get(idx as usize) {
                    out.push(*addr);
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Convenience: resolve every ALT referenced by a Metis swap-instructions
/// response so the caller has a ready-to-use slice for `simulate`.
pub fn resolve_alts(
    alt_addresses: &[String],
    alt_cache: &crate::alt_cache::AltCache,
    rpc: &solana_client::rpc_client::RpcClient,
) -> Result<Vec<AddressLookupTableAccount>> {
    let mut out = Vec::with_capacity(alt_addresses.len());
    for s in alt_addresses {
        let pk = Pubkey::try_from(s.as_str())
            .map_err(|e| anyhow!("bad ALT pubkey {s}: {e:?}"))?;
        out.push(
            alt_cache
                .get_or_fetch(&pk, rpc)
                .context("ALT fetch for sim failed")?,
        );
    }
    Ok(out)
}
