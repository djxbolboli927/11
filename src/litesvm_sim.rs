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
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    clock::Clock,
    message::VersionedMessage,
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
    /// Live mainnet slot, refreshed by a background tokio task every second.
    /// Read on every `simulate()` and pushed into LiteSVM's Clock so that
    /// PMM-style DEXes (Tessera, GoonFi, SolFi, ZeroFi) which validate
    /// `current_slot - oracle.last_update_slot < MAX_AGE_SLOTS` see a slot
    /// close to mainnet's. Keeping a stale slot here is the same class of
    /// failure as keeping a stale unix_timestamp.
    current_slot: Arc<AtomicU64>,
}

impl Simulator {
    /// Load every program listed in `program_registry::PROGRAMS` from
    /// `so_dir`. Missing files are skipped with a warning so a partial
    /// mapping doesn't block startup.
    pub fn new(
        so_dir: &str,
        wsol_ata: Pubkey,
        fail_closed: bool,
        rpc: Arc<RpcClient>,
    ) -> Result<Self> {
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
        //    Need slot > last_extended_slot. Real mainnet slot is always
        //    past every ALT's extension slot (an ALT can only have been
        //    extended at a *past* slot), so live slot satisfies this.
        //
        // 2. Pool / quote freshness: PMM-style DEXes (Tessera, GoonFi,
        //    SolFi, ZeroFi) validate `current_slot - last_update_slot <
        //    MAX_AGE_SLOTS` (typically 100-200 slots, ~40-80s). They fail
        //    SILENTLY in their entrypoint with proprietary error codes
        //    (0xffff Tessera, 0x15 GoonFi, ...) before ever calling msg!,
        //    which is exactly what we observe. Setting slot to a fake
        //    "future" value (e.g. 1e12) makes EVERY quote look ancient
        //    and triggers the same failure.
        //
        // Fetch live mainnet slot once at startup, then keep it fresh via
        // a tokio background task (similar to BlockhashCache). Each
        // simulate() reads the atomic and pushes it into Clock.slot.
        // unix_timestamp gets the same treatment using SystemTime::now().
        let initial_slot = rpc
            .get_slot()
            .context("initial RPC get_slot for sim Clock failed")?;
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut clock = svm.get_sysvar::<Clock>();
        clock.slot = initial_slot;
        clock.unix_timestamp = now_ts;
        clock.epoch_start_timestamp = now_ts;
        svm.set_sysvar::<Clock>(&clock);
        info!(initial_slot, "sim Clock initialised with live mainnet slot");

        let current_slot = Arc::new(AtomicU64::new(initial_slot));
        let slot_for_task = current_slot.clone();
        let rpc_for_task = rpc.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                let rpc_inner = rpc_for_task.clone();
                let res = tokio::task::spawn_blocking(move || rpc_inner.get_slot()).await;
                match res {
                    Ok(Ok(s)) => {
                        slot_for_task.store(s, Ordering::Relaxed);
                    }
                    Ok(Err(e)) => warn!(error = %e, "slot RPC refresh failed"),
                    Err(e) => warn!(error = %e, "slot refresh task panicked"),
                }
            }
        });

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
            current_slot,
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

        // Bump Clock.{slot,unix_timestamp} to live values BEFORE injecting
        // accounts. PMM-style DEXes (Tessera, GoonFi, SolFi, ZeroFi) check
        // both: `current_slot - quote.last_slot < MAX_AGE_SLOTS` AND
        // `unix_timestamp - quote.last_ts < MAX_AGE_SECS`. If either is
        // stale they fail their entrypoint with proprietary error codes
        // BEFORE emitting any msg!, which matches the production logs:
        // Tessera fails the moment Jupiter `invoke [2]`s into it.
        let live_slot = self.current_slot.load(Ordering::Relaxed);
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut clock = svm.get_sysvar::<Clock>();
        let mut clock_dirty = false;
        if clock.slot != live_slot {
            clock.slot = live_slot;
            clock_dirty = true;
        }
        if clock.unix_timestamp != now_ts {
            clock.unix_timestamp = now_ts;
            clock_dirty = true;
        }
        if clock_dirty {
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
                //
                // Capture FULL log stream (no truncation, no reverse) on
                // failure so the operator can see exactly what each program
                // emitted before erroring. Custom error codes alone don't
                // tell us if the issue is oracle staleness, owner mismatch,
                // signature check, etc.; the program's own `msg!` lines do.
                if self.fail_closed {
                    anyhow::bail!(
                        "sim reverted: err={:?} logs={:#?}",
                        meta.err,
                        meta.meta.logs
                    );
                } else {
                    warn!(
                        err = ?meta.err,
                        logs = ?meta.meta.logs,
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
