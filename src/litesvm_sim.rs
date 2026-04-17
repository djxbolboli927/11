//! Local LiteSVM simulation gate — ZERO RPC on the hot path.
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
//!
//! DESIGN: simulate() does NOT make any RPC calls. Every account it needs
//! must already be in the AccountCache (fed by Yellowstone gRPC + startup
//! prefetch). If an account is missing, the sim fails fast — better a quick
//! rejection than a 20-50ms RPC round-trip that kills latency. The Clock
//! slot is also sourced from the Yellowstone stream, not from `getSlot`.

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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::account_cache::AccountCache;
use crate::metrics::Metrics;

pub struct SimOutcome {
    pub compute_units: u64,
    pub wsol_after: u64,
}

pub struct Simulator {
    svm: Mutex<LiteSVM>,
    wsol_ata: Pubkey,
    fail_closed: bool,
    /// Live mainnet slot, sourced from the Yellowstone gRPC stream
    /// (via AccountCache::stream_slot). Updated by every account message
    /// the stream delivers — NO RPC involved.
    current_slot: Arc<AtomicU64>,
}

impl Simulator {
    pub fn new(
        so_dir: &str,
        wsol_ata: Pubkey,
        fail_closed: bool,
        current_slot: Arc<AtomicU64>,
    ) -> Result<Self> {
        let mut svm = LiteSVM::new()
            .with_sysvars()
            .with_precompiles()
            .with_sigverify(false)
            .with_blockhash_check(false)
            .with_spl_programs();

        let initial_slot = current_slot.load(Ordering::Relaxed);
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut clock = svm.get_sysvar::<Clock>();
        clock.slot = initial_slot;
        clock.unix_timestamp = now_ts;
        clock.epoch_start_timestamp = now_ts;
        svm.set_sysvar::<Clock>(&clock);
        debug!(initial_slot, "sim Clock initialised");

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
    /// the user's WSOL ATA. `Err` otherwise — caller should drop the send.
    ///
    /// **ZERO RPC calls.** Every account is read from the Yellowstone-fed
    /// cache. Missing accounts cause the sim to fail fast (better than
    /// adding 20-50ms RPC latency). The Clock slot comes from the
    /// Yellowstone stream, not from `getSlot`.
    pub fn simulate(
        &self,
        tx: &VersionedTransaction,
        alts: &[AddressLookupTableAccount],
        cache: &AccountCache,
        min_acceptable_out: u64,
        metrics: &Metrics,
    ) -> Result<SimOutcome> {
        metrics.sim_executed.fetch_add(1, Ordering::Relaxed);

        let accounts = collect_tx_accounts(tx, alts);

        let mut svm = self.svm.lock().unwrap();

        // Bump Clock from the Yellowstone stream slot (zero RPC).
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

        // Inject ALT accounts from the resolved alts vector.
        // These were fetched via alt_cache.get_or_fetch() before simulate() was called,
        // so we have the full AddressLookupTableAccount with addresses populated.
        // We need to serialize them properly for LiteSVM.
        for alt in alts {
            // Build the account data buffer for the ALT matching Solana's layout
            // Layout: [deactivation_slot: 8][last_extended_slot: 8][last_extended_slot_start_epoch: 8][authority: 32 or 0][addresses: ...]
            // Total header size = 8 + 8 + 8 + 32 = 56 bytes (must match deserialize_alt_addresses in transaction.rs)
            let mut data = Vec::with_capacity(56 + alt.addresses.len() * 32);
            
            // Deactivation slot (u64::MAX means active)
            data.extend_from_slice(&u64::MAX.to_le_bytes());
            
            // Last extended slot (u64)
            data.extend_from_slice(&0u64.to_le_bytes());
            
            // Last extended slot start epoch (u64) 
            data.extend_from_slice(&0u64.to_le_bytes());
            
            // Authority (Pubkey or empty if none) - using 32 bytes of zeros for no authority
            data.extend_from_slice(&[0u8; 32]);
            
            // All the addresses in the table
            for addr in &alt.addresses {
                data.extend_from_slice(addr.as_ref());
            }
            
            let raw_account = solana_sdk::account::Account {
                lamports: 1000000, // Minimum rent-exempt balance
                data,
                owner: solana_sdk::address_lookup_table::program::id(),
                executable: false,
                rent_epoch: u64::MAX,
            };
            
            if let Err(e) = svm.set_account(alt.key, raw_account) {
                warn!(alt = %alt.key, error = ?e, "set_account(ALT) failed");
            } else {
                debug!(alt = %alt.key, addrs = alt.addresses.len(), "ALT injected into sim");
            }
        }

        // Inject whatever state we have from cache. No RPC fallback.
        let mut injected = 0usize;
        let mut missing = 0usize;
        for pk in &accounts {
            match cache.get(pk) {
                Some(acct) => {
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

        let wsol_before = parse_wsol_amount(&svm, &self.wsol_ata);

        match svm.simulate_transaction(tx.clone()) {
            Ok(info) => {
                let wsol_after = info
                    .post_accounts
                    .iter()
                    .find(|(pk, _)| *pk == self.wsol_ata)
                    .and_then(|(_, acc)| parse_token_amount(acc.data()))
                    .unwrap_or(wsol_before);

                let cu = info.meta.compute_units_consumed;
                if wsol_after < min_acceptable_out {
                    metrics.sim_slippage_rejected.fetch_add(1, Ordering::Relaxed);
                    anyhow::bail!(
                        "sim unprofitable: wsol_after={} < min={}",
                        wsol_after,
                        min_acceptable_out
                    );
                }
                metrics.sim_passed.fetch_add(1, Ordering::Relaxed);
                Ok(SimOutcome {
                    compute_units: cu,
                    wsol_after,
                })
            }
            Err(meta) => {
                metrics.sim_revert_rejected.fetch_add(1, Ordering::Relaxed);
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
                    metrics.sim_passed.fetch_add(1, Ordering::Relaxed);
                    Ok(SimOutcome {
                        compute_units: meta.meta.compute_units_consumed,
                        wsol_after: 0,
                    })
                }
            }
        }
    }
}

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

pub struct SimulatorPool {
    sims: Vec<Arc<Simulator>>,
    next: AtomicUsize,
}

impl SimulatorPool {
    /// Build `workers` independent Simulators. NO RPC calls — the slot
    /// comes from the Yellowstone-fed `current_slot` atomic.
    pub fn new(
        workers: usize,
        so_dir: &str,
        wsol_ata: Pubkey,
        fail_closed: bool,
        current_slot: Arc<AtomicU64>,
    ) -> Result<Self> {
        let workers = workers.max(1);
        let mut sims = Vec::with_capacity(workers);
        for i in 0..workers {
            let sim = Simulator::new(so_dir, wsol_ata, fail_closed, current_slot.clone())
                .with_context(|| format!("failed to build sim worker #{i}"))?;
            sims.push(Arc::new(sim));
            info!(worker = i, "sim worker initialised");
        }
        info!(workers, "SimulatorPool ready");
        Ok(Self {
            sims,
            next: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn acquire(&self) -> Arc<Simulator> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.sims.len();
        self.sims[idx].clone()
    }
}

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
