//! Persistent, de-duplicated log of simulation rejections.
//!
//! Every time the LiteSVM pre-flight gate drops a transaction for a reason
//! OTHER than slippage (Jupiter 6001 / below-floor), we append one JSON line to
//! a file in the project root describing WHY — but only the FIRST time we see a
//! given (error, failing-program) combination, so the file stays small and
//! readable instead of repeating the same failure hundreds of times.
//!
//! Each entry carries, for every account the transaction touches, its owner /
//! lamports / data length AS SEEN BY THE SIMULATOR. This is the decisive
//! diagnostic: an account that should be an SPL token account but shows
//! `owner = 1111...1` (System) with `data_len = 0` was never loaded, and is the
//! cause of `InvalidAccountOwner`, `RequireGtViolated(0/0)`, or Jupiter panics.
//!
//! Slippage rejections are intentionally NOT logged (expected, high-volume).

use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

/// One account as seen by the simulator at injection time.
pub struct AcctView {
    pub pubkey: Pubkey,
    /// `None` => the account is absent from the SVM entirely (truly missing).
    pub owner: Option<Pubkey>,
    pub lamports: u64,
    pub data_len: usize,
    pub executable: bool,
}

/// An account whose state in the simulator differs from the live chain. This is
/// the decisive diagnostic for `InvalidAccountOwner` and similar: if the sim
/// loaded an account with a different owner / data length than mainnet, THAT is
/// the load bug. If nothing mismatches, the revert is the program's own logic
/// on the (correctly loaded) state — i.e. latency/state, not an account bug.
pub struct ChainMismatch {
    pub pubkey: Pubkey,
    pub sim_owner: Option<Pubkey>,
    pub chain_owner: Option<Pubkey>,
    pub sim_data_len: usize,
    pub chain_data_len: Option<usize>,
}

/// Derive the program id that failed from the program logs (the first
/// "Program <id> failed" line). Shared so the audit gate and the record use the
/// exact same signature for dedup.
pub fn failed_program_from_logs(logs: &[String]) -> String {
    logs.iter()
        .find(|l| l.starts_with("Program ") && l.contains(" failed"))
        .and_then(|l| l.strip_prefix("Program "))
        .and_then(|r| r.split_whitespace().next())
        .unwrap_or("")
        .to_string()
}

pub struct RejectLog {
    file: Mutex<Option<File>>,
    /// (error, failed_program) signatures already written — dedup so identical
    /// failures aren't repeated.
    seen: Mutex<HashSet<String>>,
    seq: AtomicU64,
}

impl RejectLog {
    pub fn new(path: &str) -> Self {
        match OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => {
                warn!(path, "sim reject log open — first occurrence of each failure recorded here");
                Self {
                    file: Mutex::new(Some(f)),
                    seen: Mutex::new(HashSet::new()),
                    seq: AtomicU64::new(0),
                }
            }
            Err(e) => {
                warn!(path, error = %e, "could not open sim reject log; rejects won't be persisted");
                Self {
                    file: Mutex::new(None),
                    seen: Mutex::new(HashSet::new()),
                    seq: AtomicU64::new(0),
                }
            }
        }
    }

    /// Has a rejection of this (error, failed_program) shape already been
    /// recorded? Lets the caller skip the expensive on-chain audit for repeats
    /// without consuming the dedup slot (record() still inserts it).
    pub fn seen_before(&self, error: &str, failed_program: &str) -> bool {
        let sig = format!("{error}|{failed_program}");
        self.seen.lock().unwrap().contains(&sig)
    }

    /// Append one non-slippage rejection (first occurrence only). `accounts` is
    /// the full account view at sim time; `missing` are accounts absent from the
    /// SVM (note: the Instructions sysvar always appears absent — it is
    /// synthesized per-transaction by LiteSVM and is NOT a real problem).
    /// `chain_mismatches` are accounts whose sim state differs from the live
    /// chain (empty unless this was the first occurrence and an audit ran).
    pub fn record(
        &self,
        error: &str,
        logs: &[String],
        accounts: &[AcctView],
        missing: &[Pubkey],
        chain_mismatches: &[ChainMismatch],
        compute_units: u64,
    ) {
        let failed_program = failed_program_from_logs(logs);

        // Dedup on (error, failing program): one rich line per distinct failure.
        let sig = format!("{error}|{failed_program}");
        {
            let mut seen = self.seen.lock().unwrap();
            if !seen.insert(sig) {
                return; // already recorded this failure shape
            }
        }

        let mut guard = self.file.lock().unwrap();
        let f = match guard.as_mut() {
            Some(f) => f,
            None => return,
        };

        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);

        let accounts_json: Vec<serde_json::Value> = accounts
            .iter()
            .map(|a| {
                serde_json::json!({
                    "pubkey": a.pubkey.to_string(),
                    "owner": a.owner.map(|o| o.to_string()),
                    "lamports": a.lamports,
                    "data_len": a.data_len,
                    "executable": a.executable,
                })
            })
            .collect();

        // Plain-language root-cause hint so the operator does not have to decode
        // Anchor/loader error numbers. Derived from the error and program logs.
        let joined = logs.join("\n");
        let hint = if error.contains("Custom(4100)") || joined.contains("DeclaredProgramIdMismatch") {
            format!(
                "WRONG .so binary for program {failed_program}: its embedded declare_id does \
                 not match the address it is loaded at. Re-dump it from mainnet with \
                 `solana program dump {failed_program} <FILE>.so` and register that exact file."
            )
        } else if error.contains("UnsupportedProgramId") {
            "A program the route CPIs into is NOT loaded in the simulator (missing or empty \
             filename in program_registry.rs). Provide its correct .so file."
                .to_string()
        } else if error.contains("InvalidAccountOwner") {
            if chain_mismatches.is_empty() {
                "InvalidAccountOwner, but EVERY account's owner/data in the sim matches the live \
                 chain (see chain_mismatches: none). So no account is loaded wrong — the program \
                 rejected on its own logic over correctly-loaded state (oracle/state/latency), \
                 NOT an account-loading bug."
                    .to_string()
            } else {
                format!(
                    "InvalidAccountOwner AND {} account(s) differ from the live chain — see \
                     chain_mismatches. Those are loaded wrong and are the real cause.",
                    chain_mismatches.len()
                )
            }
        } else if error.contains("ProgramFailedToComplete") {
            format!(
                "Program {failed_program} panicked (not a clean error). Usually an account it \
                 deserializes has unexpected/empty data, or a clock/oracle value it depends on \
                 is wrong. Inspect the per-account owner/data_len dump below."
            )
        } else {
            String::new()
        };

        let chain_mismatches_json: Vec<serde_json::Value> = chain_mismatches
            .iter()
            .map(|m| {
                serde_json::json!({
                    "pubkey": m.pubkey.to_string(),
                    "sim_owner": m.sim_owner.map(|o| o.to_string()),
                    "chain_owner": m.chain_owner.map(|o| o.to_string()),
                    "sim_data_len": m.sim_data_len,
                    "chain_data_len": m.chain_data_len,
                })
            })
            .collect();

        let entry = serde_json::json!({
            "seq": seq,
            "ts_ms": ts_ms,
            "error": error,
            "failed_program": failed_program,
            "diagnosis": hint,
            "compute_units": compute_units,
            "missing_count": missing.len(),
            "missing_accounts": missing.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "chain_mismatch_count": chain_mismatches.len(),
            "chain_mismatches": chain_mismatches_json,
            "accounts": accounts_json,
            "logs": logs,
        });

        let _ = writeln!(f, "{entry}");
        let _ = f.flush();
    }
}
