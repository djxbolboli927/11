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

    /// Append one non-slippage rejection (first occurrence only). `accounts` is
    /// the full account view at sim time; `missing` are accounts absent from the
    /// SVM (note: the Instructions sysvar always appears absent — it is
    /// synthesized per-transaction by LiteSVM and is NOT a real problem).
    pub fn record(
        &self,
        error: &str,
        logs: &[String],
        accounts: &[AcctView],
        missing: &[Pubkey],
        compute_units: u64,
    ) {
        let failed_program = logs
            .iter()
            .find(|l| l.starts_with("Program ") && l.contains(" failed"))
            .and_then(|l| l.strip_prefix("Program "))
            .and_then(|r| r.split_whitespace().next())
            .unwrap_or("")
            .to_string();

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

        let entry = serde_json::json!({
            "seq": seq,
            "ts_ms": ts_ms,
            "error": error,
            "failed_program": failed_program,
            "compute_units": compute_units,
            "missing_count": missing.len(),
            "missing_accounts": missing.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "accounts": accounts_json,
            "logs": logs,
        });

        let _ = writeln!(f, "{entry}");
        let _ = f.flush();
    }
}
