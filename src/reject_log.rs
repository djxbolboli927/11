//! Persistent log of simulation rejections.
//!
//! Every time the LiteSVM pre-flight gate drops a transaction for a reason
//! OTHER than slippage (Jupiter 6001 / below-floor), we append one JSON line to
//! a file in the project root describing WHY. The single most useful field is
//! `missing_accounts`: accounts the transaction references that are absent from
//! both the Yellowstone cache AND the loaded program set — i.e. accounts the
//! simulator had no data for, which is the usual cause of
//! `InvalidAccountOwner`, `RequireGtViolated (0/0)`, and Jupiter panics.
//!
//! Slippage rejections are intentionally NOT logged (they are expected and
//! high-volume).
//!
//! Format: JSON Lines (`.jsonl`) — one self-contained JSON object per line, so
//! the file can be tailed live and parsed with `jq`.

use solana_sdk::pubkey::Pubkey;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

pub struct RejectLog {
    file: Mutex<Option<File>>,
    /// Monotonic counter so every entry has a stable id even within the same ms.
    seq: AtomicU64,
}

impl RejectLog {
    /// Open (create/append) the reject log at `path` (relative paths land in the
    /// process working directory = project root). On failure, logging becomes a
    /// no-op rather than crashing the bot.
    pub fn new(path: &str) -> Self {
        match OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => {
                warn!(path, "sim reject log open — non-slippage drops will be recorded here");
                Self {
                    file: Mutex::new(Some(f)),
                    seq: AtomicU64::new(0),
                }
            }
            Err(e) => {
                warn!(path, error = %e, "could not open sim reject log; rejects won't be persisted");
                Self {
                    file: Mutex::new(None),
                    seq: AtomicU64::new(0),
                }
            }
        }
    }

    /// Append one non-slippage rejection. `error` is the Debug of the
    /// TransactionError; `logs` is the program log; `missing` are tx accounts
    /// absent from the simulator.
    pub fn record(
        &self,
        error: &str,
        logs: &[String],
        missing: &[Pubkey],
        total_accounts: usize,
        compute_units: u64,
    ) {
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

        // Innermost failing program = first "Program <id> failed ..." line.
        let failed_program = logs
            .iter()
            .find(|l| l.starts_with("Program ") && l.contains(" failed"))
            .and_then(|l| l.strip_prefix("Program "))
            .and_then(|r| r.split_whitespace().next())
            .unwrap_or("")
            .to_string();

        // Keep the last few log lines for context (full logs can be large).
        let tail: Vec<&String> = logs.iter().rev().take(6).collect::<Vec<_>>();
        let tail: Vec<&String> = tail.into_iter().rev().collect();

        let entry = serde_json::json!({
            "seq": seq,
            "ts_ms": ts_ms,
            "error": error,
            "failed_program": failed_program,
            "missing_count": missing.len(),
            "missing_accounts": missing.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "total_accounts": total_accounts,
            "compute_units": compute_units,
            "logs_tail": tail,
        });

        let _ = writeln!(f, "{entry}");
        let _ = f.flush();
    }
}
