//! Hot account cache fed by a Yellowstone gRPC subscription.
//!
//! This is the same data Metis consumes. By keeping a parallel copy in our
//! own process we can hand it to LiteSVM for pre-flight simulation without
//! any RPC round-trip on the hot path (a getMultipleAccounts would add
//! 20-50ms and make simulation useless).
//!
//! The cache subscribes once at startup with two filter entries:
//!   1. all DEX program ids from `program_registry::PROGRAMS` (owner filter)
//!      -> every pool account owned by those programs streams in
//!   2. the user's WSOL ATA (specific account filter)
//!      -> so the simulated tx can read / debit it
//!
//! Missing entries (token mints, intermediate ATAs) are fetched lazily from
//! RPC the first time they're needed and then cached forever (their data
//! rarely changes).

use anyhow::{Context, Result};
use dashmap::{DashMap, DashSet};
use futures::{SinkExt, StreamExt};
use solana_account::Account;
use solana_address::Address;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestPing,
};

/// Shared concurrent cache. Cloning an `AccountCache` is cheap; it's just
/// an Arc-wrapped DashMap plus an Arc-wrapped RpcClient for fallbacks.
#[derive(Clone)]
pub struct AccountCache {
    inner: Arc<DashMap<Pubkey, Account>>,
    rpc: Arc<RpcClient>,
    /// Slot of the most recent Yellowstone account update. The simulator
    /// reads this to set LiteSVM's Clock.slot — no RPC call needed.
    stream_slot: Arc<AtomicU64>,
    /// Accounts added to the live "extras" subscription at runtime (vaults
    /// discovered from Metis swap instructions). Used to dedup and to rebuild
    /// the subscription request after a reconnect.
    subscribed: Arc<DashSet<Pubkey>>,
    /// Sender used by `ensure_subscribed` to push newly-seen accounts to the
    /// running subscription task so it can update the Yellowstone filter.
    add_tx: mpsc::UnboundedSender<Pubkey>,
    /// Receiver half — taken (once) by `spawn_subscription`.
    add_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Pubkey>>>>,
}

impl AccountCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        let (add_tx, add_rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(DashMap::with_capacity(4096)),
            rpc,
            stream_slot: Arc::new(AtomicU64::new(0)),
            subscribed: Arc::new(DashSet::new()),
            add_tx,
            add_rx: Arc::new(Mutex::new(Some(add_rx))),
        }
    }

    /// Register accounts for live Yellowstone updates. Vault token accounts
    /// (owned by SPL Token, not a DEX program) are NOT caught by the owner
    /// filter and change on every swap, so each Metis swap instruction's
    /// writable accounts are registered here. New (not-yet-subscribed) keys
    /// are pushed to the subscription task, which folds them into the gRPC
    /// account filter. Already-subscribed keys are ignored (cheap dedup).
    pub fn ensure_subscribed(&self, pubkeys: &[Pubkey]) {
        for pk in pubkeys {
            // DashSet::insert returns true if the value was newly inserted.
            if self.subscribed.insert(*pk) {
                // Unbounded send never blocks; ignore error if task is gone.
                let _ = self.add_tx.send(*pk);
            }
        }
    }

    /// The latest slot seen from the Yellowstone stream. The sim pool reads
    /// this instead of making its own `get_slot` RPC.
    pub fn stream_slot(&self) -> Arc<AtomicU64> {
        self.stream_slot.clone()
    }

    /// Seed the stream slot with an initial value (from RPC at startup)
    /// so sims have a valid Clock.slot before the first Yellowstone message.
    pub fn seed_slot(&self, slot: u64) {
        self.stream_slot.store(slot, Ordering::Relaxed);
    }

    /// Fast path: read from the hot cache. Returns None if not yet populated.
    #[inline]
    pub fn get(&self, pubkey: &Pubkey) -> Option<Account> {
        self.inner.get(pubkey).map(|v| v.value().clone())
    }

    /// Slow path used only during startup warm-up and for rarely-changing
    /// accounts (token mints, ALTs) that aren't streamed over Yellowstone.
    pub fn get_or_fetch(&self, pubkey: &Pubkey) -> Result<Account> {
        if let Some(a) = self.get(pubkey) {
            return Ok(a);
        }
        let acct = self
            .rpc
            .get_account(pubkey)
            .with_context(|| format!("RPC fetch of {pubkey} failed"))?;
        let account = Account {
            lamports: acct.lamports,
            data: acct.data,
            owner: Address::from(acct.owner.to_bytes()),
            executable: acct.executable,
            rent_epoch: acct.rent_epoch,
        };
        self.inner.insert(*pubkey, account.clone());
        Ok(account)
    }

    /// Pre-fetch a batch of accounts (used at startup to warm up mints, ATAs,
    /// etc. that won't naturally stream in via the owner filter).
    pub fn prefetch(&self, pubkeys: &[Pubkey]) {
        for pk in pubkeys {
            if let Err(e) = self.get_or_fetch(pk) {
                warn!(pubkey = %pk, error = %e, "prefetch miss");
            }
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Spawn the Yellowstone subscription task. Reconnects with exponential
    /// backoff if the stream drops.
    pub fn spawn_subscription(
        &self,
        endpoint: String,
        x_token: String,
        dex_program_ids: Vec<String>,
        extra_accounts: Vec<Pubkey>,
    ) {
        let cache = self.inner.clone();
        let stream_slot = self.stream_slot.clone();
        let subscribed = self.subscribed.clone();
        // Take the receiver; spawn_subscription is called once at startup.
        let mut add_rx = self
            .add_rx
            .lock()
            .unwrap()
            .take()
            .expect("spawn_subscription called more than once");
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(
                    &endpoint,
                    &x_token,
                    &dex_program_ids,
                    &extra_accounts,
                    &subscribed,
                    &mut add_rx,
                    &cache,
                    &stream_slot,
                )
                .await
                {
                    Ok(()) => {
                        warn!("gRPC account stream ended cleanly, reconnecting");
                    }
                    Err(e) => {
                        warn!(error = %e, "gRPC account stream error, reconnecting");
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        });
    }
}

/// Build the gRPC subscription request. The "extras" account filter is the
/// union of the static startup accounts and every dynamically-registered
/// account (vaults discovered from Metis swap instructions). Rebuilt on each
/// (re)connect and whenever new accounts are registered so the full set always
/// streams.
fn build_request(
    dex_program_ids: &[String],
    extra_static: &[Pubkey],
    subscribed: &DashSet<Pubkey>,
) -> SubscribeRequest {
    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();

    accounts_filter.insert(
        "dex_pools".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: dex_program_ids.to_vec(),
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    // Union of static extras + dynamically registered vaults, deduplicated.
    let mut extras: Vec<String> = extra_static.iter().map(|p| p.to_string()).collect();
    extras.extend(subscribed.iter().map(|p| p.to_string()));
    extras.sort_unstable();
    extras.dedup();

    if !extras.is_empty() {
        accounts_filter.insert(
            "extras".to_string(),
            SubscribeRequestFilterAccounts {
                account: extras,
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );
    }

    SubscribeRequest {
        slots: HashMap::new(),
        accounts: accounts_filter,
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_stream(
    endpoint: &str,
    x_token: &str,
    dex_program_ids: &[String],
    extra_accounts: &[Pubkey],
    subscribed: &Arc<DashSet<Pubkey>>,
    add_rx: &mut mpsc::UnboundedReceiver<Pubkey>,
    cache: &Arc<DashMap<Pubkey, Account>>,
    stream_slot: &Arc<AtomicU64>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(x_token.to_string()))?
        .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("gRPC connect failed")?;

    info!(endpoint, "gRPC connected");

    // Initial request includes the static extras AND any vaults registered
    // before (or during a previous connection of) this stream.
    let request = build_request(dex_program_ids, extra_accounts, subscribed);

    let (mut tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("gRPC subscribe failed")?;

    info!(
        extras = subscribed.len(),
        "gRPC subscription active; waiting for account updates"
    );

    let mut count: u64 = 0;
    loop {
        tokio::select! {
            // Bias toward draining account updates first.
            biased;

            maybe_msg = stream.next() => {
                let msg = match maybe_msg {
                    Some(m) => m.context("stream yielded error")?,
                    None => return Ok(()), // stream ended → reconnect
                };
                match msg.update_oneof {
                    Some(UpdateOneof::Account(a)) => {
                        stream_slot.store(a.slot, Ordering::Relaxed);

                        if let Some(info) = a.account {
                            let pk = match Pubkey::try_from(info.pubkey.as_slice()) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            let owner_bytes: [u8; 32] = info.owner.as_slice()
                                .try_into()
                                .unwrap_or([0u8; 32]);
                            let account = Account {
                                lamports: info.lamports,
                                data: info.data,
                                owner: Address::from(owner_bytes),
                                executable: info.executable,
                                rent_epoch: info.rent_epoch,
                            };
                            cache.insert(pk, account);
                            count += 1;
                            if count % 10_000 == 0 {
                                debug!(count, size = cache.len(), "cache growth");
                            }
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        let _ = tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                    }
                    _ => {}
                }
            }

            // A newly-discovered vault account was registered. Drain any others
            // that are queued and resubscribe ONCE with the full account set so
            // we don't spam the server with one update per account.
            maybe_pk = add_rx.recv() => {
                match maybe_pk {
                    Some(_) => {
                        // Drain the rest of the burst (keys are already in the
                        // `subscribed` set; we only need to coalesce the wakeups).
                        while add_rx.try_recv().is_ok() {}
                        let req = build_request(dex_program_ids, extra_accounts, subscribed);
                        if let Err(e) = tx.send(req).await {
                            warn!(error = %e, "failed to push updated subscription, reconnecting");
                            return Ok(());
                        }
                        debug!(extras = subscribed.len(), "subscription updated with new vaults");
                    }
                    None => {
                        // Sender dropped (cache gone) — nothing more to do, but
                        // keep streaming existing accounts.
                    }
                }
            }
        }
    }
}
