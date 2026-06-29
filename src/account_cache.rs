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
use std::fs::OpenOptions;
use std::io::Write;
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
    /// Accounts handed to the background loader. An account is enqueued at most
    /// once (deduped here): the loader fetches it ONE time and caches it for the
    /// life of the process, so the per-transaction path never re-fetches.
    /// Removed once it lands in `inner` or is written off as bad.
    queued: Arc<DashSet<Pubkey>>,
    /// Sender feeding the background loader (`spawn_loader`).
    load_tx: mpsc::UnboundedSender<Pubkey>,
    /// Receiver half — taken (once) by `spawn_loader`.
    load_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Pubkey>>>>,
}

impl AccountCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        let (add_tx, add_rx) = mpsc::unbounded_channel();
        let (load_tx, load_rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(DashMap::with_capacity(4096)),
            rpc,
            stream_slot: Arc::new(AtomicU64::new(0)),
            subscribed: Arc::new(DashSet::new()),
            add_tx,
            add_rx: Arc::new(Mutex::new(Some(add_rx))),
            queued: Arc::new(DashSet::new()),
            load_tx,
            load_rx: Arc::new(Mutex::new(Some(load_rx))),
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

    /// Hand a set of accounts to the background loader (`spawn_loader`). Each
    /// account is enqueued AT MOST ONCE for the life of the process: ones
    /// already cached or already queued are skipped. This is non-blocking and
    /// safe to call on the hot path — it never touches the RPC itself, so a sim
    /// is never delayed by a fetch and the RPC is never hit per-transaction.
    /// The loader rate-limits, retries, and caches the result permanently.
    pub fn enqueue_load(&self, pubkeys: &[Pubkey]) {
        for pk in pubkeys {
            if self.inner.contains_key(pk) {
                continue;
            }
            // DashSet::insert returns true only the first time → enqueue once.
            if self.queued.insert(*pk) {
                let _ = self.load_tx.send(*pk);
            }
        }
    }

    /// Number of accounts still waiting to be loaded (queued or being retried).
    /// Startup waits on this so the bot only begins trading once its known
    /// accounts are in RAM.
    pub fn pending_loads(&self) -> usize {
        self.queued.len()
    }

    /// Spawn the single background account loader. It pulls pubkeys off the
    /// queue, fetches them with `getMultipleAccounts` at a fixed rate (well
    /// under the RPC's request budget), caches successes in RAM forever, and
    /// retries failures (RPC error OR account-not-found) up to `MAX_ATTEMPTS`
    /// times with a delay between tries. Accounts that still can't be loaded
    /// after all attempts are appended to `bad_accounts_path` and dropped, so a
    /// single 429 no longer means an account is lost — only a sustained failure
    /// across many seconds does, and that is recorded as genuinely unreachable.
    pub fn spawn_loader(&self, bad_accounts_path: String) {
        let mut rx = match self.load_rx.lock().unwrap().take() {
            Some(r) => r,
            None => return, // already spawned
        };
        let inner = self.inner.clone();
        let queued = self.queued.clone();
        let rpc = self.rpc.clone();

        tokio::spawn(async move {
            use std::collections::VecDeque;
            // Tunables. The free RPC allows ~20 requests/sec shared across the
            // whole bot; one getMultipleAccounts of BATCH keys is ONE request.
            // BATCH keys per request, one request per TICK → stays far under the
            // budget and leaves room for the blockhash fetcher. Raise the rate
            // (lower TICK_MS / raise BATCH) once on a higher-limit RPC.
            const BATCH: usize = 10;
            const TICK_MS: u64 = 1000; // 1 request/sec ⇒ ~10 accounts/sec
            const MAX_ATTEMPTS: u32 = 20;
            const RETRY_DELAY: Duration = Duration::from_secs(3);

            let mut bad_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&bad_accounts_path)
                .ok();

            let mut attempts: HashMap<Pubkey, u32> = HashMap::new();
            let mut ready: VecDeque<Pubkey> = VecDeque::new();
            let mut retry: VecDeque<(tokio::time::Instant, Pubkey)> = VecDeque::new();
            let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));

            loop {
                tokio::select! {
                    maybe = rx.recv() => {
                        match maybe {
                            Some(pk) => ready.push_back(pk),
                            None => return, // all senders dropped → shut down
                        }
                        while let Ok(pk) = rx.try_recv() {
                            ready.push_back(pk);
                        }
                    }
                    _ = ticker.tick() => {
                        // Promote any retries whose delay has elapsed.
                        let now = tokio::time::Instant::now();
                        while matches!(retry.front(), Some((when, _)) if *when <= now) {
                            let (_, pk) = retry.pop_front().unwrap();
                            ready.push_back(pk);
                        }
                        // Assemble one batch of not-yet-cached keys.
                        let mut batch: Vec<Pubkey> = Vec::with_capacity(BATCH);
                        while batch.len() < BATCH {
                            match ready.pop_front() {
                                Some(pk) => {
                                    if inner.contains_key(&pk) {
                                        queued.remove(&pk);
                                        attempts.remove(&pk);
                                        continue;
                                    }
                                    batch.push(pk);
                                }
                                None => break,
                            }
                        }
                        if batch.is_empty() {
                            continue;
                        }
                        // RpcClient is blocking → run it off the async worker.
                        let rpc2 = rpc.clone();
                        let keys = batch.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            rpc2.get_multiple_accounts(&keys)
                        })
                        .await;

                        // Per-account outcome: Some=loaded, None=needs retry.
                        let mut outcomes: Vec<(Pubkey, Option<Account>)> =
                            Vec::with_capacity(batch.len());
                        match result {
                            Ok(Ok(accts)) => {
                                for (pk, maybe) in batch.iter().zip(accts) {
                                    outcomes.push((
                                        *pk,
                                        maybe.map(|a| Account {
                                            lamports: a.lamports,
                                            data: a.data,
                                            owner: Address::from(a.owner.to_bytes()),
                                            executable: a.executable,
                                            rent_epoch: a.rent_epoch,
                                        }),
                                    ));
                                }
                            }
                            // Whole-batch failure (RPC error / 429 / join error):
                            // everything retries.
                            Ok(Err(e)) => {
                                debug!(error = %e, n = batch.len(), "account batch fetch failed (will retry)");
                                for pk in &batch {
                                    outcomes.push((*pk, None));
                                }
                            }
                            Err(e) => {
                                debug!(error = %e, "account fetch task join failed (will retry)");
                                for pk in &batch {
                                    outcomes.push((*pk, None));
                                }
                            }
                        }

                        for (pk, maybe) in outcomes {
                            match maybe {
                                Some(acct) => {
                                    inner.insert(pk, acct);
                                    queued.remove(&pk);
                                    attempts.remove(&pk);
                                }
                                None => {
                                    let n = attempts.entry(pk).or_insert(0);
                                    *n += 1;
                                    if *n >= MAX_ATTEMPTS {
                                        warn!(pubkey = %pk, attempts = *n, "account unreachable after max attempts → bad_accounts");
                                        if let Some(f) = bad_file.as_mut() {
                                            let _ = writeln!(f, "{pk}");
                                            let _ = f.flush();
                                        }
                                        queued.remove(&pk);
                                        attempts.remove(&pk);
                                    } else {
                                        retry.push_back((now + RETRY_DELAY, pk));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Diagnostic-only: fetch the CURRENT on-chain owner + data length of each
    /// account directly from RPC (bypassing the cache), so a sim revert can be
    /// compared against the truth on chain. Returns, per pubkey, `Some((owner,
    /// data_len))` if the account exists on chain, or `None` if it is absent.
    /// Accounts the RPC call fails for are simply omitted from the map. This is
    /// NOT on the hot path — it runs once per distinct failure shape.
    pub fn audit_fetch(&self, pubkeys: &[Pubkey]) -> HashMap<Pubkey, Option<(Pubkey, usize)>> {
        let mut out = HashMap::new();
        for chunk in pubkeys.chunks(100) {
            if let Ok(results) = self.rpc.get_multiple_accounts(chunk) {
                for (pk, maybe) in chunk.iter().zip(results) {
                    out.insert(
                        *pk,
                        maybe.map(|a| (Pubkey::new_from_array(a.owner.to_bytes()), a.data.len())),
                    );
                }
            }
        }
        out
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
