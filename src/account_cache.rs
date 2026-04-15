//! Hot account cache fed by a Yellowstone gRPC subscription.
//!
//! This is the same data Metis consumes. By keeping a parallel copy in our
//! own process we can hand it to LiteSVM for pre-flight simulation without
//! any RPC round-trip on the hot path (a getMultipleAccounts would add
//! 20–50ms and make simulation useless).
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
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use solana_account::Account;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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
}

impl AccountCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity(4096)),
            rpc,
        }
    }

    /// Fast path: read from the hot cache. Returns None if not yet populated.
    #[inline]
    pub fn get(&self, pubkey: &Pubkey) -> Option<Account> {
        self.inner.get(pubkey).map(|v| v.value().clone())
    }

    /// Slow path used only during startup warm-up and for rarely-changing
    /// accounts (token mints) that aren't streamed over Yellowstone because
    /// they are owned by the SPL Token program, not a DEX.
    pub fn get_or_fetch(&self, pubkey: &Pubkey) -> Result<Account> {
        if let Some(a) = self.get(pubkey) {
            return Ok(a);
        }
        let acct = self
            .rpc
            .get_account(pubkey)
            .with_context(|| format!("RPC fetch of {pubkey} failed"))?;
        // Convert solana_sdk::Account -> solana_account::Account via field copy.
        let account = Account {
            lamports: acct.lamports,
            data: acct.data,
            owner: acct.owner,
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
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_stream(
                    &endpoint,
                    &x_token,
                    &dex_program_ids,
                    &extra_accounts,
                    &cache,
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

async fn run_stream(
    endpoint: &str,
    x_token: &str,
    dex_program_ids: &[String],
    extra_accounts: &[Pubkey],
    cache: &Arc<DashMap<Pubkey, Account>>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(x_token.to_string()))?
        .tls_config(yellowstone_grpc_client::ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await
        .context("gRPC connect failed")?;

    info!(endpoint, "gRPC connected");

    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> =
        HashMap::new();

    accounts_filter.insert(
        "dex_pools".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: dex_program_ids.to_vec(),
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    if !extra_accounts.is_empty() {
        accounts_filter.insert(
            "extras".to_string(),
            SubscribeRequestFilterAccounts {
                account: extra_accounts.iter().map(|p| p.to_string()).collect(),
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );
    }

    let request = SubscribeRequest {
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
    };

    let (mut tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .context("gRPC subscribe failed")?;

    info!("gRPC subscription active; waiting for account updates");

    let mut count: u64 = 0;
    while let Some(msg) = stream.next().await {
        let msg = msg.context("stream yielded error")?;
        match msg.update_oneof {
            Some(UpdateOneof::Account(a)) => {
                if let Some(info) = a.account {
                    let pk = match Pubkey::try_from(info.pubkey.as_slice()) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let owner = Pubkey::try_from(info.owner.as_slice())
                        .unwrap_or_default();
                    let account = Account {
                        lamports: info.lamports,
                        data: info.data,
                        owner,
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
                // Keep the channel alive.
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
    Ok(())
}
