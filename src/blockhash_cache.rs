use solana_client::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};
use tracing::{debug, warn};

pub struct BlockhashCache {
    inner: Arc<Mutex<Hash>>,
}

impl BlockhashCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        let initial = rpc.get_latest_blockhash().unwrap_or_default();
        let inner = Arc::new(Mutex::new(initial));
        let shared = inner.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(300));
            loop {
                ticker.tick().await;
                let rpc_clone = rpc.clone();
                let result = tokio::task::spawn_blocking(move || {
                    rpc_clone.get_latest_blockhash()
                }).await;

                match result {
                    Ok(Ok(h)) => {
                        *shared.lock().unwrap() = h;
                        debug!("blockhash refreshed");
                    }
                    Ok(Err(e)) => warn!(error = %e, "blockhash RPC failed"),
                    Err(e) => warn!(error = %e, "blockhash spawn_blocking panicked"),
                }
            }
        });

        Self { inner }
    }

    #[inline]
    pub fn get(&self) -> Hash {
        *self.inner.lock().unwrap()
    }
}
