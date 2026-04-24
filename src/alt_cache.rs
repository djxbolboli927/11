use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{address_lookup_table::AddressLookupTableAccount, pubkey::Pubkey};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tracing::{debug, warn};

use crate::transaction::deserialize_alt_addresses;

pub struct AltCache {
    inner: Arc<RwLock<HashMap<Pubkey, AddressLookupTableAccount>>>,
    tip_pubkeys: Arc<Vec<Pubkey>>,
}

impl AltCache {
    pub fn new(tip_pubkeys: Vec<Pubkey>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            tip_pubkeys: Arc::new(tip_pubkeys),
        }
    }

    pub fn get_or_fetch(
        &self,
        pubkey: &Pubkey,
        rpc: &RpcClient,
    ) -> Result<AddressLookupTableAccount> {
        {
            let cache = self.inner.read().unwrap();
            if let Some(alt) = cache.get(pubkey) {
                debug!(alt = %pubkey, "ALT cache hit");
                return Ok(alt.clone());
            }
        }

        debug!(alt = %pubkey, "ALT cache miss -- fetching from RPC");
        let account = rpc
            .get_account(pubkey)
            .map_err(|e| anyhow::anyhow!("failed to fetch ALT {}: {}", pubkey, e))?;

        let mut addresses = deserialize_alt_addresses(&account.data)?;
        addresses.retain(|addr| !self.tip_pubkeys.contains(addr));

        let alt = AddressLookupTableAccount {
            key: *pubkey,
            addresses,
        };

        self.inner.write().unwrap().insert(*pubkey, alt.clone());
        Ok(alt)
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        self.inner.write().unwrap().clear();
        warn!("ALT cache cleared");
    }
}

impl Clone for AltCache {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            tip_pubkeys: self.tip_pubkeys.clone(),
        }
    }
}
