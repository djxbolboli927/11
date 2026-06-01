use crate::metis::SwapInstructionsResponse;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const MAX_ENTRIES: usize = 2_000;

/// In-RAM store for swap_instructions results.
///
/// No disk I/O, no background tasks, no TTL.
/// Plain HashMap capped at MAX_ENTRIES — inserts stop when full.
///
/// Key: (route_signature, in_amount_lamports)
/// The route_signature is a FNV-1a hash of the route_plan JSON, independent
/// of the input/output amounts so the same DEX path with different sizes
/// maps to the same route (but different cache keys because amount differs).
pub struct InstructionCache {
    map: RwLock<HashMap<(u64, u64), SwapInstructionsResponse>>,
}

/// FNV-1a 64-bit hash of route_plan JSON (amount-independent route identity).
pub fn route_sig(route_plan: &serde_json::Value) -> u64 {
    let s = route_plan.to_string();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0001_0000_01b3);
    }
    h
}

impl InstructionCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            map: RwLock::new(HashMap::new()),
        })
    }

    /// Returns a cloned entry if present.
    pub fn get(&self, sig: u64, amount: u64) -> Option<SwapInstructionsResponse> {
        self.map.read().ok()?.get(&(sig, amount)).cloned()
    }

    /// Inserts if absent and under the size cap.
    /// Returns true if the entry was newly added.
    pub fn set(&self, sig: u64, amount: u64, value: SwapInstructionsResponse) -> bool {
        let Ok(mut m) = self.map.write() else {
            return false;
        };
        if m.len() >= MAX_ENTRIES {
            return false;
        }
        use std::collections::hash_map::Entry;
        match m.entry((sig, amount)) {
            Entry::Vacant(e) => {
                e.insert(value);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        self.map.read().map(|m| m.len()).unwrap_or(0)
    }
}
