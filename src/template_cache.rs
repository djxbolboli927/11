use crate::metis::SwapInstructionsResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// ─── Constants ────────────────────────────────────────────────────────────────

const BASE_DIR: &str = "/root/c/cache";
/// Max distinct routes kept live (hot+cold ≤ 2×). For 200 pools ×2 directions
/// that's ~400 routes; 2000 gives plenty of headroom without growing unbounded.
const ROUTE_SEG_CAP: usize = 2_000;
/// Max distinct hops tracked (one entry per pool+direction, amount-independent).
const HOP_CAP: usize = 2_000;

// ─── Route signature ──────────────────────────────────────────────────────────

/// Amount-independent 128-bit structural hash of a route_plan.
/// Two independent FNV-1a passes with different seeds.
/// Hashes ALL swapInfo fields except inAmount / outAmount / feeAmount,
/// including booleans (a_to_b) which the old u64 sig missed.
pub fn route_sig(route_plan: &serde_json::Value) -> u128 {
    let h1 = fnv1a_64(route_plan, 0xcbf2_9ce4_8422_2325_u64);
    let h2 = fnv1a_64(route_plan, 0x517c_c1b7_2722_0a95_u64);
    ((h1 as u128) << 64) | (h2 as u128)
}

fn fnv1a_64(route_plan: &serde_json::Value, seed: u64) -> u64 {
    let mut h = seed;
    macro_rules! feed {
        ($b:expr) => {
            for &byte in ($b as &[u8]) {
                h ^= byte as u64;
                h = h.wrapping_mul(0x0000_0001_0000_01b3);
            }
        };
    }
    if let Some(arr) = route_plan.as_array() {
        for hop in arr {
            if let Some(si) = hop.get("swapInfo").and_then(|s| s.as_object()) {
                let mut keys: Vec<&String> = si.keys().collect();
                keys.sort();
                for k in keys {
                    if matches!(k.as_str(), "inAmount" | "outAmount" | "feeAmount") {
                        continue;
                    }
                    feed!(k.as_bytes());
                    feed!(b"=");
                    if let Some(v) = si.get(k) {
                        match v {
                            serde_json::Value::String(s) => feed!(s.as_bytes()),
                            serde_json::Value::Bool(b) => feed!(if *b { b"T" } else { b"F" }),
                            serde_json::Value::Number(n) => feed!(n.to_string().as_bytes()),
                            _ => {}
                        }
                    }
                    feed!(b";");
                }
            }
        }
    }
    h
}

// ─── DEX kind ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum DexKind {
    Whirlpool,
    WhirlpoolSwapV2,
    RaydiumClmm,
    RaydiumV2,
    MeteoraDlmm,
    MeteoraPools,
    AlphaQ,
    Aquifer,
    AldrinV2,
    Other,
}

impl DexKind {
    fn from_label(label: &str) -> Self {
        let l = label.to_lowercase();
        if l.contains("whirlpool") {
            return if l.contains("v2") || l.contains("swap_v2") || l.contains("swapv2") {
                DexKind::WhirlpoolSwapV2
            } else {
                DexKind::Whirlpool
            };
        }
        if l.contains("raydium") && l.contains("clmm") {
            return DexKind::RaydiumClmm;
        }
        if l.contains("raydium") {
            return DexKind::RaydiumV2;
        }
        if l.contains("meteora") && l.contains("dlmm") {
            return DexKind::MeteoraDlmm;
        }
        if l.contains("meteora") {
            return DexKind::MeteoraPools;
        }
        if l.contains("alphaq") || l.contains("alpha_q") {
            return DexKind::AlphaQ;
        }
        if l.contains("aquifer") {
            return DexKind::Aquifer;
        }
        if l.contains("aldrin") {
            return DexKind::AldrinV2;
        }
        DexKind::Other
    }

    fn file_stem(&self) -> &'static str {
        match self {
            DexKind::Whirlpool => "whirlpool",
            DexKind::WhirlpoolSwapV2 => "whirlpool_swap_v2",
            DexKind::RaydiumClmm => "raydium_clmm",
            DexKind::RaydiumV2 => "raydium_v2",
            DexKind::MeteoraDlmm => "meteora_dlmm",
            DexKind::MeteoraPools => "meteora_pools",
            DexKind::AlphaQ => "alphaq",
            DexKind::Aquifer => "aquifer",
            DexKind::AldrinV2 => "aldrin_v2",
            DexKind::Other => "other",
        }
    }
}

// ─── Direction ────────────────────────────────────────────────────────────────

/// Per-DEX directional parameter stored in the HopKey.
/// Amount-independent: two calls on the same pool with the same direction
/// share one template entry regardless of trade size.
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum DirectionParams {
    None,
    AToB(bool),
    SideBid,
    SideAsk,
}

fn extract_direction(
    dex: &DexKind,
    si: &serde_json::Map<String, serde_json::Value>,
) -> DirectionParams {
    match dex {
        DexKind::Whirlpool | DexKind::WhirlpoolSwapV2 | DexKind::AlphaQ => {
            for field in &["a_to_b", "aToB", "atob", "a2b"] {
                if let Some(serde_json::Value::Bool(b)) = si.get(*field) {
                    return DirectionParams::AToB(*b);
                }
            }
            DirectionParams::None
        }
        DexKind::AldrinV2 => {
            if let Some(serde_json::Value::String(s)) = si.get("side") {
                return match s.to_lowercase().as_str() {
                    "bid" => DirectionParams::SideBid,
                    "ask" => DirectionParams::SideAsk,
                    _ => DirectionParams::None,
                };
            }
            DirectionParams::None
        }
        _ => DirectionParams::None,
    }
}

// ─── HopKey ───────────────────────────────────────────────────────────────────

/// Amount-independent key for a single DEX hop.
/// Two calls with different amounts but same pool/direction share one entry.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct HopKey {
    pub dex: DexKind,
    pub pool_or_amm_key: String,
    pub input_mint: String,
    pub output_mint: String,
    pub direction: DirectionParams,
    pub remaining_accounts_hash: u64,
}

impl HopKey {
    fn from_swap_info(si: &serde_json::Map<String, serde_json::Value>) -> Option<Self> {
        let pool = si.get("ammKey").and_then(|v| v.as_str())?.to_string();
        let input = si.get("inputMint").and_then(|v| v.as_str())?.to_string();
        let output = si.get("outputMint").and_then(|v| v.as_str())?.to_string();
        let label = si.get("label").and_then(|v| v.as_str()).unwrap_or("");
        let dex = DexKind::from_label(label);
        let direction = extract_direction(&dex, si);
        Some(HopKey {
            dex,
            pool_or_amm_key: pool,
            input_mint: input,
            output_mint: output,
            direction,
            remaining_accounts_hash: 0,
        })
    }

    fn template_id(&self) -> String {
        let dir = match &self.direction {
            DirectionParams::None => "none".to_string(),
            DirectionParams::AToB(b) => format!("a2b:{b}"),
            DirectionParams::SideBid => "bid".to_string(),
            DirectionParams::SideAsk => "ask".to_string(),
        };
        format!(
            "{}:{}:{}:{}:{}",
            self.pool_or_amm_key,
            self.input_mint,
            self.output_mint,
            dir,
            self.remaining_accounts_hash,
        )
    }
}

// ─── HopTemplate (persisted to per-DEX JSON files) ────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HopTemplate {
    pub dex: DexKind,
    pub pool_or_amm_key: String,
    pub input_mint: String,
    pub output_mint: String,
    pub direction: DirectionParams,
    pub remaining_accounts_hash: u64,
    pub seen_count: u64,
    pub first_seen_slot: Option<u64>,
    pub last_seen_slot: Option<u64>,
}

// ─── RouteTemplate (in-RAM only, keyed by route_sig with NO amount) ───────────

/// Stores one complete SwapInstructionsResponse per route structure.
/// When the same route is needed with a different amount, the in_amount and
/// quoted_out_amount bytes are patched in place using known Borsh offsets.
#[derive(Clone, Debug)]
pub struct RouteTemplate {
    #[allow(dead_code)]
    pub route_signature: u128,
    pub swap_ixs: SwapInstructionsResponse,
    /// in_amount that was active when this template was first captured.
    pub template_in_amount: u64,
    /// quoted_out_amount (= on_chain_floor) captured with the template.
    pub template_quoted_out: u64,
    /// Byte offset of in_amount in the decoded swap_instruction.data.
    /// None when the Borsh layout didn't validate on first capture.
    pub in_amount_offset: Option<usize>,
    pub quoted_out_offset: Option<usize>,
    pub seen_count: u64,
    pub hit_count: u64,
}

// ─── Borsh amount patching ────────────────────────────────────────────────────

/// Locate in_amount / quoted_out_amount inside Borsh-encoded route_v2 data.
///
/// Jupiter v6 route_v2 arg order (IDL): routePlan, inAmount, quotedOutAmount,
/// slippageBps (u16=0), platformFeeBps (u8=0).
///
/// From the END of the serialized buffer:
///   [len-1]        = platformFeeBps (0)
///   [len-3..len-1] = slippageBps   (0, u16 LE)
///   [len-11..len-3]= quotedOutAmount (u64 LE)
///   [len-19..len-11]= inAmount       (u64 LE)
///
/// Validation: last 3 bytes must be 0x00, AND the discovered values must match
/// the amounts we know were active — both must hold or we return None (safe
/// fallback to Metis rather than risk a bad instruction).
fn discover_offsets(data: &[u8], in_amount: u64, quoted_out: u64) -> Option<(usize, usize)> {
    let len = data.len();
    if len < 27 {
        return None;
    }
    if data[len - 1] != 0 || data[len - 2] != 0 || data[len - 3] != 0 {
        return None;
    }
    let in_off = len - 19;
    let out_off = len - 11;
    let stored_in = u64::from_le_bytes(data[in_off..in_off + 8].try_into().ok()?);
    let stored_out = u64::from_le_bytes(data[out_off..out_off + 8].try_into().ok()?);
    if stored_in != in_amount || stored_out != quoted_out {
        return None;
    }
    Some((in_off, out_off))
}

fn patch_amounts_b64(
    data_b64: &str,
    in_off: usize,
    out_off: usize,
    new_in: u64,
    new_out: u64,
) -> Option<String> {
    let mut data = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_b64)
        .ok()?;
    if in_off + 8 > data.len() || out_off + 8 > data.len() {
        return None;
    }
    data[in_off..in_off + 8].copy_from_slice(&new_in.to_le_bytes());
    data[out_off..out_off + 8].copy_from_slice(&new_out.to_le_bytes());
    Some(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &data,
    ))
}

/// Try to serve a RouteTemplate with new amounts.
/// Returns None only when patching is required but offsets are unknown —
/// the caller must fall through to Metis in that case.
pub fn serve_route(
    tmpl: &RouteTemplate,
    new_in: u64,
    new_out: u64,
) -> Option<SwapInstructionsResponse> {
    if new_in == tmpl.template_in_amount && new_out == tmpl.template_quoted_out {
        return Some(tmpl.swap_ixs.clone());
    }
    let (in_off, out_off) = (tmpl.in_amount_offset?, tmpl.quoted_out_offset?);
    let new_data =
        patch_amounts_b64(&tmpl.swap_ixs.swap_instruction.data, in_off, out_off, new_in, new_out)?;
    let mut patched = tmpl.swap_ixs.clone();
    patched.swap_instruction.data = new_data;
    Some(patched)
}

// ─── TemplateStore ────────────────────────────────────────────────────────────

pub struct TemplateStore {
    inner: RwLock<StoreInner>,
}

struct StoreInner {
    routes_hot: HashMap<u128, RouteTemplate>,
    routes_cold: HashMap<u128, RouteTemplate>,
    hops: HashMap<HopKey, HopTemplate>,
}

impl TemplateStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(StoreInner {
                routes_hot: HashMap::new(),
                routes_cold: HashMap::new(),
                hops: HashMap::new(),
            }),
        })
    }

    // ── Route template ────────────────────────────────────────────────────────

    /// Look up a RouteTemplate. Promotes cold hits to hot.
    pub fn get_route(&self, sig: u128) -> Option<RouteTemplate> {
        {
            let g = self.inner.read().ok()?;
            if let Some(t) = g.routes_hot.get(&sig) {
                return Some(t.clone());
            }
            if !g.routes_cold.contains_key(&sig) {
                return None;
            }
        }
        let mut g = self.inner.write().ok()?;
        if let Some(mut t) = g.routes_cold.remove(&sig) {
            t.hit_count += 1;
            let out = t.clone();
            Self::push_hot_route(&mut g, sig, t);
            return Some(out);
        }
        g.routes_hot.get(&sig).cloned()
    }

    /// Record a hit on an already-served template (increments hit_count).
    pub fn record_route_hit(&self, sig: u128) {
        let Ok(mut g) = self.inner.write() else { return };
        if let Some(t) = g.routes_hot.get_mut(&sig) {
            t.hit_count += 1;
        } else if let Some(t) = g.routes_cold.get_mut(&sig) {
            t.hit_count += 1;
        }
    }

    /// Insert a new RouteTemplate (first time we see this route structure).
    /// If the route already exists, only increments seen_count.
    pub fn insert_route(
        &self,
        sig: u128,
        swap_ixs: SwapInstructionsResponse,
        in_amount: u64,
        quoted_out: u64,
    ) {
        let offsets = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &swap_ixs.swap_instruction.data,
        )
        .ok()
        .and_then(|d| discover_offsets(&d, in_amount, quoted_out));

        let Ok(mut g) = self.inner.write() else { return };

        if g.routes_hot.contains_key(&sig) {
            g.routes_hot.get_mut(&sig).unwrap().seen_count += 1;
            return;
        }
        if g.routes_cold.contains_key(&sig) {
            g.routes_cold.get_mut(&sig).unwrap().seen_count += 1;
            return;
        }

        let tmpl = RouteTemplate {
            route_signature: sig,
            swap_ixs,
            template_in_amount: in_amount,
            template_quoted_out: quoted_out,
            in_amount_offset: offsets.map(|(i, _)| i),
            quoted_out_offset: offsets.map(|(_, o)| o),
            seen_count: 1,
            hit_count: 0,
        };
        Self::push_hot_route(&mut g, sig, tmpl);
    }

    fn push_hot_route(g: &mut StoreInner, sig: u128, tmpl: RouteTemplate) {
        if g.routes_hot.len() >= ROUTE_SEG_CAP {
            g.routes_cold = std::mem::take(&mut g.routes_hot);
        }
        g.routes_hot.insert(sig, tmpl);
    }

    // ── Hop tracking ──────────────────────────────────────────────────────────

    /// Returns (all_hit, missing_count) for the hops in a merged route_plan.
    pub fn check_hops(&self, route_plan: &serde_json::Value) -> (bool, usize) {
        let Ok(g) = self.inner.read() else { return (false, 0) };
        let mut missing = 0usize;
        if let Some(arr) = route_plan.as_array() {
            for hop in arr {
                let has = hop
                    .get("swapInfo")
                    .and_then(|s| s.as_object())
                    .and_then(|si| HopKey::from_swap_info(si))
                    .map(|k| g.hops.contains_key(&k))
                    .unwrap_or(false);
                if !has {
                    missing += 1;
                }
            }
        }
        (missing == 0, missing)
    }

    /// Record hops from a successful Metis response (amount-independent).
    pub fn record_hops(&self, route_plan: &serde_json::Value, context_slot: Option<u64>) {
        let Ok(mut g) = self.inner.write() else { return };
        if g.hops.len() >= HOP_CAP {
            return;
        }
        if let Some(arr) = route_plan.as_array() {
            for hop in arr {
                if let Some(si) = hop.get("swapInfo").and_then(|s| s.as_object()) {
                    if let Some(key) = HopKey::from_swap_info(si) {
                        let ent = g.hops.entry(key.clone()).or_insert_with(|| HopTemplate {
                            dex: key.dex.clone(),
                            pool_or_amm_key: key.pool_or_amm_key.clone(),
                            input_mint: key.input_mint.clone(),
                            output_mint: key.output_mint.clone(),
                            direction: key.direction.clone(),
                            remaining_accounts_hash: key.remaining_accounts_hash,
                            seen_count: 0,
                            first_seen_slot: context_slot,
                            last_seen_slot: context_slot,
                        });
                        ent.seen_count += 1;
                        ent.last_seen_slot = context_slot;
                    }
                }
            }
        }
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub fn route_count(&self) -> usize {
        self.inner
            .read()
            .map(|g| g.routes_hot.len() + g.routes_cold.len())
            .unwrap_or(0)
    }

    pub fn hop_count(&self) -> usize {
        self.inner.read().map(|g| g.hops.len()).unwrap_or(0)
    }

    // ── Disk persistence ──────────────────────────────────────────────────────

    /// Load HopTemplates from per-DEX JSON files in cache/hops/.
    /// RouteTemplates are NOT persisted (they contain full instruction data
    /// that must be re-validated each run to stay fresh).
    pub fn load_from_disk(&self) -> usize {
        let hops_dir = format!("{BASE_DIR}/hops");
        let entries = match std::fs::read_dir(&hops_dir) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        let Ok(mut g) = self.inner.write() else { return 0 };
        let mut count = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(data) = std::fs::read_to_string(&path) else { continue };
            let file: DexHopFile = match serde_json::from_str(&data) {
                Ok(f) => f,
                Err(_) => continue,
            };
            for (_, tmpl) in file.templates {
                let key = HopKey {
                    dex: tmpl.dex.clone(),
                    pool_or_amm_key: tmpl.pool_or_amm_key.clone(),
                    input_mint: tmpl.input_mint.clone(),
                    output_mint: tmpl.output_mint.clone(),
                    direction: tmpl.direction.clone(),
                    remaining_accounts_hash: tmpl.remaining_accounts_hash,
                };
                g.hops.entry(key).or_insert(tmpl);
                count += 1;
            }
        }
        count
    }

    fn flush_to_disk(&self) {
        let hops: Vec<(HopKey, HopTemplate)> = {
            let Ok(g) = self.inner.read() else { return };
            g.hops.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        // Group by DEX (each DEX → one JSON file)
        let mut by_dex: HashMap<&'static str, (DexKind, HashMap<String, HopTemplate>)> =
            HashMap::new();
        for (key, tmpl) in &hops {
            let stem = key.dex.file_stem();
            let (_, map) =
                by_dex.entry(stem).or_insert_with(|| (key.dex.clone(), HashMap::new()));
            map.insert(key.template_id(), tmpl.clone());
        }

        let hops_dir = format!("{BASE_DIR}/hops");
        let _ = std::fs::create_dir_all(&hops_dir);

        for (stem, (dex, templates)) in by_dex {
            let path = format!("{hops_dir}/{stem}.json");
            let file = DexHopFile { schema_version: 1, dex, templates };
            let Ok(json) = serde_json::to_string_pretty(&file) else { continue };
            let tmp = format!("{path}.tmp");
            if std::fs::write(&tmp, json.as_bytes()).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
    }

    pub fn spawn_flush_task(self: &Arc<Self>, secs: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            interval.tick().await;
            loop {
                interval.tick().await;
                let c = this.clone();
                let _ = tokio::task::spawn_blocking(move || c.flush_to_disk()).await;
            }
        });
    }
}

// ─── Disk format ──────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct DexHopFile {
    schema_version: u32,
    dex: DexKind,
    /// Keyed by template_id() so repeated pool/direction never adds a second row.
    templates: HashMap<String, HopTemplate>,
}
