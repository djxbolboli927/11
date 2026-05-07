use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub metis: MetisConfig,
    pub trading: TradingConfig,
    pub jito: JitoConfig,
    pub rpc: RpcConfig,
    pub yellowstone_grpc: YellowstoneGrpcConfig,
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub simulation: SimulationConfig,
    #[serde(default)]
    pub jito_grpc: JitoGrpcConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetisConfig {
    pub url: String,
    #[allow(dead_code)]
    pub binary_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradingConfig {
    pub min_amount_sol: f64,
    pub max_amount_sol: f64,
    pub step_sol: f64,
    pub min_profit_lamports: u64,
    /// Base Solana network fee in lamports (e.g. 10000 = 0.00001 SOL).
    pub base_fee_lamports: u64,
    /// Fraction of expected net profit to sacrifice in the output floor.
    /// 0.70 means sacrifice 70% and retain 30% as guaranteed profit.
    #[serde(default = "default_profit_sacrifice_percent")]
    pub profit_sacrifice_percent: f64,
    pub tokens_file: String,
}

fn default_profit_sacrifice_percent() -> f64 {
    0.0
}

#[derive(Debug, Deserialize, Clone)]
pub struct JitoConfig {
    /// Multiple Jito block engine URLs -- bundles are sent to ALL concurrently.
    pub urls: Vec<String>,
    pub uuid: String,
    pub trading_keypair: String,
    pub tip_min_lamports: u64,
    pub tip_max_lamports: u64,
    pub tip_profit_percent: f64,
    pub max_bundles_per_second: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct YellowstoneGrpcConfig {
    pub endpoint: String,
    pub x_token: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SimulationConfig {
    /// If false, bot sends every profitable tx without any local sim gate
    /// (pre-LiteSVM behaviour). Default: disabled so legacy configs keep
    /// working until the operator opts in.
    #[serde(default)]
    pub enabled: bool,
    /// Directory containing the DEX .so binaries listed in `program_registry`.
    #[serde(default = "default_so_dir")]
    pub so_dir: String,
    /// Directory containing per-pool account files (`dex/<DEX>/<pool>.toml`).
    /// These are pre-fetched at startup and the vault accounts within are
    /// subscribed for live Yellowstone updates.
    #[serde(default = "default_dex_dir")]
    pub dex_dir: String,
    /// When sim reverts or errors, `fail_closed=true` drops the send (safest);
    /// `false` logs and forwards to Jito anyway (useful during rollout).
    #[serde(default = "default_true")]
    pub fail_closed: bool,
    /// Number of INDEPENDENT Simulator instances to spin up. Each Simulator
    /// owns its own `Mutex<LiteSVM>`, so N workers = N sims in parallel.
    /// Sizing guidance: in steady state each sim takes ~2-5ms of CPU, so
    /// `workers` should roughly equal the peak number of profitable
    /// opportunities that arrive per 5ms window. In production, 8 is a
    /// sensible default (handles ~1600 sims/sec with headroom).
    #[serde(default = "default_workers")]
    pub workers: usize,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            so_dir: default_so_dir(),
            dex_dir: default_dex_dir(),
            fail_closed: true,
            workers: default_workers(),
        }
    }
}

fn default_so_dir() -> String {
    "/home/soluser/m/so".to_string()
}

fn default_dex_dir() -> String {
    "vendor/litesvm/dex".to_string()
}

fn default_true() -> bool {
    true
}

fn default_workers() -> usize {
    8
}

/// Second Jito submission path via SearcherService gRPC.
///
/// Runs alongside the REST UUID client in `jito.rs`. Each path has its
/// own rate limiter, so the effective Jito throughput is
/// `jito.max_bundles_per_second + jito_grpc.max_bundles_per_second`.
///
/// Like the REST client, gRPC fans out to every regional Block Engine
/// endpoint concurrently — first regional success wins. Per-region auth
/// is attempted using the whitelisted keypair, which gives 5 req/s per
/// region. Regions whose auth fails downgrade to no-auth mode (1 req/s).
#[derive(Debug, Deserialize, Clone)]
pub struct JitoGrpcConfig {
    /// If false, only the REST UUID path is used (pre-gRPC behaviour).
    #[serde(default)]
    pub enabled: bool,
    /// All Jito Block Engine gRPC endpoints. Bundles are broadcast to ALL
    /// of these per send call, mirroring the REST multi-region fan-out.
    #[serde(default = "default_jito_grpc_endpoints")]
    pub endpoints: Vec<String>,
    /// Path to the Solana keypair JSON whose pubkey Jito has whitelisted
    /// for gRPC auth. This wallet holds no funds — it is an identity only.
    /// If empty or auth fails, regions fall back to no-auth (1 req/s).
    #[serde(default)]
    pub auth_keypair: String,
    /// Per-second rate limit applied *before* the gRPC SendBundle call.
    /// REST and gRPC limiters operate independently.
    #[serde(default = "default_grpc_rate")]
    pub max_bundles_per_second: u32,
}

impl Default for JitoGrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: default_jito_grpc_endpoints(),
            auth_keypair: String::new(),
            max_bundles_per_second: default_grpc_rate(),
        }
    }
}

fn default_jito_grpc_endpoints() -> Vec<String> {
    vec![
        "https://amsterdam.mainnet.block-engine.jito.wtf".to_string(),
        "https://dublin.mainnet.block-engine.jito.wtf".to_string(),
        "https://frankfurt.mainnet.block-engine.jito.wtf".to_string(),
        "https://london.mainnet.block-engine.jito.wtf".to_string(),
        "https://ny.mainnet.block-engine.jito.wtf".to_string(),
        "https://slc.mainnet.block-engine.jito.wtf".to_string(),
        "https://singapore.mainnet.block-engine.jito.wtf".to_string(),
        "https://tokyo.mainnet.block-engine.jito.wtf".to_string(),
    ]
}

fn default_grpc_rate() -> u32 {
    5
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    /// Number of tokio worker threads (multi-thread runtime).
    pub threads: usize,
    pub quote_timeout_ms: u64,
    /// CU limits per hop count: index 0 = 2 hops, index 1 = 3 hops, etc.
    /// If hops exceed the array, the last value is used.
    pub cu_limits: Vec<u32>,
    /// Optional CPU affinity for each worker thread.
    /// If non-empty, worker i is pinned to core `bot_cpu_cores[i % len]`.
    /// Leave empty `[]` to disable pinning.
    #[serde(default)]
    pub bot_cpu_cores: Vec<usize>,
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
