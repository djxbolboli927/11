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
    pub tokens_file: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JitoConfig {
    pub url: String,
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
    /// RPC for tx simulation before sending to Jito (e.g. eRPC).
    /// If empty, simulation is skipped.
    #[serde(default)]
    pub simulation_url: String,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct YellowstoneGrpcConfig {
    pub endpoint: String,
    pub x_token: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    pub threads: usize,
    pub quote_timeout_ms: u64,
    /// CU limits per hop count: index 0 = 2 hops, index 1 = 3 hops, etc.
    /// If hops exceed the array, the last value is used.
    pub cu_limits: Vec<u32>,
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
