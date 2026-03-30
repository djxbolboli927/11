mod alt_cache;
mod arbitrage;
mod blockhash_cache;
mod config;
mod jito;
mod metis;
mod rate_limiter;
mod tokens;
mod transaction;
mod wallet;

use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use std::sync::Arc;
use tracing::{error, info, warn};

use alt_cache::AltCache;
use blockhash_cache::BlockhashCache;
use rate_limiter::RateLimiter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::new(
                "info,hyper_util=warn,hyper=warn,reqwest=warn,h2=warn,tonic=warn"
            ),
        )
        .init();

    let config = config::Config::load("config.toml")?;
    info!("config loaded");

    let token_mints = tokens::load_tokens(&config.trading.tokens_file)?;
    info!(count = token_mints.len(), "tokens loaded");

    let trading_keypair = wallet::read_keypair(&config.jito.trading_keypair)?;
    info!(trading_wallet = %trading_keypair.pubkey(), "keypair loaded");

    let rpc_client = Arc::new(RpcClient::new(config.rpc.url.clone()));

    let sim_rpc_client = if !config.rpc.simulation_url.is_empty() {
        let client = Arc::new(RpcClient::new(config.rpc.simulation_url.clone()));
        info!(url = config.rpc.simulation_url.as_str(), "simulation RPC enabled");
        Some(client)
    } else {
        info!("simulation RPC disabled (no simulation_url)");
        None
    };

    // Verify WSOL ATA exists (required because wrapAndUnwrapSol=false)
    let wsol_mint = solana_sdk::pubkey::Pubkey::from_str_const(tokens::WSOL_MINT);
    let wsol_ata = spl_associated_token_account::get_associated_token_address(
        &trading_keypair.pubkey(),
        &wsol_mint,
    );
    match rpc_client.get_account(&wsol_ata) {
        Ok(_) => info!(ata = %wsol_ata, "WSOL ATA verified"),
        Err(e) => {
            warn!(
                ata = %wsol_ata,
                error = %e,
                "WSOL ATA not found — run: spl-token wrap <amount>"
            );
        }
    }

    // Initialize BlockhashCache — refreshes every 300ms in background
    let blockhash_cache = BlockhashCache::new(rpc_client.clone());
    info!("blockhash cache initialized (refresh every 300ms)");

    // Initialize AltCache — Jito tip accounts excluded from ALT entries
    let tip_pubkeys = transaction::jito_tip_pubkeys();
    let alt_cache = AltCache::new(tip_pubkeys);
    info!("ALT cache initialized");

    let metis = metis::MetisClient::new(&config.metis.url, config.performance.quote_timeout_ms);

    let jito_client = jito::JitoClient::new(&config.jito.url, &config.jito.uuid);

    let mut jito_limiter = RateLimiter::new(config.jito.max_bundles_per_second);

    info!(
        tokens = token_mints.len(),
        min_sol = config.trading.min_amount_sol,
        max_sol = config.trading.max_amount_sol,
        step = config.trading.step_sol,
        base_fee = config.trading.base_fee_lamports,
        "starting arbitrage scanner"
    );

    loop {
        if let Err(e) = arbitrage::scan_all_tokens(
            &metis,
            &token_mints,
            &config,
            &jito_client,
            &trading_keypair,
            &rpc_client,
            sim_rpc_client.as_deref(),
            &mut jito_limiter,
            &blockhash_cache,
            &alt_cache,
        )
        .await
        {
            error!(error = %e, "scan cycle error");
        }
    }
}
