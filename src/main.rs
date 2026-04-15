mod account_cache;
mod alt_cache;
mod arbitrage;
mod blockhash_cache;
mod config;
mod jito;
mod litesvm_sim;
mod metis;
mod program_registry;
mod rate_limiter;
mod tokens;
mod transaction;
mod wallet;

use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing::{error, info, warn};

use alt_cache::AltCache;
use blockhash_cache::BlockhashCache;
use rate_limiter::RateLimiter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::new(
                "info,hyper_util=warn,hyper=warn,reqwest=warn,h2=warn,tonic=warn",
            ),
        )
        .init();

    let config = config::Config::load("config.toml")?;
    info!("config loaded");

    // Build a multi-thread tokio runtime with EXACTLY the number of worker
    // threads requested in [performance].threads. If bot_cpu_cores is set,
    // pin each worker thread to a specific core via core_affinity.
    let worker_threads = config.performance.threads.max(1);
    let pinned_cores: Vec<usize> = config.performance.bot_cpu_cores.clone();
    let available_cores = core_affinity::get_core_ids().unwrap_or_default();
    let next_worker = Arc::new(AtomicUsize::new(0));

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads).enable_all();
    builder.thread_name("arb-worker");

    if !pinned_cores.is_empty() {
        let cores = pinned_cores.clone();
        let available = available_cores.clone();
        let counter = next_worker.clone();
        builder.on_thread_start(move || {
            let idx = counter.fetch_add(1, Ordering::SeqCst);
            let target = cores[idx % cores.len()];
            if let Some(core_id) = available.iter().find(|c| c.id == target) {
                let ok = core_affinity::set_for_current(*core_id);
                if ok {
                    tracing::info!(worker = idx, core = target, "pinned tokio worker to core");
                } else {
                    tracing::warn!(worker = idx, core = target, "failed to pin worker to core");
                }
            } else {
                tracing::warn!(worker = idx, core = target, "requested core not available");
            }
        });
    }

    let runtime = builder.build()?;
    info!(
        worker_threads,
        pinned = !pinned_cores.is_empty(),
        cores = ?pinned_cores,
        "tokio runtime built"
    );

    runtime.block_on(async_main(config))
}

async fn async_main(config: config::Config) -> Result<()> {
    let token_mints = tokens::load_tokens(&config.trading.tokens_file)?;
    info!(count = token_mints.len(), "tokens loaded");

    let trading_keypair = wallet::read_keypair(&config.jito.trading_keypair)?;
    info!(trading_wallet = %trading_keypair.pubkey(), "keypair loaded");

    let rpc_client = Arc::new(RpcClient::new(config.rpc.url.clone()));

    // Verify WSOL ATA exists
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

    // BlockhashCache — refreshes every 300ms in background
    let blockhash_cache = BlockhashCache::new(rpc_client.clone());
    info!("blockhash cache initialized (refresh every 300ms)");

    // AltCache — Jito tip accounts excluded from ALT entries
    let tip_pubkeys = transaction::jito_tip_pubkeys();
    let alt_cache = AltCache::new(tip_pubkeys);
    info!("ALT cache initialized");

    let metis = metis::MetisClient::new(&config.metis.url, config.performance.quote_timeout_ms);

    // Multi-region Jito client — sends to ALL endpoints concurrently.
    // Wrapped in Arc so send tasks can be tokio::spawn'd with 'static lifetime.
    let jito_client = Arc::new(jito::JitoClient::new(&config.jito.urls, &config.jito.uuid));
    info!(
        regions = config.jito.urls.len(),
        urls = ?config.jito.urls,
        "Jito multi-region client ready"
    );

    let mut jito_limiter = RateLimiter::new(config.jito.max_bundles_per_second);

    // ── LiteSVM pre-flight simulation (optional, enabled via [simulation]) ──
    // Spins up an AccountCache backed by the same Yellowstone gRPC stream
    // Metis reads from, plus a Simulator that loads every DEX .so at startup.
    // On every profitable opportunity, scan_all_tokens will ask the Simulator
    // to run the tx locally before paying for a Jito base fee.
    let (sim_cache, simulator) = if config.simulation.enabled {
        let cache = account_cache::AccountCache::new(rpc_client.clone());
        cache.spawn_subscription(
            config.yellowstone_grpc.endpoint.clone(),
            config.yellowstone_grpc.x_token.clone(),
            program_registry::all_program_ids(),
            vec![wsol_ata],
        );
        info!(
            endpoint = %config.yellowstone_grpc.endpoint,
            dex_programs = program_registry::PROGRAMS.len(),
            "Yellowstone account cache subscribed"
        );

        // Pre-warm: token mints and the user's WSOL ATA are not streamed via
        // the DEX-owner filter. Fetch once from RPC so the first sim doesn't
        // miss them.
        let mut warm: Vec<solana_sdk::pubkey::Pubkey> = token_mints
            .iter()
            .filter_map(|s| solana_sdk::pubkey::Pubkey::try_from(s.as_str()).ok())
            .collect();
        warm.push(wsol_mint);
        warm.push(wsol_ata);
        cache.prefetch(&warm);
        info!(warmed = cache.len(), "account cache pre-warmed");

        let sim = litesvm_sim::Simulator::new(
            &config.simulation.so_dir,
            wsol_ata,
            config.simulation.fail_closed,
        )?;
        (Some(Arc::new(cache)), Some(Arc::new(sim)))
    } else {
        info!("LiteSVM simulation disabled via config");
        (None, None)
    };

    info!(
        tokens = token_mints.len(),
        min_sol = config.trading.min_amount_sol,
        max_sol = config.trading.max_amount_sol,
        step = config.trading.step_sol,
        sim = config.simulation.enabled,
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
            &mut jito_limiter,
            &blockhash_cache,
            &alt_cache,
            sim_cache.as_ref(),
            simulator.as_ref(),
        )
        .await
        {
            error!(error = %e, "scan cycle error");
        }
    }
}
