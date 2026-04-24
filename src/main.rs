mod account_cache;
mod alt_cache;
mod arbitrage;
mod blockhash_cache;
mod config;
mod jito;
mod jito_grpc;
mod litesvm_sim;
mod metis;
mod metrics;
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
    Arc, Mutex,
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

    let trading_keypair = Arc::new(wallet::read_keypair(&config.jito.trading_keypair)?);
    info!(trading_wallet = %trading_keypair.pubkey(), "keypair loaded");

    let rpc_client = Arc::new(RpcClient::new(config.rpc.url.clone()));

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
                "WSOL ATA not found -- run: spl-token wrap <amount>"
            );
        }
    }

    let metrics = metrics::Metrics::new();
    metrics.spawn_reporter();
    info!("pipeline metrics reporter started (120s window)");

    let blockhash_cache = BlockhashCache::new(rpc_client.clone());
    info!("blockhash cache initialized (refresh every 300ms)");

    let tip_pubkeys = transaction::jito_tip_pubkeys();
    let alt_cache = AltCache::new(tip_pubkeys);
    info!("ALT cache initialized");

    let metis = metis::MetisClient::new(&config.metis.url, config.performance.quote_timeout_ms);

    let jito_client = Arc::new(jito::JitoClient::new(&config.jito.urls, &config.jito.uuid));
    info!(
        regions = config.jito.urls.len(),
        urls = ?config.jito.urls,
        "Jito multi-region client ready"
    );

    // Arc<Mutex<>> so spawned sim tasks can acquire after sim passes.
    let jito_limiter = Arc::new(Mutex::new(
        RateLimiter::new(config.jito.max_bundles_per_second),
    ));

    // Optional second submission path: SearcherService gRPC.
    // Each path has its own rate limiter; the arbitrage dispatcher tries
    // REST first and falls back to gRPC when REST is saturated.
    let (jito_grpc_client, jito_grpc_limiter) = if config.jito_grpc.enabled {
        match jito_grpc::JitoGrpcClient::new(
            &config.jito_grpc.endpoints,
            &config.jito_grpc.auth_keypair,
        )
        .await
        {
            Ok(client) => {
                info!(
                    endpoints = config.jito_grpc.endpoints.len(),
                    rate = config.jito_grpc.max_bundles_per_second,
                    "Jito gRPC searcher client ready"
                );
                let limiter = Arc::new(Mutex::new(RateLimiter::new(
                    config.jito_grpc.max_bundles_per_second,
                )));
                (Some(Arc::new(client)), Some(limiter))
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Jito gRPC init failed, continuing with REST-only path"
                );
                (None, None)
            }
        }
    } else {
        info!("Jito gRPC path disabled via config");
        (None, None)
    };

    let (sim_cache, sim_pool) = if config.simulation.enabled {
        let cache = account_cache::AccountCache::new(rpc_client.clone());

        // Seed the Yellowstone slot with a one-time RPC call so sims have
        // a valid Clock.slot before the first gRPC message arrives.
        match rpc_client.get_slot() {
            Ok(s) => {
                cache.seed_slot(s);
                info!(initial_slot = s, "sim Clock seeded from RPC (one-time)");
            }
            Err(e) => warn!(error = %e, "initial get_slot failed, sims start at slot 0"),
        }

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

        let mut warm: Vec<solana_sdk::pubkey::Pubkey> = token_mints
            .iter()
            .filter_map(|s| solana_sdk::pubkey::Pubkey::try_from(s.as_str()).ok())
            .collect();
        warm.push(wsol_mint);
        warm.push(wsol_ata);
        warm.push(trading_keypair.pubkey());
        for mint_str in &token_mints {
            if let Ok(mint) = solana_sdk::pubkey::Pubkey::try_from(mint_str.as_str()) {
                let ata = spl_associated_token_account::get_associated_token_address(
                    &trading_keypair.pubkey(),
                    &mint,
                );
                warm.push(ata);
            }
        }
        cache.prefetch(&warm);
        info!(warmed = cache.len(), "account cache pre-warmed");

        // SimulatorPool gets its slot from the Yellowstone stream — zero RPC.
        let pool = litesvm_sim::SimulatorPool::new(
            config.simulation.workers,
            &config.simulation.so_dir,
            wsol_ata,
            config.simulation.fail_closed,
            cache.stream_slot(),
        )?;
        (Some(Arc::new(cache)), Some(Arc::new(pool)))
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
            &jito_limiter,
            &blockhash_cache,
            &alt_cache,
            sim_cache.as_ref(),
            sim_pool.as_ref(),
            jito_grpc_client.as_ref(),
            jito_grpc_limiter.as_ref(),
            &metrics,
        )
        .await
        {
            error!(error = %e, "scan cycle error");
        }
    }
}
