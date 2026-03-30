use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    system_instruction,
    transaction::VersionedTransaction,
};
use std::str::FromStr;

use crate::metis::{InstructionData, SwapInstructionsResponse};

/// Jito tip account addresses — pick one at random for each bundle.
/// Per Jito docs: do NOT use ALTs for tip accounts.
const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Convert a Metis instruction into a Solana SDK Instruction.
fn to_sdk_instruction(ix: &InstructionData) -> Result<Instruction> {
    let program_id = Pubkey::from_str(&ix.program_id)?;
    let accounts: Vec<AccountMeta> = ix
        .accounts
        .iter()
        .map(|a| {
            let pubkey = Pubkey::from_str(&a.pubkey).expect("invalid pubkey in instruction");
            if a.is_writable {
                AccountMeta::new(pubkey, a.is_signer)
            } else {
                AccountMeta::new_readonly(pubkey, a.is_signer)
            }
        })
        .collect();
    let data = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &ix.data,
    )
    .context("failed to decode instruction data")?;
    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}

/// Calculate the Jito tip amount.
/// Per Jito docs: minimum tip is 1000 lamports for bundles.
pub fn calculate_tip(
    profit_lamports: u64,
    tip_percent: f64,
    tip_min: u64,
    tip_max: u64,
) -> u64 {
    let dynamic_tip = (profit_lamports as f64 * tip_percent) as u64;
    dynamic_tip.max(tip_min).min(tip_max)
}

/// Build a versioned transaction with exactly 3 instructions:
///
/// #1 - Compute Budget: SetComputeUnitLimit (manual, based on hop count from config)
/// #2 - Jupiter Aggregator V6: route/route_v2 (single instruction for entire circular arb)
/// #3 - System Program: Transfer (Jito tip, MUST be last)
///
/// The merged route handles all swap hops internally (e.g. WSOL→USDC→hyUSD→WSOL).
/// Setup/cleanup instructions are NOT needed because:
/// - useSharedAccounts=false in circular arb mode
/// - WSOL ATA pre-exists (verified at startup)
///
/// CU limit is set manually (not via Metis dynamicComputeUnitLimit) to avoid
/// an extra RPC simulation call that adds latency.
///
/// Tip account is NEVER placed in ALT — Jito requires direct write-lock visibility.
pub fn build_arb_transaction(
    swap_ixs: &SwapInstructionsResponse,
    payer: &Keypair,
    tip_lamports: u64,
    cu_limit: u32,
    recent_blockhash: Hash,
    rpc_client: &RpcClient,
) -> Result<VersionedTransaction> {
    let mut instructions: Vec<Instruction> = Vec::new();

    // #1 — SetComputeUnitLimit built manually from hop count (config-driven).
    // dynamicComputeUnitLimit=false in Metis, so we don't rely on Metis for CU.
    // This avoids an extra RPC simulation call that Metis would make.
    let cu_limit_ix = Instruction {
        program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111")?,
        accounts: vec![],
        data: {
            let mut data = vec![0x02]; // SetComputeUnitLimit discriminator
            data.extend_from_slice(&cu_limit.to_le_bytes());
            data
        },
    };
    instructions.push(cu_limit_ix);

    // #2 — Single route_v2 for the entire circular swap
    instructions.push(to_sdk_instruction(&swap_ixs.swap_instruction)?);

    // #3 — Jito tip (MUST be last instruction, MUST NOT be in ALT)
    let tip_account = {
        let mut rng = rand::thread_rng();
        let addr = JITO_TIP_ACCOUNTS.choose(&mut rng).unwrap();
        Pubkey::from_str(addr)?
    };
    #[allow(deprecated)]
    instructions.push(system_instruction::transfer(
        &payer.pubkey(),
        &tip_account,
        tip_lamports,
    ));

    // Collect Jito tip account pubkeys to exclude from ALT resolution
    let tip_pubkeys: Vec<Pubkey> = JITO_TIP_ACCOUNTS
        .iter()
        .filter_map(|a| Pubkey::from_str(a).ok())
        .collect();

    // Fetch ALTs from Metis response
    let mut alt_addresses: Vec<Pubkey> = Vec::new();
    for addr in &swap_ixs.address_lookup_table_addresses {
        let pubkey = Pubkey::from_str(addr)?;
        if !alt_addresses.contains(&pubkey) {
            alt_addresses.push(pubkey);
        }
    }

    let mut address_lookup_tables: Vec<AddressLookupTableAccount> = Vec::new();
    for alt_pubkey in &alt_addresses {
        let raw_account = rpc_client
            .get_account(alt_pubkey)
            .with_context(|| format!("failed to fetch ALT {}", alt_pubkey))?;

        let mut addresses = deserialize_alt_addresses(&raw_account.data)?;

        // Remove Jito tip accounts from ALT entries to prevent them
        // being compressed into ALT references (Jito needs direct write-lock)
        addresses.retain(|addr| !tip_pubkeys.contains(addr));

        let alt_account = AddressLookupTableAccount {
            key: *alt_pubkey,
            addresses,
        };
        address_lookup_tables.push(alt_account);
    }

    // Build VersionedTransaction v0
    let message = v0::Message::try_compile(
        &payer.pubkey(),
        &instructions,
        &address_lookup_tables,
        recent_blockhash,
    )
    .context("failed to compile v0 message")?;

    let versioned_message = VersionedMessage::V0(message);
    let tx = VersionedTransaction::try_new(versioned_message, &[payer])
        .context("failed to sign versioned transaction")?;

    Ok(tx)
}

/// Deserialize the addresses stored in an Address Lookup Table account.
pub fn deserialize_alt_addresses(data: &[u8]) -> Result<Vec<Pubkey>> {
    const HEADER_SIZE: usize = 56;
    if data.len() < HEADER_SIZE {
        anyhow::bail!("ALT account data too short: {} bytes", data.len());
    }
    let addresses_data = &data[HEADER_SIZE..];
    if addresses_data.len() % 32 != 0 {
        anyhow::bail!(
            "ALT addresses data has invalid length: {} (not a multiple of 32)",
            addresses_data.len()
        );
    }
    let addresses: Vec<Pubkey> = addresses_data
        .chunks_exact(32)
        .map(|chunk| Pubkey::new_from_array(chunk.try_into().unwrap()))
        .collect();
    Ok(addresses)
}
