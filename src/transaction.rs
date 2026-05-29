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

use crate::alt_cache::AltCache;
use crate::metis::{InstructionData, SwapInstructionsResponse};

/// Jito tip account addresses -- pick one at random for each bundle.
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

/// Return Jito tip account pubkeys (used by AltCache to filter them out).
pub fn jito_tip_pubkeys() -> Vec<Pubkey> {
    JITO_TIP_ACCOUNTS
        .iter()
        .filter_map(|a| Pubkey::from_str(a).ok())
        .collect()
}

/// Overwrite `quoted_out_amount` in a Jupiter route_v2 instruction.
///
/// Binary layout of route_v2 instruction data (Anchor):
///   [0..8]   discriminator
///   [8..16]  in_amount       (u64 LE)
///   [16..24] quoted_out_amount (u64 LE)  ← patched here
///   [24..26] slippage_bps    (u16 LE)
///   ...
///
/// With slippage_bps=0 the EVM minimum = quoted_out_amount.
/// We set it to our own floor (input + fees) instead of Metis's quote.
fn patch_swap_min_out(ix: &InstructionData, min_out: u64) -> Result<InstructionData> {
    let mut data = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &ix.data,
    )
    .context("failed to decode swap instruction data")?;

    const QUOTED_OUT_OFFSET: usize = 16; // 8 discriminator + 8 in_amount
    if data.len() < QUOTED_OUT_OFFSET + 8 {
        anyhow::bail!(
            "swap instruction data too short ({} bytes) to patch quoted_out_amount",
            data.len()
        );
    }
    data[QUOTED_OUT_OFFSET..QUOTED_OUT_OFFSET + 8].copy_from_slice(&min_out.to_le_bytes());

    Ok(InstructionData {
        program_id: ix.program_id.clone(),
        accounts: ix.accounts.clone(),
        data: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &data),
    })
}

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

/// Build a versioned transaction with exactly 3 instructions:
///
/// #1 - Compute Budget: SetComputeUnitLimit
/// #2 - Jupiter Aggregator: route/route_v2 (entire circular arb)
/// #3 - System Program: Transfer (Jito tip, MUST be last)
///
/// Uses AltCache for ALT lookups (0ns on cache hit vs ~5ms RPC call).
/// Uses pre-cached blockhash (passed in, ~100ns read vs ~5ms RPC call).
///
/// `min_out` is written into the route_v2 instruction's `quoted_out_amount`
/// field (bytes 16-23 after the 8-byte Anchor discriminator).  With
/// slippage_bps=0 that field IS the on-chain minimum output, so the tx
/// reverts if and only if actual output < min_out.
pub fn build_arb_transaction(
    swap_ixs: &SwapInstructionsResponse,
    payer: &Keypair,
    tip_lamports: u64,
    cu_limit: u32,
    recent_blockhash: Hash,
    alt_cache: &AltCache,
    rpc_client: &RpcClient,
    min_out: u64,
) -> Result<VersionedTransaction> {
    let mut instructions: Vec<Instruction> = Vec::new();

    // #1 -- SetComputeUnitLimit
    let cu_limit_ix = Instruction {
        program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111")?,
        accounts: vec![],
        data: {
            let mut data = vec![0x02];
            data.extend_from_slice(&cu_limit.to_le_bytes());
            data
        },
    };
    instructions.push(cu_limit_ix);

    // #2 -- Single route_v2 for the entire circular swap
    // Patch quoted_out_amount so the on-chain minimum = min_out (our floor),
    // not Metis's optimistic quote which may no longer be achievable.
    let patched_swap_ix = patch_swap_min_out(&swap_ixs.swap_instruction, min_out)?;
    instructions.push(to_sdk_instruction(&patched_swap_ix)?);

    // #3 -- Jito tip (MUST be last, MUST NOT be in ALT)
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

    // Fetch ALTs via cache (instant on hit, RPC on first miss only)
    let mut alt_addresses: Vec<Pubkey> = Vec::new();
    for addr in &swap_ixs.address_lookup_table_addresses {
        let pubkey = Pubkey::from_str(addr)?;
        if !alt_addresses.contains(&pubkey) {
            alt_addresses.push(pubkey);
        }
    }

    let mut address_lookup_tables: Vec<AddressLookupTableAccount> = Vec::new();
    for alt_pubkey in &alt_addresses {
        let alt_account = alt_cache.get_or_fetch(alt_pubkey, rpc_client)?;
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
