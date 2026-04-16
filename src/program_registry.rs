//! Mapping between Metis DEX_PROGRAM_IDS and the .so binaries the user has
//! placed on disk. These .so files are loaded into LiteSVM at startup so the
//! simulator can execute real on-chain bytecode (not a mathematical model).
//!
//! Every entry is (on-chain program id, filename inside `simulation.so_dir`).
//! If a filename is empty or the file is missing, that program is skipped
//! and any tx touching it will bypass local simulation (logged as a miss).
//!
//! REMOVED (proprietary "dark" PMMs that cannot be simulated locally):
//!   - Tessera V         TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH
//!   - GoonFi V2         goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE
//!   - SolFi             SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe
//!   - SolFi V2          SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF
//!   - ZeroFi            ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY
//!   - REALQq (unknown)  REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2
//!
//! These DEXes rely on same-slot oracle freshness in the Jito auction. Their
//! pool/oracle accounts carry an internal `last_update_slot` that is frozen
//! at Yellowstone snapshot time, and they fail their entrypoint silently
//! (Custom error codes, no msg!) before LiteSVM can do anything about it.
//! See FORBIDDEN_DEX_PROGRAM_IDS below for the matching route-plan filter.

pub const PROGRAMS: &[(&str, &str)] = &[
    // --- Aggregator (top-level program the swap_instruction targets) ---
    // Metis is a Jupiter fork: its swap_instruction carries Jupiter v6's
    // program_id. Without this loaded as executable, LiteSVM rejects the tx
    // with "Program account JUP6Lkb... is not executable".
    ("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "Jupiter_Aggregator_v6.so"),

    // --- high confidence ---
    ("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", "Meteora_DAMM_v2.so"),
    ("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "Raydium_Concentrated_Liquidity.so"),
    ("MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms", "Manifest.so"),
    ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "Whirlpools_Program.so"),
    ("BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "BonkSwap.so"),
    ("ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA", "AlphaQ.so"),
    ("fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9", "Fusion_AMM.so"),
    ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "Meteora_DLMM_Program.so"),
    ("DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm", "1Dex_Program.so"),
    ("AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", "Aquifer.so"),
    ("MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky", "Mercurial_Stable_Swap.so"),
    ("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "Raydium_CPMM.so"),
    ("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "Meteora_Pools_Program.so"),

    // --- best-effort (verify against solscan / program label) ---
    ("HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq", "Byreal:_CLMM.so"),
    ("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi", "Invariant_Swap.so"),
    ("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "PancakeSwap.so"),
    ("HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "Meteora_Vault_Program.so"),
];

/// Program ids the bot must REFUSE to route through, no matter what Metis
/// returns. Defense-in-depth: even if the Metis server's DEX filter still
/// has these enabled, every quote whose route_plan touches one of these
/// programs is dropped before we ever spend a base fee. See the module
/// header for the full reasoning.
pub const FORBIDDEN_DEX_PROGRAM_IDS: &[&str] = &[
    "TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH",
    "goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE",
    "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe",
    "SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF",
    "ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY",
    "REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2",
];

/// Jupiter/Metis DEX label substrings we ask the server to exclude up-front.
/// These are matched case-insensitively against the `swapInfo.label` field
/// of every `route_plan` entry. Combined with `excludeDexes` on the /quote
/// URL this should keep the routes from ever being proposed.
pub const FORBIDDEN_DEX_LABELS: &[&str] = &[
    "Tessera",
    "GoonFi",
    "Goonfi",
    "SolFi",
    "ZeroFi",
];

/// Program ids that the simulator should subscribe to on Yellowstone gRPC so
/// their pool accounts end up in the hot cache. This is the superset of
/// `PROGRAMS` as strings (unchanged if the user edits one side).
pub fn all_program_ids() -> Vec<String> {
    PROGRAMS.iter().map(|(id, _)| (*id).to_string()).collect()
}
