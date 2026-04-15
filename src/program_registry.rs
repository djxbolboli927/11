//! Mapping between Metis DEX_PROGRAM_IDS and the .so binaries the user has
//! placed on disk. These .so files are loaded into LiteSVM at startup so the
//! simulator can execute real on-chain bytecode (not a mathematical model).
//!
//! Every entry is (on-chain program id, filename inside `simulation.so_dir`).
//! If a filename is empty or the file is missing, that program is skipped
//! and any tx touching it will bypass local simulation (logged as a miss).

/// Best-effort mapping. The 23 program ids below match the DEX filter
/// currently configured in Metis. Filenames correspond to the .so files the
/// user downloaded to `/home/soluser/m/so/`. A few of the lesser-known ids
/// are labelled TODO — the bot will simply skip sim for routes that touch
/// an un-mapped program until the mapping is filled in.
pub const PROGRAMS: &[(&str, &str)] = &[
    // --- high confidence ---
    ("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", "Meteora_DAMM_v2.so"),
    ("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "Raydium_Concentrated_Liquidity.so"),
    ("MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms", "Manifest.so"),
    ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "Whirlpool_Program.so"),
    ("BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "BonkSwap.so"),
    ("ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA", "AlphaQ.so"),
    ("TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH", "Tessera_V.so"),
    ("fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9", "Fusion_AMM.so"),
    ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "Meteora_DLMM_Program.so"),
    ("DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm", "1Dex_Program.so"),
    ("ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY", "ZeroFi.so"),
    ("goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE", "GoonFi_V2.so"),
    ("AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", "Aquifier.so"),
    ("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe", "SoFi.so"),
    ("SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF", "SolFi_V2.so"),
    ("MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky", "Mercurial_Stable_Swap.so"),
    ("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "Raydium_CPMM.so"),
    ("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "Meteora_Pools_Program.so"),

    // --- best-effort (verify against solscan / program label) ---
    ("HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq", "Byreal_CLMM.so"),
    ("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi", "Invariant_Swap.so"),
    ("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "PancakeSwap.so"),
    ("HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "Meteora_Vault_Prog.so"),
    ("REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2", "Orca_Token_Swap_V2.so"),
];

/// Program ids that the simulator should subscribe to on Yellowstone gRPC so
/// their pool accounts end up in the hot cache. This is the superset of
/// `PROGRAMS` as strings (unchanged if the user edits one side).
pub fn all_program_ids() -> Vec<String> {
    PROGRAMS.iter().map(|(id, _)| (*id).to_string()).collect()
}
