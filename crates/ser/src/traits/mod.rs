//! Traits for serialization.

pub mod compressor;
pub use compressor::Compressor;

// Re-export op-alloy-rpc-jsonrpsee crates
pub use op_alloy_rpc_jsonrpsee::traits::{
    OpAdminApiClient, OpAdminApiServer, OpP2PApiClient, OpP2PApiServer, RollupNodeClient,
    RollupNodeServer,
};
