//! Contains the RPC Definition

use alloy_eips::BlockNumberOrTag;
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use op_alloy_rpc_jsonrpsee::traits::RollupNodeServer;
use op_alloy_rpc_types::{
    config::RollupConfig, output::OutputResponse, safe_head::SafeHeadResponse, sync::SyncStatus,
};
use tracing::trace;

/// An implementation of the [`RollupNodeServer`] trait.
#[derive(Debug, Clone)]
pub struct RollupNodeRpc {
    /// The version of the node.
    version: String,
}

#[async_trait]
impl RollupNodeServer for RollupNodeRpc {
    async fn op_output_at_block(
        &self,
        block_number: BlockNumberOrTag,
    ) -> RpcResult<OutputResponse> {
        trace!("op_output_at_block: {:?}", block_number);
        unimplemented!()
    }

    async fn op_safe_head_at_l1_block(
        &self,
        block_number: BlockNumberOrTag,
    ) -> RpcResult<SafeHeadResponse> {
        trace!("op_safe_head_at_l1_block: {:?}", block_number);
        unimplemented!()
    }

    async fn op_sync_status(&self) -> RpcResult<SyncStatus> {
        unimplemented!()
    }

    async fn op_rollup_config(&self) -> RpcResult<RollupConfig> {
        unimplemented!()
    }

    async fn op_version(&self) -> RpcResult<String> {
        Ok(self.version.clone())
    }
}
