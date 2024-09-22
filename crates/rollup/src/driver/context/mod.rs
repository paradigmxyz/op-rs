//! Context abstraction for the rollup driver.

use std::{collections::BTreeMap, sync::Arc};

use alloy::{
    consensus::TxEnvelope,
    primitives::{BlockNumber, U256},
    rpc::types::Block,
};
use async_trait::async_trait;
use kona_providers::chain_provider::reth_to_alloy_tx;
use reth::rpc::types::{BlockTransactions, Header};
use reth_execution_types::Chain;
use reth_exex::ExExNotification;
use tokio::sync::mpsc::error::SendError;

mod exex;
pub use exex::ExExHeraContext;

mod standalone;
pub use standalone::StandaloneHeraContext;

/// Context for the rollup driver.
///
/// The context is responsible for handling notifications from the state of the
/// canonical chain updates (new blocks, reorgs, etc) and translating them into
/// events that the rollup driver can use to make progress.
#[async_trait]
pub trait DriverContext {
    /// Receives a notification from the execution client.
    async fn recv_notification(&mut self) -> Option<ChainNotification>;

    /// Sends an event indicating that the processed tip has been updated.
    fn send_processed_tip_event(&mut self, tip: BlockNumber) -> Result<(), SendError<BlockNumber>>;
}

/// A notification representing a chain of blocks that come from an execution client.
#[derive(Debug, Clone)]
pub enum ChainNotification {
    /// A new chain of blocks has been processed.
    New { new_blocks: Blocks },
    /// Some blocks have been reverted and are no longer part of the chain.
    Revert { old_blocks: Blocks },
    /// The chain has been reorganized with new canonical blocks.
    Reorg { old_blocks: Blocks, new_blocks: Blocks },
}

impl ChainNotification {
    /// Returns the new chain of blocks contained in the notification event.
    ///
    /// For reorgs, this returns the new chain of blocks that replaced the old one.
    pub fn new_chain(&self) -> Option<Blocks> {
        match self {
            ChainNotification::New { new_blocks } => Some(new_blocks.clone()),
            ChainNotification::Reorg { new_blocks, .. } => Some(new_blocks.clone()),
            ChainNotification::Revert { .. } => None,
        }
    }

    /// Returns the old chain of blocks contained in the notification event.
    ///
    /// For reorgs, this returns the old canonical chain that was reorged.
    pub fn reverted_chain(&self) -> Option<Blocks> {
        match self {
            ChainNotification::Revert { old_blocks } => Some(old_blocks.clone()),
            ChainNotification::Reorg { old_blocks, .. } => Some(old_blocks.clone()),
            ChainNotification::New { .. } => None,
        }
    }
}

/// A collection of blocks that form a chain.
#[derive(Debug, Clone, Default)]
pub struct Blocks(Arc<BTreeMap<BlockNumber, Block<TxEnvelope>>>);

impl Blocks {
    /// Returns the tip of the chain.
    pub fn tip(&self) -> BlockNumber {
        *self.0.last_key_value().expect("Blocks should have at least one block").0
    }

    /// Returns a reference to the block at the tip of the chain.
    pub fn tip_block(&self) -> &Block<TxEnvelope> {
        self.0.get(&self.tip()).expect("Blocks should have at least one block")
    }

    /// Returns the block number at the fork point of the chain (the block before the first).
    pub fn fork_block_number(&self) -> BlockNumber {
        let first = self.0.first_key_value().expect("Blocks should have at least one block").0;
        first.saturating_sub(1)
    }
}

impl From<Block<TxEnvelope>> for Blocks {
    fn from(value: Block<TxEnvelope>) -> Self {
        let mut blocks = BTreeMap::new();
        blocks.insert(value.header.number, value);
        Self(Arc::new(blocks))
    }
}

impl From<Vec<Block<TxEnvelope>>> for Blocks {
    fn from(value: Vec<Block<TxEnvelope>>) -> Self {
        let mut blocks = BTreeMap::new();
        for block in value {
            blocks.insert(block.header.number, block);
        }
        Self(Arc::new(blocks))
    }
}

impl From<Arc<Chain>> for Blocks {
    fn from(value: Arc<Chain>) -> Self {
        let mut blocks = BTreeMap::new();
        for (block_number, sealed_block) in value.blocks() {
            // from reth::primitives::SealedBlock to alloy::rpc::types::Block
            let block = Block {
                header: parse_reth_rpc_header(sealed_block),
                uncles: sealed_block.ommers.iter().map(|x| x.hash_slow()).collect(),
                transactions: BlockTransactions::Full(
                    sealed_block.transactions().flat_map(reth_to_alloy_tx).collect(),
                ),
                size: Some(U256::from(sealed_block.size())),
                withdrawals: sealed_block.withdrawals.clone().map(|w| w.into_inner()),
            };
            blocks.insert(*block_number, block);
        }
        Self(Arc::new(blocks))
    }
}

impl From<ExExNotification> for ChainNotification {
    fn from(value: ExExNotification) -> Self {
        match value {
            ExExNotification::ChainCommitted { new } => Self::New { new_blocks: new.into() },
            ExExNotification::ChainReverted { old } => Self::Revert { old_blocks: old.into() },
            ExExNotification::ChainReorged { old, new } => {
                Self::Reorg { old_blocks: old.into(), new_blocks: new.into() }
            }
        }
    }
}

// from reth::primitives::SealedBlock to alloy::rpc::types::Header
fn parse_reth_rpc_header(block: &reth::primitives::SealedBlock) -> Header {
    Header {
        hash: block.header.hash(),
        parent_hash: block.parent_hash,
        uncles_hash: block.ommers_hash,
        miner: block.beneficiary,
        state_root: block.state_root,
        transactions_root: block.transactions_root,
        receipts_root: block.receipts_root,
        logs_bloom: block.logs_bloom,
        difficulty: block.difficulty,
        number: block.number,
        gas_limit: block.gas_limit as u128,
        gas_used: block.gas_used as u128,
        timestamp: block.timestamp,
        total_difficulty: Some(block.difficulty),
        extra_data: block.extra_data.clone(),
        mix_hash: Some(block.mix_hash),
        nonce: Some(block.nonce.into()),
        base_fee_per_gas: block.base_fee_per_gas.map(|x| x as u128),
        withdrawals_root: block.withdrawals_root,
        blob_gas_used: block.blob_gas_used.map(|x| x as u128),
        excess_blob_gas: block.excess_blob_gas.map(|x| x as u128),
        parent_beacon_block_root: block.parent_beacon_block_root,
        requests_root: block.requests_root,
    }
}
