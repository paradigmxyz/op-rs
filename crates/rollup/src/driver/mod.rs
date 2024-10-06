//! Rollup Node Driver

use std::{fmt::Debug, sync::Arc};

use eyre::{bail, eyre, Result};
use kona_derive::{
    errors::{PipelineError, PipelineErrorKind},
    traits::{BlobProvider, ChainProvider, L2ChainProvider},
};
use kona_providers::{
    blob_provider::DurableBlobProvider, InMemoryChainProvider, LayeredBlobProvider, Pipeline,
    StepResult,
};
use kona_providers_alloy::{AlloyChainProvider, AlloyL2ChainProvider, OnlineBlobProviderBuilder};
use op_alloy_genesis::RollupConfig;
use op_alloy_protocol::{BlockInfo, L2BlockInfo};
use reth::rpc::types::engine::JwtSecret;
use reth_exex::ExExContext;
use reth_node_api::FullNodeComponents;
use tracing::{debug, error, info, trace, warn};

use crate::{
    cli::ValidationMode,
    new_rollup_pipeline,
    validator::{EngineApiValidator, TrustedValidator},
    AttributesValidator, HeraArgsExt, RollupPipeline,
};

mod context;
use context::{ChainNotification, DriverContext, ExExHeraContext, StandaloneHeraContext};

mod cursor;
use cursor::SyncCursor;

/// The Rollup Driver entrypoint.
#[derive(Debug)]
pub struct Driver<DC, CP, BP> {
    /// The rollup configuration
    cfg: Arc<RollupConfig>,
    /// The context of the node
    ctx: DC,
    /// The L1 chain provider
    l1_chain_provider: CP,
    /// The L1 blob provider
    blob_provider: BP,
    /// The L2 chain provider
    l2_chain_provider: AlloyL2ChainProvider,
    /// Cursor to keep track of the L2 tip
    cursor: SyncCursor,
    /// The validator to verify newly derived L2 attributes
    validator: Box<dyn AttributesValidator>,
}

impl<N: FullNodeComponents> Driver<ExExHeraContext<N>, InMemoryChainProvider, LayeredBlobProvider> {
    /// Create a new Hera Execution Extension Driver
    pub fn exex(ctx: ExExContext<N>, args: HeraArgsExt, cfg: Arc<RollupConfig>) -> Self {
        let chain_provider = InMemoryChainProvider::with_capacity(args.l1_chain_cache_size);
        let blob_provider = LayeredBlobProvider::new(
            args.l1_beacon_client_url.clone(),
            args.l1_blob_archiver_url.clone(),
        );

        // The ExEx Hera context is responsible for handling notifications from the execution
        // extension, and will automatically cache L1 blocks as they come in to make them available
        // to the derivation pipeline's L1 chain provider.
        let exex_ctx = ExExHeraContext::new(ctx, chain_provider.clone());

        Self::with_components(exex_ctx, args, cfg, chain_provider, blob_provider)
    }
}

impl Driver<StandaloneHeraContext, AlloyChainProvider, DurableBlobProvider> {
    /// Create a new Standalone Hera Driver
    pub async fn standalone(args: HeraArgsExt, cfg: Arc<RollupConfig>) -> Result<Self> {
        let chain_provider = AlloyChainProvider::new_http(args.l1_rpc_url.clone());
        let blob_provider = OnlineBlobProviderBuilder::new()
            .with_primary(args.l1_beacon_client_url.as_str().trim_end_matches('/').to_string())
            .with_fallback(
                args.l1_blob_archiver_url
                    .clone()
                    .map(|url| url.as_str().trim_end_matches('/').to_string()),
            )
            .build();

        // The Standalone Hera context is responsible for handling notifications from the node.
        // Currently there is no cache layer for L1 data as it is assumed that it will be fetched
        // from the L1 chain provider directly.
        let standalone_ctx = StandaloneHeraContext::new(args.l1_rpc_url.clone()).await?;

        Ok(Self::with_components(standalone_ctx, args, cfg, chain_provider, blob_provider))
    }
}

impl<DC, CP, BP> Driver<DC, CP, BP>
where
    DC: DriverContext,
    CP: ChainProvider + Clone + Send + Sync + Debug + 'static,
    BP: BlobProvider + Clone + Send + Sync + Debug + 'static,
{
    /// Create a new Hera Driver with the provided components.
    fn with_components(
        ctx: DC,
        args: HeraArgsExt,
        cfg: Arc<RollupConfig>,
        l1_chain_provider: CP,
        blob_provider: BP,
    ) -> Self {
        let cursor = SyncCursor::new(cfg.channel_timeout);
        let validator: Box<dyn AttributesValidator> = match args.validation_mode {
            ValidationMode::Trusted => Box::new(TrustedValidator::new_http(
                args.l2_rpc_url.clone(),
                cfg.canyon_time.unwrap_or(0),
            )),
            ValidationMode::EngineApi => Box::new(EngineApiValidator::new_http(
                args.l2_engine_api_url.expect("Missing L2 engine API URL"),
                match args.l2_engine_jwt_secret.as_ref() {
                    Some(fpath) => JwtSecret::from_file(fpath).expect("Invalid L2 JWT secret file"),
                    None => panic!("Missing L2 engine JWT secret"),
                },
            )),
        };
        let l2_chain_provider = AlloyL2ChainProvider::new_http(args.l2_rpc_url, cfg.clone());

        Self { cfg, ctx, l1_chain_provider, blob_provider, l2_chain_provider, cursor, validator }
    }

    /// Wait for the L2 genesis' corresponding L1 block to be available in the L1 chain.
    async fn wait_for_l2_genesis_l1_block(&mut self) -> Result<()> {
        loop {
            if let Some(notification) = self.ctx.recv_notification().await {
                if let Some(new_chain) = notification.new_chain() {
                    let tip = new_chain.tip();

                    if let Err(err) = self.ctx.send_processed_tip_event(tip) {
                        bail!("Failed to send processed tip event: {:?}", err);
                    }

                    if tip >= self.cfg.genesis.l1.number {
                        break Ok(());
                    } else {
                        debug!("Chain not yet synced to rollup genesis. L1 block number: {}", tip);
                    }
                }
            }
        }
    }

    /// Initialize the rollup pipeline from the driver's components.
    async fn init_pipeline(&mut self) -> Result<RollupPipeline<CP, BP>> {
        // Fetch the current L2 tip and its corresponding L1 origin block
        let l2_tip = self.l2_chain_provider.latest_block_number().await.map_err(|e| eyre!(e))?;
        let (l2_tip_l1_origin, l2_tip_block_info) = self.fetch_new_tip(l2_tip).await?;

        // Advance the cursor to the L2 tip before starting the pipeline
        self.cursor.advance(l2_tip_l1_origin, l2_tip_block_info);

        Ok(new_rollup_pipeline(
            self.cfg.clone(),
            self.l1_chain_provider.clone(),
            self.blob_provider.clone(),
            self.l2_chain_provider.clone(),
            l2_tip_l1_origin,
        ))
    }

    /// Advance the pipeline to the next L2 block.
    ///
    /// Returns `true` if the pipeline can move forward again, `false` otherwise.
    async fn step(&mut self, pipeline: &mut RollupPipeline<CP, BP>) -> bool {
        let l2_tip = self.cursor.tip();

        match pipeline.step(l2_tip).await {
            StepResult::PreparedAttributes => trace!("Prepared new attributes"),
            StepResult::AdvancedOrigin => trace!("Advanced origin"),
            StepResult::OriginAdvanceErr(err) => warn!("Could not advance origin: {:?}", err),
            StepResult::StepFailed(err) => match err {
                PipelineErrorKind::Temporary(tmp) => match tmp {
                    PipelineError::NotEnoughData => debug!("Not enough data to advance pipeline"),
                    _ => error!("Unexpected temporary error stepping pipeline: {:?}", tmp),
                },
                other => error!("Error stepping derivation pipeline: {:?}", other),
            },
        }

        let derived_attributes = if let Some(attributes) = pipeline.peek() {
            match self.validator.validate(attributes).await {
                Ok(true) => {
                    trace!("Validated payload attributes");
                    pipeline.next().expect("Peeked attributes must be available")
                }
                Ok(false) => {
                    error!("Failed payload attributes validation");
                    // TODO: allow users to specify how they want to treat invalid payloads.
                    // In the default scenario we just log an error and continue.
                    return false;
                }
                Err(err) => {
                    error!("Error while validating payload attributes: {:?}", err);
                    return false;
                }
            }
        } else {
            debug!("No attributes available to validate");
            return false;
        };

        let derived = derived_attributes.parent.block_info.number + 1;
        let (new_l1_origin, new_l2_tip) = match self.fetch_new_tip(derived).await {
            Ok(tip_info) => tip_info,
            Err(err) => {
                // TODO: add a retry mechanism?
                error!("Failed to fetch new tip: {:?}", err);
                return false;
            }
        };

        // Perform a sanity check on the new tip
        if new_l2_tip.block_info.number != derived {
            error!("Expected L2 block number {} but got {}", derived, new_l2_tip.block_info.number);
            return false;
        }

        // Advance the cursor to the new L2 block
        self.cursor.advance(new_l1_origin, new_l2_tip);
        info!("Advanced derivation pipeline to L2 block: {}", derived);
        true
    }

    /// Fetch the new L2 tip and L1 origin block info for the given L2 block number.
    async fn fetch_new_tip(&mut self, l2_tip: u64) -> Result<(BlockInfo, L2BlockInfo)> {
        let l2_block = self.l2_chain_provider.l2_block_info_by_number(l2_tip).await?;

        let l1_origin = self
            .l1_chain_provider
            .block_info_by_number(l2_block.l1_origin.number)
            .await
            .map_err(|e| eyre!(e.to_string()))?;

        Ok((l1_origin, l2_block))
    }

    /// Handle a chain notification from the driver context.
    async fn handle_notification(
        &mut self,
        notification: ChainNotification,
        pipeline: &mut RollupPipeline<CP, BP>,
    ) -> Result<()> {
        if let Some(reverted_chain) = notification.reverted_chain() {
            // The reverted chain contains the list of blocks that were invalidated by the
            // reorg. we need to reset the cursor to the last canonical block, which corresponds
            // to the block before the reorg happened.
            let fork_block = reverted_chain.fork_block();

            // Find the last known L2 block that is still valid after the reorg,
            // and reset the cursor and pipeline to it.
            let (l2_safe_tip, l2_safe_tip_l1_origin) = self.cursor.reset(fork_block);

            warn!("Reverting derivation pipeline to L2 block: {}", l2_safe_tip.number);
            if let Err(e) = pipeline.reset(l2_safe_tip, l2_safe_tip_l1_origin).await {
                bail!("Failed to reset pipeline: {:?}", e);
            }
        }

        if let Some(new_chain) = notification.new_chain() {
            let tip = new_chain.tip();

            if let Err(err) = self.ctx.send_processed_tip_event(tip) {
                bail!("Failed to send processed tip event: {:?}", err);
            }
        }

        Ok(())
    }

    /// Starts the Hera derivation loop and tries to advance the driver to
    /// the L2 chain tip.
    ///
    /// # Errors
    ///
    /// This function should never error. If it does, it means the entire driver
    /// will shut down. If running as ExEx, the entire L1 node + all other running
    /// execution extensions will be shutdown as well.
    pub async fn start(mut self) -> Result<()> {
        // Step 1: Wait for the L2 origin block to be available
        self.wait_for_l2_genesis_l1_block().await?;
        info!("L1 chain synced to the rollup genesis block");

        // Step 2: Initialize the rollup pipeline
        let mut pipeline = self.init_pipeline().await?;
        info!("Derivation pipeline initialized");

        // Step 3: Start the processing loop
        loop {
            // Try to advance the pipeline until there's no more data to process
            if self.step(&mut pipeline).await {
                continue;
            }

            // Handle any incoming notifications from the context
            if let Some(notification) = self.ctx.recv_notification().await {
                self.handle_notification(notification, &mut pipeline).await?;
            }
        }
    }
}
