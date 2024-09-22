//! Runner for OP-RS execution extensions

#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/paradigmxyz/op-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::Parser;
use eyre::{bail, Result};
use reth::{args::utils::DefaultChainSpecParser, cli::Cli};
use reth_node_ethereum::EthereumNode;
use tracing::{info, warn};

use rollup::{Driver, HeraArgsExt, HERA_EXEX_ID};

/// The Reth CLI arguments with optional Hera Execution Extension support.
#[derive(Debug, Clone, Parser)]
pub(crate) struct RethArgsExt {
    /// Whether to install the Hera Execution Extension.
    ///
    /// Additional Hera-specific flags will be parsed if this flag is set.
    #[clap(long, default_value_t = false)]
    pub hera: bool,
    /// The Hera Execution Extension configuration.
    ///
    /// This is only used if the `hera` flag is set.
    #[clap(flatten)]
    pub hera_config: Option<HeraArgsExt>,
}

fn main() -> Result<()> {
    Cli::<DefaultChainSpecParser, RethArgsExt>::parse().run(|builder, args| async move {
        if args.hera {
            info!("Running Reth with the Hera Execution Extension");
            let Some(hera_args) = args.hera_config else {
                bail!("Hera Execution Extension configuration is required when the `hera` flag is set");
            };

            let cfg = hera_args.get_l2_config()?;
            let node = EthereumNode::default();
            let hera = move |ctx| async { Ok(Driver::exex(ctx, hera_args, cfg).await.start()) };
            let handle = builder.node(node).install_exex(HERA_EXEX_ID, hera).launch().await?;
            handle.wait_for_node_exit().await
        } else {
            warn!("Running Reth without the Hera Execution Extension");
            let node = EthereumNode::default();
            let handle = builder.node(node).launch().await?;
            handle.wait_for_node_exit().await
        }
    })
}
