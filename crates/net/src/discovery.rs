//! Discovery Module.

use eyre::Result;
use std::time::Duration;
use tokio::{
    sync::mpsc::{channel, Receiver},
    time::sleep,
};
use tracing::{trace, warn};

use discv5::{
    enr::{CombinedKey, Enr, NodeId},
    ConfigBuilder, Discv5, ListenConfig,
};

use crate::{
    bootnodes::BOOTNODES,
    op_enr::OpStackEnr,
    types::{NetworkAddress, Peer},
};

/// The number of peers to buffer in the channel.
const DISCOVERY_PEER_CHANNEL_SIZE: usize = 256;

/// Discovery service builder.
#[derive(Debug, Default, Clone)]
pub struct DiscoveryBuilder {
    /// The discovery service address.
    address: Option<NetworkAddress>,
    /// The chain ID of the network.
    chain_id: Option<u64>,
}

impl DiscoveryBuilder {
    /// Creates a new discovery builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the discovery service address.
    pub fn with_address(mut self, address: NetworkAddress) -> Self {
        self.address = Some(address);
        self
    }

    /// Sets the chain ID of the network.
    pub fn with_chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// Builds a [DiscoveryDriver].
    pub fn build(self) -> Result<DiscoveryDriver> {
        let addr = self.address.ok_or_else(|| eyre::eyre!("address not set"))?;
        let chain_id = self.chain_id.ok_or_else(|| eyre::eyre!("chain ID not set"))?;
        let opstack = OpStackEnr::new(chain_id, 0);
        let opstack_data: Vec<u8> = opstack.into();

        let key = CombinedKey::generate_secp256k1();
        let enr = Enr::builder().add_value_rlp("opstack", opstack_data.into()).build(&key)?;
        let listen_config = ListenConfig::from_ip(addr.ip.into(), addr.port);
        let config = ConfigBuilder::new(listen_config).build();

        let disc = Discv5::new(enr, key, config)
            .map_err(|_| eyre::eyre!("could not create disc service"))?;

        Ok(DiscoveryDriver::new(disc, chain_id))
    }
}

/// The discovery driver handles running the discovery service.
pub struct DiscoveryDriver {
    /// The [Discv5] discovery service.
    disc: Discv5,
    /// The chain ID of the network.
    chain_id: u64,
}

impl DiscoveryDriver {
    /// Instantiates a new [DiscoveryDriver].
    pub fn new(disc: Discv5, chain_id: u64) -> Self {
        Self { disc, chain_id }
    }

    /// Spawns a new [Discv5] discovery service in a new tokio task.
    ///
    /// Returns a [Receiver] to receive [Peer] structs.
    ///
    /// ## Errors
    ///
    /// Returns an error if the address or chain ID is not set
    /// on the [DiscoveryBuilder].
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use op_net::{discovery::DiscoveryBuilder, types::NetworkAddress};
    /// use std::net::Ipv4Addr;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let network_addr = NetworkAddress { ip: Ipv4Addr::new(127, 0, 0, 1), port: 9000 };
    ///     let mut discovery = DiscoveryBuilder::new()
    ///         .with_address(network_addr)
    ///         .with_chain_id(10) // OP Mainnet chain id
    ///         .build()
    ///         .expect("Failed to build discovery service");
    ///     let mut peer_recv = discovery.start().expect("Failed to start discovery service");
    ///
    ///     loop {
    ///         if let Some(peer) = peer_recv.recv().await {
    ///             println!("Received peer: {:?}", peer);
    ///         }
    ///     }
    /// }
    /// ```
    pub fn start(mut self) -> Result<Receiver<Peer>> {
        // Clone the bootnodes since the spawned thread takes mutable ownership.
        let bootnodes = BOOTNODES.clone();

        // Create a multi-producer, single-consumer (mpsc) channel to receive
        // peers bounded by `DISCOVERY_PEER_CHANNEL_SIZE`.
        let (sender, recv) = channel::<Peer>(DISCOVERY_PEER_CHANNEL_SIZE);

        tokio::spawn(async move {
            bootnodes.into_iter().for_each(|enr| _ = self.disc.add_enr(enr));
            self.disc.start().await.unwrap();

            trace!("Started peer discovery");

            loop {
                let target = NodeId::random();
                match self.disc.find_node(target).await {
                    Ok(nodes) => {
                        let peers = nodes
                            .iter()
                            .filter(|node| OpStackEnr::is_valid_node(node, self.chain_id))
                            .flat_map(Peer::try_from);

                        for peer in peers {
                            _ = sender.send(peer).await;
                        }
                    }
                    Err(err) => {
                        warn!("discovery error: {:?}", err);
                    }
                }

                sleep(Duration::from_secs(10)).await;
            }
        });

        Ok(recv)
    }
}
