//! Peer Types

use discv5::enr::{CombinedKey, Enr};
use eyre::Result;
use libp2p::{multiaddr::Protocol, Multiaddr};
use std::net::{IpAddr, SocketAddr};

/// A wrapper around a peer's [SocketAddr].
#[derive(Debug, Clone)]
pub struct Peer {
    /// The peer's [SocketAddr].
    pub socket: SocketAddr,
}

impl TryFrom<&Enr<CombinedKey>> for Peer {
    type Error = eyre::Report;

    /// Converts an [Enr] to a Peer
    fn try_from(value: &Enr<CombinedKey>) -> Result<Self> {
        let ip = value.ip4().ok_or(eyre::eyre!("missing ip"))?;
        let port = value.tcp4().ok_or(eyre::eyre!("missing port"))?;
        let socket = SocketAddr::new(IpAddr::V4(ip), port);
        Ok(Peer { socket })
    }
}

impl From<Peer> for Multiaddr {
    /// Converts a Peer to a [Multiaddr]
    fn from(value: Peer) -> Self {
        let mut multiaddr = Multiaddr::empty();
        match value.socket.ip() {
            IpAddr::V4(ip) => multiaddr.push(Protocol::Ip4(ip)),
            IpAddr::V6(ip) => multiaddr.push(Protocol::Ip6(ip)),
        }
        multiaddr.push(Protocol::Tcp(value.socket.port()));
        multiaddr
    }
}
