//! The engine relay handles forwarding [OpAttributesWithParent] to the L2 Engine API.

use std::ops::Deref;

use alloy_network::AnyNetwork;
use alloy_primitives::Bytes;
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret};
use alloy_transport_http::{
    hyper_util::{
        client::legacy::{connect::HttpConnector, Client},
        rt::TokioExecutor,
    },
    AuthLayer, AuthService, Http, HyperClient,
};
use eyre::Result;
use http_body_util::Full;
use op_alloy_provider::ext::engine::OpEngineApi;
use op_alloy_rpc_types_engine::OpAttributesWithParent;
use tower::ServiceBuilder;
use tracing::warn;
use url::Url;

/// A Hyper HTTP client with a JWT authentication layer.
type HyperAuthClient<B = Full<Bytes>> = HyperClient<B, AuthService<Client<HttpConnector, B>>>;

/// The [`Engine`] is responsible for interacting with an L2 Engine API server.
#[derive(Debug, Clone)]
pub struct EngineRelay {
    provider: RootProvider<Http<HyperAuthClient>, AnyNetwork>,
}

impl EngineRelay {
    /// Creates a new [`EngineRelay`] from the provided [Url] and [JwtSecret].
    pub fn new_http(url: Url, jwt: JwtSecret) -> Self {
        let hyper_client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();

        let auth_layer = AuthLayer::new(jwt);
        let service = ServiceBuilder::new().layer(auth_layer).service(hyper_client);

        let layer_transport = HyperClient::with_service(service);
        let http_hyper = Http::with_client(layer_transport, url);
        let rpc_client = RpcClient::new(http_hyper, true);
        let provider = RootProvider::<_, AnyNetwork>::new(rpc_client);

        Self { provider }
    }

    /// Validates the payload using the Fork Choice Update API.
    pub async fn validate_payload_fcu(&self, attributes: &OpAttributesWithParent) -> Result<bool> {
        // TODO: use the correct values
        let fork_choice_state = ForkchoiceState {
            head_block_hash: attributes.parent.block_info.hash,
            finalized_block_hash: attributes.parent.block_info.hash,
            safe_block_hash: attributes.parent.block_info.hash,
        };

        let attributes = Some(attributes.attributes.clone());
        let fcu = self.provider.fork_choice_updated_v2(fork_choice_state, attributes).await?;

        if fcu.is_valid() {
            Ok(true)
        } else {
            warn!(status = %fcu.payload_status, "Engine API returned invalid fork choice update");
            Ok(false)
        }
    }
}

impl Deref for EngineRelay {
    type Target = RootProvider<Http<HyperAuthClient>, AnyNetwork>;

    fn deref(&self) -> &Self::Target {
        &self.provider
    }
}
