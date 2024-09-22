use alloy::{
    network::AnyNetwork,
    primitives::B256,
    providers::RootProvider,
    rpc::client::ClientBuilder,
    transports::{BoxTransport, TransportErrorKind, TransportResult},
};
use op_alloy_provider::ext::engine::OpEngineApi;
use op_alloy_rpc_types_engine::{
    OptimismExecutionPayloadEnvelopeV3, OptimismExecutionPayloadEnvelopeV4,
    OptimismPayloadAttributes, ProtocolVersion, SuperchainSignal,
};
use reth::{
    payload::PayloadId,
    rpc::types::{
        engine::{
            ExecutionPayloadEnvelopeV2, ExecutionPayloadInputV2, ForkchoiceState,
            ForkchoiceUpdated, JwtSecret, PayloadStatus,
        },
        ExecutionPayload,
    },
};
use reth_node_api::EngineApiMessageVersion;
use url::Url;

mod transport;
use transport::AuthenticatedTransportConnect;

#[derive(Debug, Clone)]
pub struct EngineApiClient {
    auth_provider: RootProvider<BoxTransport, AnyNetwork>,
}

impl EngineApiClient {
    /// Creates a new [`EngineApiClient`] instance.
    pub async fn new(rpc_url: Url, jwt: JwtSecret) -> eyre::Result<Self> {
        let auth_transport = AuthenticatedTransportConnect::new(rpc_url, jwt);
        let client = ClientBuilder::default().connect_boxed(auth_transport).await?;
        Ok(Self { auth_provider: RootProvider::<_, AnyNetwork>::new(client) })
    }

    /// Calls the `engine_newPayload` method on the engine API.
    ///
    /// - The version is determined by the execution payload version passed in.
    /// - Returns the engine API message version and the payload status.
    pub async fn new_payload(
        &self,
        payload: ExecutionPayload,
        parent_beacon_block_root: Option<B256>,
    ) -> TransportResult<(EngineApiMessageVersion, PayloadStatus)> {
        match payload {
            ExecutionPayload::V4(payload) => {
                let parent_root = parent_beacon_block_root.ok_or_else(|| {
                    TransportErrorKind::custom_str("parent beacon root must be provided")
                })?;

                Ok((
                    EngineApiMessageVersion::V4,
                    self.auth_provider.new_payload_v4(payload, parent_root).await?,
                ))
            }
            ExecutionPayload::V3(payload) => {
                let parent_root = parent_beacon_block_root.ok_or_else(|| {
                    TransportErrorKind::custom_str("parent beacon root must be provided")
                })?;

                Ok((
                    EngineApiMessageVersion::V3,
                    self.auth_provider.new_payload_v3(payload, parent_root).await?,
                ))
            }
            ExecutionPayload::V2(payload) => {
                let input = ExecutionPayloadInputV2 {
                    execution_payload: payload.payload_inner,
                    withdrawals: Some(payload.withdrawals),
                };

                Ok((EngineApiMessageVersion::V2, self.auth_provider.new_payload_v2(input).await?))
            }
            ExecutionPayload::V1(_) => {
                Err(TransportErrorKind::custom_str("V1 payloads are not supported"))
            }
        }
    }

    /// Calls the `engine_getPayload` method on the engine API.
    ///
    /// - The version is determined by the execution payload version passed in.
    /// - Returns the execution payload envelope based on the version.
    pub async fn get_payload(
        &self,
        message_version: EngineApiMessageVersion,
        payload_id: PayloadId,
    ) -> TransportResult<GetPayloadResponse> {
        match message_version {
            EngineApiMessageVersion::V1 | EngineApiMessageVersion::V2 => {
                Ok(GetPayloadResponse::V2(self.auth_provider.get_payload_v2(payload_id).await?))
            }
            EngineApiMessageVersion::V3 => {
                Ok(GetPayloadResponse::V3(self.auth_provider.get_payload_v3(payload_id).await?))
            }
            EngineApiMessageVersion::V4 => {
                Ok(GetPayloadResponse::V4(self.auth_provider.get_payload_v4(payload_id).await?))
            }
        }
    }

    /// Calls the `fork_choice_updated` method on the engine API.
    ///
    /// - The version is determined by the execution payload version passed in.
    /// - Returns the fork choice updated response.
    pub async fn fork_choice_updated(
        &self,
        message_version: EngineApiMessageVersion,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<OptimismPayloadAttributes>,
    ) -> TransportResult<ForkchoiceUpdated> {
        match message_version {
            EngineApiMessageVersion::V4 | EngineApiMessageVersion::V3 => {
                self.auth_provider
                    .fork_choice_updated_v3(fork_choice_state, payload_attributes)
                    .await
            }
            EngineApiMessageVersion::V1 | EngineApiMessageVersion::V2 => {
                self.auth_provider
                    .fork_choice_updated_v2(fork_choice_state, payload_attributes)
                    .await
            }
        }
    }

    /// Calls the `signal_superchain` method on the engine API.
    ///
    /// - Returns the latest protocol version supported by the execution engine.
    pub async fn signal_superchain(
        &self,
        signal: SuperchainSignal,
    ) -> TransportResult<ProtocolVersion> {
        self.auth_provider.signal_superchain_v1(signal).await
    }
}

/// The response from the `engine_getPayload` versioned method.
pub enum GetPayloadResponse {
    V2(ExecutionPayloadEnvelopeV2),
    V3(OptimismExecutionPayloadEnvelopeV3),
    V4(OptimismExecutionPayloadEnvelopeV4),
}
