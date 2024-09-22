use std::{
    sync::Arc,
    task::{Context, Poll},
};

use alloy::{
    providers::{IpcConnect, WsConnect},
    pubsub::{PubSubConnect, PubSubFrontend},
    rpc::json_rpc::{RequestPacket, ResponsePacket},
    transports::{
        http::{Http, ReqwestTransport},
        utils::guess_local_url,
        Authorization, Pbf, TransportConnect, TransportError, TransportErrorKind, TransportFut,
    },
};
use futures::FutureExt;
use reqwest::{
    header::{self, HeaderValue},
    Client,
};
use reth::rpc::types::engine::{Claims, JwtSecret};
use tokio::sync::RwLock;
use tower::Service;
use url::Url;

/// An enum representing the different transports that can be used to connect to a runtime.
/// Only meant to be used internally by [`AuthenticatedTransport`].
#[derive(Clone, Debug)]
pub enum InnerTransport {
    /// HTTP transport
    Http(ReqwestTransport),
    /// `WebSocket` transport
    Ws(PubSubFrontend),
    /// IPC transport
    Ipc(PubSubFrontend),
}

impl InnerTransport {
    /// Connects to a transport based on the given URL and JWT.
    /// Returns an [`InnerTransport`] and the [`Claims`] generated from the jwt.
    async fn connect(url: Url, jwt: JwtSecret) -> Result<(Self, Claims), AuthTransportError> {
        match url.scheme() {
            "http" | "https" => Self::connect_http(url, jwt),
            "ws" | "wss" => Self::connect_ws(url, jwt).await,
            "file" => Ok((Self::connect_ipc(url).await?, Claims::default())),
            _ => Err(AuthTransportError::BadScheme(url.scheme().to_string())),
        }
    }

    /// Connects to an HTTP transport.
    /// Returns an [`InnerTransport`] and the [Claims] generated from the jwt.
    fn connect_http(url: Url, jwt: JwtSecret) -> Result<(Self, Claims), AuthTransportError> {
        let mut client_builder = Client::builder().tls_built_in_root_certs(url.scheme() == "https");

        // Add the JWT it to the headers if we can decode it.
        let (auth, claims) = build_auth(jwt).map_err(AuthTransportError::InvalidJwt)?;

        let mut auth_value = HeaderValue::from_str(&auth.to_string()).expect("invalid string");
        auth_value.set_sensitive(true);

        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth_value);
        client_builder = client_builder.default_headers(headers);

        let client = client_builder.build().map_err(AuthTransportError::HttpConstructionError)?;
        let inner = Self::Http(Http::with_client(client, url));

        Ok((inner, claims))
    }

    /// Connects to a `WebSocket` transport.
    /// Returns an [`InnerTransport`] and the [`Claims`] generated from the jwt.
    async fn connect_ws(url: Url, jwt: JwtSecret) -> Result<(Self, Claims), AuthTransportError> {
        // Add the JWT to the headers if we can decode it.
        let (auth, claims) = build_auth(jwt).map_err(AuthTransportError::InvalidJwt)?;

        let ws = WsConnect { url: url.to_string(), auth: Some(auth) };

        match ws.into_service().await {
            Ok(ws) => Ok((Self::Ws(ws), claims)),
            Err(e) => Err(AuthTransportError::TransportError(e, url.to_string())),
        }
    }

    /// Connects to an IPC transport. Returns an [`InnerTransport`].
    /// Does not return any [`Claims`] because IPC does not require them.
    async fn connect_ipc(url: Url) -> Result<Self, AuthTransportError> {
        // IPC, even for engine, typically does not require auth because it's local
        let ipc = IpcConnect::new(url.to_string());

        match ipc.into_service().await {
            Ok(ipc) => Ok(Self::Ipc(ipc)),
            Err(e) => Err(AuthTransportError::TransportError(e, url.to_string())),
        }
    }
}

/// An error that can occur when creating an authenticated transport.
#[derive(Debug, thiserror::Error)]
pub enum AuthTransportError {
    /// The JWT is invalid.
    #[error("The JWT is invalid: {0}")]
    InvalidJwt(eyre::Error),
    /// The transport failed to connect.
    #[error("The transport failed to connect to {1}, transport error: {0}")]
    TransportError(TransportError, String),
    /// The http client could not be built.
    #[error("The http client could not be built")]
    HttpConstructionError(reqwest::Error),
    /// The scheme is invalid.
    #[error("The URL scheme is invalid: {0}")]
    BadScheme(String),
}

/// An authenticated transport that can be used to send requests that contain a jwt bearer token.
#[derive(Debug, Clone)]
pub struct AuthenticatedTransport {
    /// The inner actual transport used.
    ///
    /// Also contains the current claims being used. This is used to determine whether or not we
    /// should create another client.
    inner_and_claims: Arc<RwLock<(InnerTransport, Claims)>>,
    /// The current jwt being used. This is so we can recreate claims.
    jwt: JwtSecret,
    /// The current URL being used. This is so we can recreate the client if needed.
    url: Url,
}

impl AuthenticatedTransport {
    /// Create a new builder with the given URL and JWT secret.
    pub async fn connect(url: Url, jwt: JwtSecret) -> Result<Self, AuthTransportError> {
        let (inner, claims) = InnerTransport::connect(url.clone(), jwt).await?;
        Ok(Self { inner_and_claims: Arc::new(RwLock::new((inner, claims))), jwt, url })
    }

    /// Sends a request using the underlying transport.
    ///
    /// For sending the actual request, this action is delegated down to the underlying transport
    /// through Tower's [`tower::Service::call`].
    ///
    /// See tower's [`tower::Service`] trait for more information.
    fn request(&self, req: RequestPacket) -> TransportFut<'static> {
        let this = self.clone();

        Box::pin(async move {
            let mut inner_and_claims = this.inner_and_claims.write().await;

            // shift the iat forward by one second so there is some buffer time
            let mut shifted_claims = inner_and_claims.1;
            shifted_claims.iat -= 1;

            // if the claims are out of date, reset the inner transport
            if !shifted_claims.is_within_time_window() {
                match InnerTransport::connect(this.url, this.jwt).await {
                    Ok((new_inner, new_claims)) => *inner_and_claims = (new_inner, new_claims),
                    Err(e) => return Err(TransportErrorKind::custom(Box::new(e))),
                }
            }

            match inner_and_claims.0 {
                InnerTransport::Http(ref mut http) => http.call(req),
                InnerTransport::Ws(ref mut ws) => ws.call(req),
                InnerTransport::Ipc(ref mut ipc) => ipc.call(req),
            }
            .await
        })
    }
}

/// Generate claims (iat with current timestamp).
/// This happens by default using the Default trait for Claims.
fn build_auth(secret: JwtSecret) -> eyre::Result<(Authorization, Claims)> {
    let claims = Claims::default();
    let token = secret.encode(&claims)?;
    let auth = Authorization::Bearer(token);

    Ok((auth, claims))
}

/// This specifies how to connect to an authenticated transport.
#[derive(Clone, Debug)]
pub struct AuthenticatedTransportConnect {
    /// The URL to connect to.
    url: Url,
    /// The JWT secret used to authenticate the transport.
    jwt: JwtSecret,
}

impl AuthenticatedTransportConnect {
    /// Create a new builder with the given URL.
    pub const fn new(url: Url, jwt: JwtSecret) -> Self {
        Self { url, jwt }
    }
}

impl TransportConnect for AuthenticatedTransportConnect {
    type Transport = AuthenticatedTransport;

    fn is_local(&self) -> bool {
        guess_local_url(&self.url)
    }

    fn get_transport<'a: 'b, 'b>(&'a self) -> Pbf<'b, Self::Transport, TransportError> {
        AuthenticatedTransport::connect(self.url.clone(), self.jwt)
            .map(|res| match res {
                Ok(transport) => Ok(transport),
                Err(err) => {
                    Err(TransportError::Transport(TransportErrorKind::Custom(Box::new(err))))
                }
            })
            .boxed()
    }
}

impl tower::Service<RequestPacket> for AuthenticatedTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        self.request(req)
    }
}

impl tower::Service<RequestPacket> for &AuthenticatedTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        self.request(req)
    }
}
