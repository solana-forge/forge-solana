use {thiserror::Error, tonic::Status};

mod delayer;
mod filters;
mod forwarder;
mod grpc;
mod interceptor;
pub mod pbs_stage;
pub mod pbs_stage2;
mod slot_boundary;

#[derive(Error, Debug)]
pub enum PbsError {
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),

    #[error("stream disconnected")]
    GrpcStreamDisconnected,

    #[error("PbsConnectionError: {0:?}")]
    PbsConnectionError(String),

    #[error("PbsConnectionTimeout")]
    PbsConnectionTimeout,

    #[error("MethodTimeout: {0:?}")]
    MethodTimeout(String),

    #[error("MethodError: {0:?}")]
    MethodError(String),

    #[error("AuthenticationConnectionError: {0}")]
    AuthenticationConnectionError(String),

    #[error("PacketForwardError")]
    PacketForwardError,

    #[error("SlotBoundaryCheckerError")]
    SlotBoundaryCheckerError,

    #[error("SubscriptionFiltersError")]
    SubscriptionFiltersError,
}
