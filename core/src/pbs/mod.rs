use {
    solana_perf::sigverify::PacketError,
    solana_sdk::{packet::Packet, short_vec::decode_shortu16_len, signature::Signature},
    std::mem::size_of,
    thiserror::Error,
    tonic::Status,
};

mod delayer;
mod filters;
mod forwarder;
mod grpc;
mod interceptor;
pub mod pbs_stage;
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

fn extract_first_signature(packet: &Packet) -> Result<Signature, PacketError> {
    // should have at least 1 signature and sig lengths
    let _ = 1usize
        .checked_add(size_of::<Signature>())
        .filter(|v| *v <= packet.meta().size)
        .ok_or(PacketError::InvalidLen)?;

    // read the length of Transaction.signatures (serialized with short_vec)
    let (_sig_len_untrusted, sig_start) = packet
        .data(..)
        .and_then(|bytes| decode_shortu16_len(bytes).ok())
        .ok_or(PacketError::InvalidShortVec)?;

    let sig_end = sig_start
        .checked_add(size_of::<Signature>())
        .ok_or(PacketError::InvalidLen)?;

    packet
        .data(sig_start..sig_end)
        .ok_or(PacketError::InvalidLen)?
        .try_into()
        .map_err(|_| PacketError::InvalidLen)
}
