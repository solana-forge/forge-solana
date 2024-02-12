use {
    crate::bundle_execution::LoadAndExecuteBundleError, solana_poh::poh_recorder::PohRecorderError,
    thiserror::Error,
};

pub mod bundle_execution;

pub type BundleExecutionResult<T> = Result<T, BundleExecutionError>;

#[derive(Error, Debug, Clone)]
pub enum BundleExecutionError {
    #[error("The bank has hit the max allotted time for processing transactions")]
    BankProcessingTimeLimitReached,

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("Runtime error while executing the bundle: {0}")]
    TransactionFailure(#[from] LoadAndExecuteBundleError),

    #[error("Error locking bundle because a transaction is malformed")]
    LockError,

    #[error("PoH record error: {0}")]
    PohRecordError(#[from] PohRecorderError),
}
