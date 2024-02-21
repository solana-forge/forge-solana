use {
    forge_protos::proto::pbs::{
        transaction_with_simulation_result, BundleError as ProtoBundleError,
        SanitizedTransaction as ProtoSanitizedTransaction,
        SimulationResult as ProtoSimulationResult, TransactionError as ProtoTransactionError,
    },
    solana_accounts_db::transaction_results::InnerInstructionsList,
    solana_bundle::bundle_execution::{LoadAndExecuteBundleError, LoadAndExecuteBundleResult},
    solana_sdk::transaction::SanitizedTransaction,
};

pub type SimulationResult = LoadAndExecuteBundleResult<Option<InnerInstructionsList>>;
pub type PbsBatch = Vec<SanitizedTransactionWithSimulationResult>;

pub struct SanitizedTransactionWithSimulationResult {
    pub transaction: SanitizedTransaction,
    pub simulation_result: Option<SimulationResult>,
}

impl SanitizedTransactionWithSimulationResult {
    pub fn new(transaction: SanitizedTransaction, simulation_result: SimulationResult) -> Self {
        Self {
            transaction,
            simulation_result: Some(simulation_result),
        }
    }
}

impl From<SanitizedTransaction> for SanitizedTransactionWithSimulationResult {
    fn from(transaction: SanitizedTransaction) -> Self {
        Self {
            transaction,
            simulation_result: None,
        }
    }
}

pub fn sanitized_to_proto_sanitized(tx: SanitizedTransaction) -> Option<ProtoSanitizedTransaction> {
    let versioned_transaction = bincode::serialize(&tx.to_versioned_transaction()).ok()?;
    let message_hash = tx.message_hash().to_bytes().to_vec();
    let loaded_addresses = bincode::serialize(&tx.get_loaded_addresses()).ok()?;

    Some(ProtoSanitizedTransaction {
        versioned_transaction,
        message_hash,
        loaded_addresses,
    })
}

pub fn simulation_result_to_proto_simulation_result(
    simulation_result: SimulationResult,
) -> transaction_with_simulation_result::Simulation {
    match simulation_result {
        Ok(inner_instructions) => transaction_with_simulation_result::Simulation::SimulationResult(
            inner_instructions_to_proto(inner_instructions),
        ),
        Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(_)) => {
            transaction_with_simulation_result::Simulation::BundleError(
                ProtoBundleError::ProcessingTimeExceeded as i32,
            )
        }
        Err(LoadAndExecuteBundleError::LockError { .. }) => {
            transaction_with_simulation_result::Simulation::BundleError(
                ProtoBundleError::LockError as i32,
            )
        }
        Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts) => {
            transaction_with_simulation_result::Simulation::BundleError(
                ProtoBundleError::InvalidPreOrPostAccounts as i32,
            )
        }
        Err(err @ LoadAndExecuteBundleError::TransactionError { .. }) => {
            transaction_with_simulation_result::Simulation::TransactionError(
                ProtoTransactionError {
                    err: err.to_string(),
                },
            )
        }
    }
}

fn inner_instructions_to_proto(
    inner_instructions: Option<InnerInstructionsList>,
) -> ProtoSimulationResult {
    use forge_protos::proto::pbs::{
        InnerInstruction as ProtoInnerInstruction, InnerInstructions as ProtoInnerInstructions,
    };

    ProtoSimulationResult {
        inner_instructions_none: inner_instructions.is_none(),
        inner_instructions: inner_instructions
            .into_iter()
            .flatten()
            .map(|inner_instructions| ProtoInnerInstructions {
                instructions: inner_instructions
                    .iter()
                    .map(|ixn| ProtoInnerInstruction {
                        program_id_index: ixn.instruction.program_id_index as u32,
                        accounts: ixn.instruction.accounts.clone(),
                        data: ixn.instruction.data.clone(),
                        stack_height: ixn.stack_height as u32,
                    })
                    .collect(),
            })
            .collect(),
    }
}
