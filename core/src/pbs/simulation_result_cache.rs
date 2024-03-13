use {
    crate::pbs::grpc::SanitizedTransactionWithSimulationResult,
    solana_bundle::bundle_execution::LoadAndExecuteBundleError,
    solana_sdk::{hash::Hash, transaction::SanitizedTransaction},
    std::collections::HashSet,
};

pub struct SimulationResultCache {
    cache: HashSet<Hash>,
}

impl SimulationResultCache {
    pub fn new() -> Self {
        Self {
            cache: HashSet::new(),
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }

    pub fn contains(&self, sanitized_transaction: &SanitizedTransaction) -> bool {
        let mut message = sanitized_transaction.to_versioned_transaction().message;
        message.set_recent_blockhash(Default::default());
        self.cache.contains(&message.hash())
    }

    pub fn populate_with_failed_simulations(
        &mut self,
        simulation_results: &[SanitizedTransactionWithSimulationResult],
    ) {
        self.cache.extend(simulation_results.iter().filter_map(|v| {
            if let Some(Err(LoadAndExecuteBundleError::TransactionError { .. })) =
                v.simulation_result
            {
                let mut message = v.transaction.to_versioned_transaction().message;
                message.set_recent_blockhash(Default::default());
                Some(message.hash())
            } else {
                None
            }
        }))
    }
}
