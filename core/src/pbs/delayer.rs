use {
    crate::{
        banking_trace::{BankingPacketBatch, BankingPacketSender},
        pbs::{
            extract_first_signature,
            filters::SubscriptionFilters,
            grpc::{PbsBatch, SanitizedTransactionWithSimulationResult, SimulationResult},
            simulation_result_cache::SimulationResultCache,
            slot_boundary::SlotBoundaryStatus,
        },
    },
    crossbeam_channel::SendError,
    futures::StreamExt,
    solana_accounts_db::transaction_results::InnerInstructionsList,
    solana_bundle::bundle_execution::{
        load_and_execute_bundle, LoadAndExecuteBundleError, LoadAndExecuteBundleResult,
    },
    solana_measure::measure_us,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::{
        bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle},
        clock::MAX_PROCESSING_AGE,
        saturating_add_assign,
        signature::Signature,
        transaction::{MessageHash, SanitizedTransaction},
    },
    std::{
        collections::{HashSet, VecDeque},
        slice::from_ref,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc::{UnboundedReceiver, UnboundedSender},
            watch,
        },
        task::JoinHandle,
        time::{interval, Instant},
    },
    tokio_util::time::DelayQueue,
};

const SIMULATION_DEADLINE_MS: u64 = 200;
const DELAY_PACKET_BATCHES_MS: u64 = 500;

const SIMULATION_CHUNK_SIZE: usize = 20;
const MAX_TX_SIMULATION_TIME_MS: u64 = 10;

pub type TimestampedBankingPacketBatch = (BankingPacketBatch, Instant);

#[derive(Default)]
struct PbsDelayerStageStats {
    num_sanitized_txs: u64,
    num_filtered_txs: u64,
    num_empty_batches: u64,
    num_simulated_chunks: u64,
    num_simulated_txs: u64,
    num_skip_simulation_txs: u64,
    simulation_us: u64,

    num_failed_sims: u64,
    num_proc_time_exceeded_sims: u64,
    num_lock_err_sims: u64,
    num_success_sims: u64,

    num_missing_delay_deadlines: u64,
    num_missing_sim_deadlines: u64,
    num_unprocessed_txs: u64,
}

impl PbsDelayerStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_sanitized_txs", self.num_sanitized_txs, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_filtered_txs", self.num_filtered_txs, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_empty_batches", self.num_empty_batches, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_simulated_chunks", self.num_simulated_chunks, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("simulation_us", self.simulation_us, i64),
        );

        datapoint_info!(
            "pbs_delayer-stats",
            ("num_failed_sims", self.num_failed_sims, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            (
                "num_proc_time_exceeded_sims",
                self.num_proc_time_exceeded_sims,
                i64
            ),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_lock_err_sims", self.num_lock_err_sims, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_success_sims", self.num_success_sims, i64),
        );

        datapoint_info!(
            "pbs_delayer-stats",
            (
                "num_missing_delay_deadlines",
                self.num_missing_delay_deadlines,
                i64
            ),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            (
                "num_missing_sim_deadlines",
                self.num_missing_sim_deadlines,
                i64
            ),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_unprocessed_txs", self.num_unprocessed_txs, i64),
        );
        datapoint_info!(
            "pbs_delayer-stats",
            ("num_skip_simulation_txs", self.num_skip_simulation_txs, i64),
        );
    }
}

pub struct PacketDelayer {
    receiver: UnboundedReceiver<TimestampedBankingPacketBatch>,
    banking: BankingPacketSender,
    pbs: UnboundedSender<PbsBatch>,
    drop_packets_receiver: UnboundedReceiver<Vec<Signature>>,
    slot_boundary_watch: watch::Receiver<SlotBoundaryStatus>,
    connection_watch: watch::Receiver<Option<SubscriptionFilters>>,
    bank_forks: Arc<RwLock<BankForks>>,
    exit: Arc<AtomicBool>,
}

async fn run_delayer(actor: PacketDelayer) {
    let PacketDelayer {
        mut receiver,
        banking,
        pbs,
        mut drop_packets_receiver,
        mut slot_boundary_watch,
        mut connection_watch,
        bank_forks,
        exit,
    } = actor;

    let mut stats = PbsDelayerStageStats::default();
    let mut connection_state = connection_watch.borrow_and_update().clone();
    let mut unprocessed_batches = VecDeque::new();
    let mut delay_queue = DelayQueue::new();
    let mut drop_signatures: HashSet<Signature> = HashSet::new();
    let mut simulation_result_cache = SimulationResultCache::new();
    let mut metrics_tick = interval(Duration::from_secs(1));

    while !exit.load(Ordering::Relaxed) {
        tokio::select! {
            biased;

            maybe_slot_boundary_changed = slot_boundary_watch.changed() => {
                if maybe_slot_boundary_changed.is_err() {
                    error!("slot boundary checker updater closed");
                    break;
                }
                if let SlotBoundaryStatus::StandBy = *slot_boundary_watch.borrow_and_update() {
                    // Clear drop signatures and simulation_result_cache at the end of leader slot
                    drop_signatures.clear();
                    simulation_result_cache.clear();
                }
            }

            maybe_connection_changed = connection_watch.changed() => {
                if maybe_connection_changed.is_err() {
                    error!("pbs connection updater closed");
                    break;
                }
                connection_state = connection_watch.borrow_and_update().clone();
                if connection_state.is_none() && immediately_forward(&banking, &mut unprocessed_batches).is_err() {
                    error!("banking receiver closed");
                    break;
                }
            }

            Some(signatures) = drop_packets_receiver.recv() => {
                drop_signatures.extend(signatures);
            }

            Some(packet_batch) = receiver.recv() => {
                let batch = match (&connection_state, is_deadline_elapsed(&packet_batch, Duration::from_millis(DELAY_PACKET_BATCHES_MS))) {
                    (Some(filters), false) => {
                        PacketBatchInProcess::sanitize_and_filter(&packet_batch, bank_forks.as_ref(), filters, &mut stats)
                    }
                    (_, true) => {
                        saturating_add_assign!(stats.num_missing_delay_deadlines, 1);
                        None
                    }
                    _ => None
                };
                if let Some(batch) = batch {
                    unprocessed_batches.push_back(batch);
                } else if banking.send(packet_batch.0).is_err() {
                    error!("banking receiver closed");
                    break;
                }
            }

            Some(maybe_batch) = delay_queue.next() => {
                match maybe_batch {
                    Ok(batch) => {
                        if handle_delayed_banking_packet_batch(batch.into_inner(), &banking, &drop_signatures).is_err() {
                            error!("banking packet receiver closed");
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("delay queue error: {}", err);
                    }
                }
            }

            _ = metrics_tick.tick() => {
                stats.report();
                stats = PbsDelayerStageStats::default();
            }

            Some(batch) = futures::future::ready(unprocessed_batches.front_mut()) => {
                let pbs_batch = {
                    let bank = bank_forks.read().unwrap().working_bank();
                    batch.simulate_chunk(bank.as_ref(), SIMULATION_CHUNK_SIZE, &simulation_result_cache, &mut stats)
                };
                // simulation_result_cache.populate_with_failed_simulations(&pbs_batch);
                if pbs.send(pbs_batch).is_err() {
                    error!("pbs receiver closed");
                    break;
                }
                if batch.is_empty() {
                    if let Some(PacketBatchInProcess {banking_packet_batch, delay_deadline, unprocessed, ..}) = unprocessed_batches.pop_front() {
                        delay_queue.insert_at(banking_packet_batch, delay_deadline);
                        if !unprocessed.is_empty() {
                            saturating_add_assign!(stats.num_missing_sim_deadlines, 1);
                            saturating_add_assign!(stats.num_unprocessed_txs, unprocessed.len() as u64);
                            if pbs.send(unprocessed.into_iter().map(Into::into).collect()).is_err() {
                                error!("pbs receiver closed");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

impl PacketDelayer {
    pub fn start(
        receiver: UnboundedReceiver<TimestampedBankingPacketBatch>,
        banking: BankingPacketSender,
        pbs: UnboundedSender<PbsBatch>,
        drop_packets_receiver: UnboundedReceiver<Vec<Signature>>,
        slot_boundary_watch: watch::Receiver<SlotBoundaryStatus>,
        connection_watch: watch::Receiver<Option<SubscriptionFilters>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let actor = PacketDelayer {
            receiver,
            banking,
            pbs,
            drop_packets_receiver,
            slot_boundary_watch,
            connection_watch,
            bank_forks,
            exit,
        };

        tokio::spawn(run_delayer(actor))
    }
}

struct PacketBatchInProcess {
    banking_packet_batch: BankingPacketBatch,
    simulation_deadline: Instant,
    delay_deadline: Instant,
    unprocessed: Vec<SanitizedTransaction>,
}

impl PacketBatchInProcess {
    pub fn is_simulation_deadline_elapsed(&self) -> bool {
        Instant::now() > self.simulation_deadline
    }

    pub fn sanitize_and_filter(
        batch: &TimestampedBankingPacketBatch,
        bank_forks: &RwLock<BankForks>,
        filters: &SubscriptionFilters,
        stats: &mut PbsDelayerStageStats,
    ) -> Option<Self> {
        let (banking_packet_batch, start) = batch;
        let simulation_deadline = *start + Duration::from_millis(SIMULATION_DEADLINE_MS);
        let delay_deadline = *start + Duration::from_millis(DELAY_PACKET_BATCHES_MS);
        assert!(delay_deadline > simulation_deadline);

        let mut num_sanitized_txs = 0u64;
        let bank = bank_forks.read().unwrap().working_bank();
        let unprocessed: Vec<_> = banking_packet_batch
            .0
            .iter()
            .flat_map(|batch| {
                batch.iter().filter_map(|packet| {
                    let tx = packet.deserialize_slice(..).ok()?;
                    SanitizedTransaction::try_create(tx, MessageHash::Compute, None, bank.as_ref())
                        .ok()
                })
            })
            .inspect(|_| num_sanitized_txs += 1)
            .filter(|tx| filters.is_tx_have_to_be_processed(tx))
            .collect();

        saturating_add_assign!(stats.num_sanitized_txs, num_sanitized_txs);
        saturating_add_assign!(stats.num_filtered_txs, unprocessed.len() as u64);

        if unprocessed.is_empty() {
            saturating_add_assign!(stats.num_empty_batches, 1);
            return None;
        }

        Some(PacketBatchInProcess {
            banking_packet_batch: banking_packet_batch.clone(),
            simulation_deadline,
            delay_deadline,
            unprocessed,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.unprocessed.is_empty()
    }

    pub fn simulate_chunk(
        &mut self,
        bank: &Bank,
        chunk_size: usize,
        simulation_result_cache: &SimulationResultCache,
        stats: &mut PbsDelayerStageStats,
    ) -> Vec<SanitizedTransactionWithSimulationResult> {
        let start = self.unprocessed.len().saturating_sub(chunk_size);
        let batch_size = self.unprocessed.len() - start;

        let batch: Vec<_> = self
            .unprocessed
            .drain(start..)
            .filter_map(|tx| {
                if !bank.is_blockhash_valid(tx.message().recent_blockhash()) {
                    return None;
                }
                // if simulation_result_cache.contains(&tx) {
                //     // Skip simulation and processing of already simulated tx but with different
                //     // blockhash
                //     return None;
                // }
                // let (simulation_result, simulation_us) = measure_us!(simulate(&tx, bank));
                // saturating_add_assign!(stats.simulation_us, simulation_us);
                // collect_simulation_stats(&simulation_result, stats);
                Some(SanitizedTransactionWithSimulationResult::new(tx, Ok(None)))
            })
            .collect();

        saturating_add_assign!(stats.num_simulated_chunks, 1);
        saturating_add_assign!(stats.num_simulated_txs, batch.len() as u64);
        saturating_add_assign!(
            stats.num_skip_simulation_txs,
            (batch_size - batch.len()) as u64
        );

        batch
    }
}

fn collect_simulation_stats(tx: &SimulationResult, stats: &mut PbsDelayerStageStats) {
    match tx {
        Ok(_) => saturating_add_assign!(stats.num_success_sims, 1),
        Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(_)) => {
            saturating_add_assign!(stats.num_proc_time_exceeded_sims, 1)
        }
        Err(LoadAndExecuteBundleError::LockError { .. }) => {
            saturating_add_assign!(stats.num_lock_err_sims, 1)
        }
        Err(LoadAndExecuteBundleError::TransactionError { .. }) => {
            saturating_add_assign!(stats.num_failed_sims, 1)
        }
        _ => {}
    }
}

fn simulate(
    tx: &SanitizedTransaction,
    bank: &Bank,
) -> LoadAndExecuteBundleResult<Option<InnerInstructionsList>> {
    const MAX_TX_SIMULATION_TIME: Duration = Duration::from_millis(MAX_TX_SIMULATION_TIME_MS);

    let sanitized_bundle = SanitizedBundle {
        bundle_id: derive_bundle_id_from_sanitized_transactions(from_ref(tx)),
        transactions: vec![tx.clone()],
    };

    let bundle_execution_result = load_and_execute_bundle(
        bank,
        &sanitized_bundle,
        MAX_PROCESSING_AGE,
        &MAX_TX_SIMULATION_TIME,
        true,
        false,
        false,
        false,
        &None,
        true,
        None,
        &vec![None],
        &vec![None],
    );

    bundle_execution_result.result().clone().map(|_| {
        bundle_execution_result
            .bundle_transaction_results()
            .first()
            .and_then(|bundle_execution_result| bundle_execution_result.execution_results().first())
            .and_then(|transaction_execution_result| transaction_execution_result.details())
            .and_then(|transaction_execution_details| {
                transaction_execution_details.inner_instructions.as_ref()
            })
            .cloned()
    })
}

fn is_deadline_elapsed(batch: &TimestampedBankingPacketBatch, duration: Duration) -> bool {
    Instant::now() > batch.1 + duration
}

fn immediately_forward(
    banking: &BankingPacketSender,
    unprocessed_batches: &mut VecDeque<PacketBatchInProcess>,
) -> Result<(), SendError<BankingPacketBatch>> {
    for PacketBatchInProcess {
        banking_packet_batch,
        ..
    } in unprocessed_batches.drain(..)
    {
        banking.send(banking_packet_batch)?;
    }

    Ok(())
}

fn handle_delayed_banking_packet_batch(
    batch: BankingPacketBatch,
    banking: &BankingPacketSender,
    drop_signatures: &HashSet<Signature>,
) -> Result<(), SendError<BankingPacketBatch>> {
    let exclude: Vec<_> = batch
        .0
        .iter()
        .enumerate()
        .filter_map(|(pos_outer, batch)| {
            let positions: Vec<_> = batch
                .iter()
                .enumerate()
                .filter_map(|(pos_inner, packet)| {
                    let signature = extract_first_signature(packet).ok()?;
                    drop_signatures.contains(&signature).then_some(pos_inner)
                })
                .collect();
            (!positions.is_empty()).then_some((pos_outer, positions))
        })
        .collect();

    if exclude.is_empty() {
        return banking.send(batch);
    }

    let (mut banking_packet_batch, stats) = batch.as_ref().clone();
    for (outer, inner) in exclude {
        let batch = &mut banking_packet_batch[outer];
        for pos in inner {
            batch[pos].meta_mut().set_discard(true);
        }
    }

    banking.send(BankingPacketBatch::new((banking_packet_batch, stats)))
}
