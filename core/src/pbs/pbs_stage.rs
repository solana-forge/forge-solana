use {
    crate::{
        banking_trace::{BankingPacketBatch, BankingPacketReceiver, BankingPacketSender},
        packet_bundle::PacketBundle,
        pbs::{interceptor::AuthInterceptor, PbsError},
        proto_packet_to_packet,
    },
    crossbeam_channel::Sender,
    forge_protos::proto::pbs::{
        pbs_validator_client::PbsValidatorClient, transaction_with_simulation_result,
        BundleError as ProtoBundleError, BundlesResponse,
        SanitizedTransaction as ProtoSanitizedTransaction, SanitizedTransactionRequest,
        SimulationResult as ProtoSimulationResult, SubscriptionFiltersRequest,
        SubscriptionFiltersResponse, TransactionError as ProtoTransactionError,
        TransactionWithSimulationResult as ProtoTransactionWithSimulationResult,
    },
    futures::StreamExt,
    prost_types::Timestamp,
    solana_accounts_db::transaction_results::InnerInstructionsList,
    solana_bundle::bundle_execution::{
        load_and_execute_bundle, LoadAndExecuteBundleError, LoadAndExecuteBundleResult,
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure_us,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::{
        bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle},
        clock::MAX_PROCESSING_AGE,
        pubkey::Pubkey,
        saturating_add_assign,
        signature::Signer,
        transaction::{MessageHash, SanitizedTransaction},
    },
    std::{
        slice::from_ref,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        sync::mpsc::{UnboundedReceiver, UnboundedSender},
        task::{self, JoinHandle as TokioJoinHandle},
        time::{interval, sleep, timeout, Instant},
        try_join,
    },
    tokio_stream::wrappers::UnboundedReceiverStream,
    tokio_util::time::DelayQueue,
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Status,
    },
};

const CONNECTION_TIMEOUT_S: u64 = 10;
const CONNECTION_BACKOFF_S: u64 = 5;

const DELAY_PACKET_BATCHES_MS: u64 = 300;

#[derive(Default)]
struct PbsStageStats {
    num_bundles: u64,
    num_bundle_packets: u64,
    num_missing_deadlines: u64,
    num_empty_packet_batches: u64,
    num_sanitized_transactions: u64,
    num_empty_tx_batches: u64,

    num_filtered_for_sims: u64,
    num_failed_sims: u64,
    num_proc_time_exceeded_sims: u64,
    num_invalid_accs_sims: u64, // should never happen
    num_lock_err_sims: u64,
    num_success_simulations: u64,

    simulation_us: u64,
}

impl PbsStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!("pbs_stage-stats", ("num_bundles", self.num_bundles, i64),);
        datapoint_info!(
            "pbs_stage-stats",
            ("num_bundle_packets", self.num_bundle_packets, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_missing_deadlines", self.num_missing_deadlines, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            (
                "num_empty_packet_batches",
                self.num_empty_packet_batches,
                i64
            ),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_sanitized_transactions", self.num_bundles, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_empty_tx_batches", self.num_empty_tx_batches, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_failed_sims", self.num_failed_sims, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_filtered_for_sims", self.num_filtered_for_sims, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            (
                "num_proc_time_exceeded_sims",
                self.num_proc_time_exceeded_sims,
                i64
            ),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_lock_err_sims", self.num_lock_err_sims, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_success_simulations", self.num_success_simulations, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_invalid_accs_sims", self.num_invalid_accs_sims, i64),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("simulation_us", self.simulation_us, i64),
        );
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PbsConfig {
    pub pbs_url: String,
    pub uuid: String,
}

pub struct PbsEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}

impl PbsEngineStage {
    pub fn new(
        pbs_config: Arc<Mutex<PbsConfig>>,
        // Channel that bundles get piped through.
        bundle_tx: Sender<Vec<PacketBundle>>,
        // The keypair stored here is used to auth
        cluster_info: Arc<ClusterInfo>,
        // Channel that trusted packets after SigVerify get piped through.
        sigverified_receiver: BankingPacketReceiver,
        // Channel that trusted packets get piped through.
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> Self {
        let thread = Builder::new()
            .name("pbs-stage".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(Self::start(
                    pbs_config,
                    bundle_tx,
                    cluster_info,
                    sigverified_receiver,
                    banking_packet_sender,
                    exit,
                    bank_forks,
                    poh_recorder,
                ));
            })
            .unwrap();

        Self {
            t_hdls: vec![thread],
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        pbs_config: Arc<Mutex<PbsConfig>>,
        bundle_tx: Sender<Vec<PacketBundle>>,
        cluster_info: Arc<ClusterInfo>,
        sigverified_receiver: BankingPacketReceiver,
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) {
        const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_S);
        const CONNECTION_BACKOFF: Duration = Duration::from_secs(CONNECTION_BACKOFF_S);
        let mut error_count: u64 = 0;

        let is_pbs_active = Arc::new(AtomicBool::new(false));
        let (forwarder_sender, forwarder_receiver) = tokio::sync::mpsc::unbounded_channel();
        let forwarder_join_handle = Self::start_forward_packet_batches(
            sigverified_receiver,
            forwarder_sender,
            banking_packet_sender.clone(),
            is_pbs_active.clone(),
            exit.clone(),
        );

        let (slot_boundary_sender, mut slot_boundary_receiver) =
            tokio::sync::watch::channel(SlotBoundaryStatus::default());
        let slot_boundary_checker_join_handle =
            Self::start_slot_boundary_checker(poh_recorder, slot_boundary_sender, exit.clone());

        let (pbs_sender, mut pbs_receiver) = tokio::sync::mpsc::unbounded_channel();
        let delayer_join_handle = Self::start_delayer(
            forwarder_receiver,
            banking_packet_sender,
            pbs_sender,
            exit.clone(),
            slot_boundary_receiver.clone(),
        );

        while !exit.load(Ordering::Relaxed) {
            // Wait until a valid config is supplied (either initially or by admin rpc)
            // Use if!/else here to avoid extra CONNECTION_BACKOFF wait on successful termination
            let local_pbs_config = {
                let local_pbs_config = pbs_config.clone();
                task::spawn_blocking(move || local_pbs_config.lock().unwrap().clone())
                    .await
                    .unwrap()
            };

            if !Self::is_valid_pbs_config(&local_pbs_config) {
                sleep(CONNECTION_BACKOFF).await;
            } else if let Err(err) = Self::connect_and_stream(
                &local_pbs_config,
                &pbs_config,
                &bundle_tx,
                &cluster_info,
                &mut pbs_receiver,
                &is_pbs_active,
                &exit,
                &bank_forks,
                &mut slot_boundary_receiver,
                &CONNECTION_TIMEOUT,
            )
            .await
            {
                is_pbs_active.store(false, Ordering::Relaxed);

                error_count += 1;
                datapoint_warn!(
                    "pbs_stage-error",
                    ("count", error_count, i64),
                    ("error", err.to_string(), String),
                );
                sleep(CONNECTION_BACKOFF).await;
            }
        }
        is_pbs_active.store(false, Ordering::Relaxed);

        try_join!(
            forwarder_join_handle,
            delayer_join_handle,
            slot_boundary_checker_join_handle
        )
        .unwrap();
    }

    // Forward packets from the sigverified_receiver to the banking_packet_sender if pbs isn't ready
    fn start_forward_packet_batches(
        sigverified_receiver: BankingPacketReceiver,
        pbs_sender: UnboundedSender<BankingPacketBatch>,
        banking_packet_sender: BankingPacketSender,
        is_pbs_active: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
    ) -> TokioJoinHandle<()> {
        task::spawn_blocking(move || {
            Self::forward_packet_batches(
                sigverified_receiver,
                pbs_sender,
                banking_packet_sender,
                is_pbs_active,
                exit,
            )
        })
    }

    fn forward_packet_batches(
        sigverified_receiver: BankingPacketReceiver,
        pbs_sender: UnboundedSender<BankingPacketBatch>,
        banking_packet_sender: BankingPacketSender,
        is_pbs_active: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
    ) {
        while !exit.load(Ordering::Relaxed) {
            let Ok(packet_batch) = sigverified_receiver.recv() else {
                error!("sigverified packet receiver closed");
                break;
            };
            if is_pbs_active.load(Ordering::Relaxed) {
                if pbs_sender.send(packet_batch).is_err() {
                    error!("psb stage packet consumer closed");
                    break;
                }
            } else if banking_packet_sender.send(packet_batch).is_err() {
                error!("banking packet sender closed");
                break;
            }
        }
    }

    fn start_delayer(
        sigverified_receiver: UnboundedReceiver<BankingPacketBatch>,
        banking_packet_sender: BankingPacketSender,
        pbs_sender: UnboundedSender<(BankingPacketBatch, Instant)>,
        exit: Arc<AtomicBool>,
        slot_boundary_receiver: tokio::sync::watch::Receiver<SlotBoundaryStatus>,
    ) -> TokioJoinHandle<()> {
        tokio::spawn(Self::delay_packet_batches(
            sigverified_receiver,
            banking_packet_sender,
            pbs_sender,
            exit,
            slot_boundary_receiver,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_and_stream(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        cluster_info: &Arc<ClusterInfo>,
        receiver: &mut UnboundedReceiver<(BankingPacketBatch, Instant)>,
        is_pbs_active: &Arc<AtomicBool>,
        exit: &Arc<AtomicBool>,
        bank_forks: &Arc<RwLock<BankForks>>,
        slot_boundary_receiver: &mut tokio::sync::watch::Receiver<SlotBoundaryStatus>,
        connection_timeout: &Duration,
    ) -> Result<(), PbsError> {
        let mut backend_endpoint = Endpoint::from_shared(local_config.pbs_url.clone())
            .map_err(|_| {
                PbsError::PbsConnectionError(format!(
                    "invalid block engine url value: {}",
                    local_config.pbs_url
                ))
            })?
            .tcp_keepalive(Some(Duration::from_secs(60)));

        if local_config.pbs_url.starts_with("https") {
            backend_endpoint = backend_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|_| {
                    PbsError::PbsConnectionError(
                        "failed to set tls_config for block engine service".to_string(),
                    )
                })?;
        }

        debug!("connecting to block engine: {}", local_config.pbs_url);

        let pbs_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| PbsError::PbsConnectionTimeout)?
            .map_err(|e| PbsError::PbsConnectionError(e.to_string()))?;

        let mut pbs_client = PbsValidatorClient::with_interceptor(
            pbs_channel,
            AuthInterceptor::new(local_config.uuid.clone(), cluster_info.keypair().pubkey()),
        );

        let subscription_filters = timeout(
            *connection_timeout,
            pbs_client.get_subscription_filters(SubscriptionFiltersRequest {}),
        )
        .await
        .map_err(|_| PbsError::MethodTimeout("pbs_simulation_settings".to_string()))?
        .map_err(|e| PbsError::MethodError(e.to_string()))?
        .into_inner()
        .try_into()?;

        Self::start_consuming(
            local_config,
            global_config,
            pbs_client,
            bundle_tx,
            receiver,
            is_pbs_active,
            exit,
            bank_forks,
            slot_boundary_receiver,
            connection_timeout,
            &subscription_filters,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_consuming(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        mut client: PbsValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        receiver: &mut UnboundedReceiver<(BankingPacketBatch, Instant)>,
        is_pbs_active: &Arc<AtomicBool>,
        exit: &Arc<AtomicBool>,
        bank_forks: &Arc<RwLock<BankForks>>,
        slot_boundary_receiver: &mut tokio::sync::watch::Receiver<SlotBoundaryStatus>,
        connection_timeout: &Duration,
        subscription_filters: &SubscriptionFilters,
    ) -> Result<(), PbsError> {
        const METRICS_TICK: Duration = Duration::from_secs(1);

        let (remote_sender, remote_receiver) = tokio::sync::mpsc::unbounded_channel();
        let remote_receiver_stream = UnboundedReceiverStream::new(remote_receiver);

        let mut bundles_stream = timeout(
            *connection_timeout,
            client.subscribe_sanitized(remote_receiver_stream),
        )
        .await
        .map_err(|_| PbsError::MethodTimeout("pbs_subscribe".to_string()))?
        .map_err(|e| PbsError::MethodError(e.to_string()))?
        .into_inner();

        let mut pbs_stats = PbsStageStats::default();
        let mut retry_bundles = Vec::new();
        let mut slot_boundary_status = *slot_boundary_receiver.borrow_and_update();
        let mut metrics_tick = interval(METRICS_TICK);
        is_pbs_active.store(true, Ordering::Relaxed);

        info!("connected to pbs stream");
        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                biased;

                maybe_slot_boundary_status = slot_boundary_receiver.changed() => {
                    maybe_slot_boundary_status.map_err(|_| PbsError::SlotBoundaryCheckerError)?;
                    slot_boundary_status = *slot_boundary_receiver.borrow_and_update();
                    if let SlotBoundaryStatus::InProgress = slot_boundary_status {
                        bundle_tx.send(retry_bundles).map_err(|_| PbsError::PacketForwardError)?;
                        retry_bundles = Vec::new();
                    }
                }

                maybe_bundles = bundles_stream.message() => {
                    let bundles = Self::handle_maybe_bundles(maybe_bundles, &mut pbs_stats)?;
                    if let SlotBoundaryStatus::StandBy = slot_boundary_status {
                        retry_bundles.extend(bundles.clone());
                    }
                    bundle_tx.send(bundles).map_err(|_| PbsError::PacketForwardError)?;
                }

                Some((packet_batch, deadline)) = receiver.recv() => {
                    Self::handle_packet_batch(packet_batch, deadline, &remote_sender, bank_forks, subscription_filters, &mut pbs_stats)?;
                }

                _ = metrics_tick.tick() => {
                    pbs_stats.report();
                    pbs_stats = PbsStageStats::default();

                    let global_config = global_config.clone();
                    if *local_config != task::spawn_blocking(move || global_config.lock().unwrap().clone())
                        .await
                        .unwrap() {
                        return Err(PbsError::AuthenticationConnectionError("pbs config changed".to_string()));
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_maybe_bundles(
        maybe_bundles_response: Result<Option<BundlesResponse>, Status>,
        pbs_stats: &mut PbsStageStats,
    ) -> Result<Vec<PacketBundle>, PbsError> {
        let bundles_response = maybe_bundles_response?.ok_or(PbsError::GrpcStreamDisconnected)?;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .map(|bundle| PacketBundle {
                batch: PacketBatch::new(
                    bundle
                        .packets
                        .into_iter()
                        .map(proto_packet_to_packet)
                        .collect(),
                ),
                bundle_id: bundle.uuid,
            })
            .collect();

        saturating_add_assign!(pbs_stats.num_bundles, bundles.len() as u64);
        saturating_add_assign!(
            pbs_stats.num_bundle_packets,
            bundles.iter().map(|bundle| bundle.batch.len() as u64).sum()
        );
        Ok(bundles)
    }

    fn handle_packet_batch(
        packet_batches: BankingPacketBatch,
        deadline: Instant,
        remote_sender: &UnboundedSender<SanitizedTransactionRequest>,
        bank_forks: &Arc<RwLock<BankForks>>,
        subscription_filters: &SubscriptionFilters,
        pbs_stats: &mut PbsStageStats,
    ) -> Result<(), PbsError> {
        if deadline < Instant::now() {
            saturating_add_assign!(pbs_stats.num_missing_deadlines, 1);
            return Ok(());
        }

        if packet_batches.0.is_empty() {
            saturating_add_assign!(pbs_stats.num_empty_packet_batches, 1);
            return Ok(());
        }

        let transactions_with_simulation: Vec<_> = {
            let bank = bank_forks.read().unwrap().working_bank();

            packet_batches
                .0
                .iter()
                .flat_map(|batch| {
                    batch
                        .iter()
                        .filter(|packet| !packet.meta().discard())
                        .filter_map(|packet| {
                            let tx = packet.deserialize_slice(..).ok()?;
                            SanitizedTransaction::try_create(
                                tx,
                                MessageHash::Compute,
                                None,
                                bank.as_ref(),
                            )
                            .ok()
                        })
                })
                .inspect(|_| saturating_add_assign!(pbs_stats.num_sanitized_transactions, 1))
                .filter(|tx| subscription_filters.is_tx_have_to_be_processed(tx))
                .inspect(|_| saturating_add_assign!(pbs_stats.num_filtered_for_sims, 1))
                .filter_map(|tx| {
                    let (simulation_result, simulation_us) =
                        measure_us!(simulate(&tx, bank.as_ref()));
                    saturating_add_assign!(pbs_stats.simulation_us, simulation_us);
                    let transaction = sanitized_to_proto_sanitized(tx)?;

                    let simulation = match simulation_result {
                        Ok(simulation_result) => {
                            saturating_add_assign!(pbs_stats.num_success_simulations, 1);
                            transaction_with_simulation_result::Simulation::SimulationResult(
                                simulation_result,
                            )
                        }
                        Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(_)) => {
                            saturating_add_assign!(pbs_stats.num_proc_time_exceeded_sims, 1);
                            transaction_with_simulation_result::Simulation::BundleError(
                                ProtoBundleError::ProcessingTimeExceeded as i32,
                            )
                        }
                        Err(LoadAndExecuteBundleError::LockError { .. }) => {
                            saturating_add_assign!(pbs_stats.num_lock_err_sims, 1);
                            transaction_with_simulation_result::Simulation::BundleError(
                                ProtoBundleError::LockError as i32,
                            )
                        }
                        Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts) => {
                            saturating_add_assign!(pbs_stats.num_invalid_accs_sims, 1);
                            transaction_with_simulation_result::Simulation::BundleError(
                                ProtoBundleError::InvalidPreOrPostAccounts as i32,
                            )
                        }
                        Err(err @ LoadAndExecuteBundleError::TransactionError { .. }) => {
                            saturating_add_assign!(pbs_stats.num_failed_sims, 1);
                            transaction_with_simulation_result::Simulation::TransactionError(
                                ProtoTransactionError {
                                    err: err.to_string(),
                                },
                            )
                        }
                    };
                    Some(ProtoTransactionWithSimulationResult {
                        transaction: Some(transaction),
                        simulation: Some(simulation),
                    })
                })
                .collect()
        };

        if transactions_with_simulation.is_empty() {
            saturating_add_assign!(pbs_stats.num_empty_tx_batches, 1);
            return Ok(());
        }

        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if remote_sender
            .send(SanitizedTransactionRequest {
                ts: Some(Timestamp {
                    seconds: ts.as_secs() as i64,
                    nanos: ts.subsec_nanos() as i32,
                }),
                transactions_with_simulation,
            })
            .is_err()
        {
            return Err(PbsError::GrpcStreamDisconnected);
        }

        Ok(())
    }

    async fn delay_packet_batches(
        mut sigverified_receiver: UnboundedReceiver<BankingPacketBatch>,
        banking_packet_sender: BankingPacketSender,
        pbs_sender: UnboundedSender<(BankingPacketBatch, Instant)>,
        exit: Arc<AtomicBool>,
        mut slot_boundary_receiver: tokio::sync::watch::Receiver<SlotBoundaryStatus>,
    ) {
        const SLOT_START_DELAY: Duration = Duration::from_millis(20);
        const DELAY_PACKET_BATCHES: Duration = Duration::from_millis(DELAY_PACKET_BATCHES_MS);
        let mut delayed_queue: DelayQueue<BankingPacketBatch> = DelayQueue::new();

        let mut status = match *slot_boundary_receiver.borrow_and_update() {
            SlotBoundaryStatus::StandBy => RunningLeaderStatus::StandBy,
            SlotBoundaryStatus::InProgress => RunningLeaderStatus::InProgress,
        };

        // The maximum duration for a sleep, so it will not wake up
        let slot_start_delay = tokio::time::sleep(Duration::from_secs(68719476734));
        tokio::pin!(slot_start_delay);

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                biased;

                maybe_slot_boundary_changed = slot_boundary_receiver.changed() => {
                    if maybe_slot_boundary_changed.is_err() {
                        error!("slot boundary checker sender closed");
                        break;
                    }
                    status = match (status, *slot_boundary_receiver.borrow_and_update()) {
                        (RunningLeaderStatus::StandBy, SlotBoundaryStatus::InProgress) | (RunningLeaderStatus::Completed, SlotBoundaryStatus::InProgress) =>
                        {
                            slot_start_delay.as_mut().reset(Instant::now() + SLOT_START_DELAY);
                            RunningLeaderStatus::BankStart
                        },
                        (RunningLeaderStatus::BankStart, SlotBoundaryStatus::StandBy) => RunningLeaderStatus::StandBy,
                        (RunningLeaderStatus::InProgress, SlotBoundaryStatus::StandBy) => RunningLeaderStatus::Completed,
                        (_, _) => status,
                    };
                }

                _ = &mut slot_start_delay, if matches!(status, RunningLeaderStatus::BankStart) => {
                    status = RunningLeaderStatus::InProgress;
                }

                Some(packet_batch) = delayed_queue.next(), if matches!(status, RunningLeaderStatus::InProgress | RunningLeaderStatus::Completed) => {
                    match packet_batch {
                        Ok(packet_batch) => {
                            if banking_packet_sender.send(packet_batch.into_inner()).is_err() {
                                error!("banking packet receiver closed");
                                break;
                            }
                            if delayed_queue.is_empty() && matches!(status, RunningLeaderStatus::Completed) {
                                status = RunningLeaderStatus::StandBy;
                            }
                        }
                        Err(err) => {
                            warn!("delayed_queue timer error: {}", err.to_string());
                        }
                    }
                }

                Some(packet_batch) = sigverified_receiver.recv() => {
                    let deadline = Instant::now() + DELAY_PACKET_BATCHES;
                    delayed_queue.insert_at(packet_batch.clone(), deadline);
                    if pbs_sender.send((packet_batch, deadline)).is_err() {
                        error!("pbs receiver closed");
                        break;
                    }
                }
            }
        }
    }

    fn start_slot_boundary_checker(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        leader_status_sender: tokio::sync::watch::Sender<SlotBoundaryStatus>,
        exit: Arc<AtomicBool>,
    ) -> TokioJoinHandle<()> {
        tokio::spawn(Self::slot_boundary_checker(
            poh_recorder,
            leader_status_sender,
            exit,
        ))
    }

    async fn slot_boundary_checker(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        leader_status_sender: tokio::sync::watch::Sender<SlotBoundaryStatus>,
        exit: Arc<AtomicBool>,
    ) {
        const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);
        let mut slot_boundary_check_tick = interval(SLOT_BOUNDARY_CHECK_PERIOD);

        while !exit.load(Ordering::Relaxed) {
            slot_boundary_check_tick.tick().await;
            let recent_status =
                SlotBoundaryStatus::from(is_poh_recorder_in_progress(&poh_recorder));
            leader_status_sender.send_if_modified(|status| {
                if *status != recent_status {
                    *status = recent_status;
                    true
                } else {
                    false
                }
            });
        }
    }

    pub fn is_valid_pbs_config(config: &PbsConfig) -> bool {
        if config.pbs_url.is_empty() {
            warn!("can't connect to pbs. missing pbs_url.");
            return false;
        }
        if config.uuid.is_empty() {
            warn!("can't connect to pbs. missing uuid.");
            return false;
        }
        if let Err(e) = tonic::metadata::MetadataValue::try_from(&config.uuid) {
            warn!("can't connect to pbs. invalid uuid - {}", e.to_string());
            return false;
        }
        true
    }
}

fn sanitized_to_proto_sanitized(tx: SanitizedTransaction) -> Option<ProtoSanitizedTransaction> {
    let versioned_transaction = bincode::serialize(&tx.to_versioned_transaction()).ok()?;
    let message_hash = tx.message_hash().to_bytes().to_vec();
    let loaded_addresses = bincode::serialize(&tx.get_loaded_addresses()).ok()?;

    Some(ProtoSanitizedTransaction {
        versioned_transaction,
        message_hash,
        loaded_addresses,
    })
}

fn inner_instructions_to_proto(
    inner_instructions: Option<&InnerInstructionsList>,
) -> ProtoSimulationResult {
    use forge_protos::proto::pbs::{
        InnerInstruction as ProtoInnerInstruction, InnerInstructions as ProtoInnerInstructions,
    };

    ProtoSimulationResult {
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
        inner_instructions_none: inner_instructions.is_none(),
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum RunningLeaderStatus {
    #[default]
    StandBy,
    BankStart,
    InProgress,
    Completed,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum SlotBoundaryStatus {
    #[default]
    StandBy,
    InProgress,
}

impl From<bool> for SlotBoundaryStatus {
    fn from(value: bool) -> Self {
        if value {
            Self::InProgress
        } else {
            Self::StandBy
        }
    }
}

fn is_poh_recorder_in_progress(poh_recorder: &Arc<RwLock<PohRecorder>>) -> bool {
    let poh_recorder = poh_recorder.read().unwrap();
    poh_recorder
        .bank_start()
        .map(|bank_start| bank_start.should_working_bank_still_be_processing_txs())
        .unwrap_or_default()
}

struct SubscriptionFilters {
    stream_all: bool,
    account_include: Vec<Pubkey>,
    account_exclude: Vec<Pubkey>,
    account_required: Vec<Pubkey>,
}

impl TryFrom<SubscriptionFiltersResponse> for SubscriptionFilters {
    type Error = PbsError;
    fn try_from(value: SubscriptionFiltersResponse) -> Result<Self, Self::Error> {
        let SubscriptionFiltersResponse {
            stream_all,
            account_include,
            account_exclude,
            account_required,
        } = value;

        Ok(Self {
            stream_all: stream_all.unwrap_or_default(),
            account_include: decode_pubkeys_into_vec(account_include)?,
            account_exclude: decode_pubkeys_into_vec(account_exclude)?,
            account_required: decode_pubkeys_into_vec(account_required)?,
        })
    }
}

fn decode_pubkeys_into_vec(pubkeys: Vec<Vec<u8>>) -> Result<Vec<Pubkey>, PbsError> {
    let mut vec: Vec<_> = pubkeys
        .into_iter()
        .map(Pubkey::try_from)
        .collect::<Result<_, _>>()
        .map_err(|_| PbsError::SimulationSettingsError)?;
    vec.sort();
    Ok(vec)
}

impl SubscriptionFilters {
    pub fn is_tx_have_to_be_processed(&self, transaction: &SanitizedTransaction) -> bool {
        if self.stream_all {
            return true;
        }

        let accounts = transaction.message().account_keys();

        if !self.account_include.is_empty()
            && accounts
                .iter()
                .all(|pubkey| self.account_include.binary_search(pubkey).is_err())
        {
            return false;
        }

        if !self.account_exclude.is_empty()
            && accounts
                .iter()
                .any(|pubkey| self.account_exclude.binary_search(pubkey).is_ok())
        {
            return false;
        }

        if !self.account_required.is_empty() {
            let mut other: Vec<&Pubkey> = accounts.iter().collect();

            let is_subset = if self.account_required.len() <= other.len() {
                other.sort();
                self.account_required
                    .iter()
                    .all(|pubkey| other.binary_search(&pubkey).is_ok())
            } else {
                false
            };

            if !is_subset {
                return false;
            }
        }

        true
    }
}

fn simulate(
    tx: &SanitizedTransaction,
    bank: &Bank,
) -> LoadAndExecuteBundleResult<ProtoSimulationResult> {
    const MAX_BUNDLE_SIMULATION_TIME: Duration = Duration::from_millis(50);

    let sanitized_bundle = SanitizedBundle {
        bundle_id: derive_bundle_id_from_sanitized_transactions(from_ref(tx)),
        transactions: vec![tx.clone()],
    };

    let bundle_execution_result = load_and_execute_bundle(
        bank,
        &sanitized_bundle,
        MAX_PROCESSING_AGE,
        &MAX_BUNDLE_SIMULATION_TIME,
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
        let inner_instructions = bundle_execution_result
            .bundle_transaction_results()
            .first()
            .and_then(|bundle_execution_result| bundle_execution_result.execution_results().first())
            .and_then(|transaction_execution_result| transaction_execution_result.details())
            .and_then(|transaction_execution_details| {
                transaction_execution_details.inner_instructions.as_ref()
            });
        inner_instructions_to_proto(inner_instructions)
    })
}
