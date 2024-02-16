use {
    crate::{
        banking_trace::{BankingPacketBatch, BankingPacketReceiver, BankingPacketSender},
        packet_bundle::PacketBundle,
        pbs::{interceptor::AuthInterceptor, PbsError},
        proto_packet_to_packet,
    },
    crossbeam_channel::Sender,
    forge_protos::proto::pbs::{
        pbs_validator_client::PbsValidatorClient, BundlesResponse,
        SanitizedTransaction as ProtoSanitizedTransaction, SanitizedTransactionRequest,
    },
    futures::StreamExt,
    prost_types::Timestamp,
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        saturating_add_assign,
        signature::Signer,
        transaction::{MessageHash, SanitizedTransaction},
    },
    std::{
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

        let (slot_boundary_sender, slot_boundary_receiver) =
            tokio::sync::watch::channel(SlotBoundaryStatus::default());
        let slot_boundary_checker_join_handle =
            Self::start_slot_boundary_checker(poh_recorder, slot_boundary_sender, exit.clone());

        let (pbs_sender, mut pbs_receiver) = tokio::sync::mpsc::unbounded_channel();
        let delayer_join_handle = Self::start_delayer(
            forwarder_receiver,
            banking_packet_sender,
            pbs_sender,
            exit.clone(),
            slot_boundary_receiver,
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
                if let Err(_) = pbs_sender.send(packet_batch) {
                    error!("psb stage packet consumer closed");
                    break;
                }
            } else {
                if let Err(_) = banking_packet_sender.send(packet_batch) {
                    error!("banking packet sender closed");
                    break;
                }
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

    async fn connect_and_stream(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        cluster_info: &Arc<ClusterInfo>,
        receiver: &mut UnboundedReceiver<(BankingPacketBatch, Instant)>,
        is_pbs_active: &Arc<AtomicBool>,
        exit: &Arc<AtomicBool>,
        bank_forks: &Arc<RwLock<BankForks>>,
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

        let pbs_client = PbsValidatorClient::with_interceptor(
            pbs_channel,
            AuthInterceptor::new(local_config.uuid.clone(), cluster_info.keypair().pubkey()),
        );

        Self::start_consuming(
            local_config,
            global_config,
            pbs_client,
            bundle_tx,
            receiver,
            is_pbs_active,
            exit,
            bank_forks,
            connection_timeout,
        )
        .await
    }

    async fn start_consuming(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        mut client: PbsValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        receiver: &mut UnboundedReceiver<(BankingPacketBatch, Instant)>,
        is_pbs_active: &Arc<AtomicBool>,
        exit: &Arc<AtomicBool>,
        bank_forks: &Arc<RwLock<BankForks>>,
        connection_timeout: &Duration,
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

        is_pbs_active.store(true, Ordering::Relaxed);
        let mut pbs_stats = PbsStageStats::default();
        let mut metrics_tick = interval(METRICS_TICK);

        info!("connected to pbs stream");
        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                biased;

                maybe_bundles = bundles_stream.message() => {
                    Self::handle_maybe_bundles(maybe_bundles, bundle_tx, &mut pbs_stats)?;
                }

                Some((packet_batch, deadline)) = receiver.recv() => {
                    Self::handle_packet_batch(packet_batch, deadline, &remote_sender, bank_forks, &mut pbs_stats)?;
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
        bundle_sender: &Sender<Vec<PacketBundle>>,
        pbs_stats: &mut PbsStageStats,
    ) -> Result<(), PbsError> {
        let bundles_response = maybe_bundles_response?.ok_or(PbsError::GrpcStreamDisconnected)?;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                Some(PacketBundle {
                    batch: PacketBatch::new(
                        bundle
                            .packets
                            .into_iter()
                            .map(proto_packet_to_packet)
                            .collect(),
                    ),
                    bundle_id: bundle.uuid,
                })
            })
            .collect();

        saturating_add_assign!(pbs_stats.num_bundles, bundles.len() as u64);
        saturating_add_assign!(
            pbs_stats.num_bundle_packets,
            bundles.iter().map(|bundle| bundle.batch.len() as u64).sum()
        );

        // NOTE: bundles are sanitized in bundle_sanitizer module
        bundle_sender
            .send(bundles)
            .map_err(|_| PbsError::PacketForwardError)
    }

    fn handle_packet_batch(
        packet_batches: BankingPacketBatch,
        deadline: Instant,
        remote_sender: &UnboundedSender<SanitizedTransactionRequest>,
        bank_forks: &Arc<RwLock<BankForks>>,
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

        let bank = bank_forks.read().unwrap().working_bank();

        let transactions: Vec<_> = packet_batches
            .0
            .iter()
            .flat_map(|batch| {
                batch
                    .iter()
                    .filter(|packet| !packet.meta().discard())
                    .filter_map(|packet| {
                        let tx = packet.deserialize_slice(..).ok()?;
                        SanitizedTransaction::try_create(tx, MessageHash::Compute, None, &*bank)
                            .ok()
                            .and_then(sanitized_to_proto_sanitized)
                    })
            })
            .collect();

        if transactions.is_empty() {
            saturating_add_assign!(pbs_stats.num_empty_tx_batches, 1);
            return Ok(());
        }

        let num_sanitized_transactions = transactions.len();

        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if let Err(_) = remote_sender.send(SanitizedTransactionRequest {
            ts: Some(Timestamp {
                seconds: ts.as_secs() as i64,
                nanos: ts.subsec_nanos() as i32,
            }),
            transactions,
        }) {
            return Err(PbsError::GrpcStreamDisconnected);
        }

        saturating_add_assign!(
            pbs_stats.num_sanitized_transactions,
            num_sanitized_transactions as u64
        );

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
                    if let Err(_) = maybe_slot_boundary_changed {
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
                            if let Err(_) = banking_packet_sender.send(packet_batch.into_inner()) {
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
                    if let Err(_) = pbs_sender.send((packet_batch, deadline)) {
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
