use {
    crate::{
        banking_trace::{BankingPacketReceiver, BankingPacketSender},
        packet_bundle::PacketBundle,
        pbs::{
            delayer::PacketDelayer,
            extract_first_signature,
            filters::SubscriptionFilters,
            forwarder::PacketBatchesForwarder,
            grpc::{
                sanitized_to_proto_sanitized, simulation_result_to_proto_simulation_result,
                PbsBatch,
            },
            interceptor::AuthInterceptor,
            slot_boundary::{SlotBoundaryChecker, SlotBoundaryStatus},
            PbsError,
        },
        proto_packet_to_packet,
    },
    crossbeam_channel::Sender,
    forge_protos::proto::pbs::{
        pbs_validator_client::PbsValidatorClient, BundlesResponse, SanitizedTransactionRequest,
        SubscriptionFiltersRequest,
        TransactionWithSimulationResult as ProtoTransactionWithSimulationResult,
    },
    prost_types::Timestamp,
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        saturating_add_assign,
        signature::{Signature, Signer},
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
        sync::{
            mpsc,
            mpsc::{UnboundedReceiver, UnboundedSender},
            watch,
        },
        task,
        time::{interval, sleep, timeout},
        try_join,
    },
    tokio_stream::wrappers::UnboundedReceiverStream,
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Status,
    },
};

const CONNECTION_TIMEOUT_S: u64 = 10;
const CONNECTION_BACKOFF_S: u64 = 5;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PbsConfig {
    pub pbs_url: String,
    pub uuid: String,
}

pub struct PbsEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}

#[derive(Default)]
struct PbsStageStats {
    num_bundles: u64,
    num_bundle_packets: u64,
    num_empty_packet_batches: u64,
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
            (
                "num_empty_packet_batches",
                self.num_empty_packet_batches,
                i64
            ),
        );
        datapoint_info!(
            "pbs_stage-stats",
            ("num_empty_tx_batches", self.num_empty_tx_batches, i64),
        );
    }
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

        let (mut connection_updater, connection_watch) = watch::channel(None);
        let (forwarder_sender, delayer_receiver) = mpsc::unbounded_channel();
        let forwarder_jh = PacketBatchesForwarder::start(
            sigverified_receiver,
            forwarder_sender,
            banking_packet_sender.clone(),
            connection_watch.clone(),
            exit.clone(),
        );

        let (mut slot_boundary_watch, slot_boundary_checker_jh) =
            SlotBoundaryChecker::start(poh_recorder, exit.clone());
        let (delayer_sender, mut pbs_receiver) = mpsc::unbounded_channel();
        let (drop_packets_sender, drop_packets_receiver) = mpsc::unbounded_channel();

        let delayer_jh = PacketDelayer::start(
            delayer_receiver,
            banking_packet_sender.clone(),
            delayer_sender,
            drop_packets_receiver,
            slot_boundary_watch.clone(),
            connection_watch,
            bank_forks,
            exit.clone(),
        );

        let mut error_count: u64 = 0;

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
                &mut connection_updater,
                &drop_packets_sender,
                &exit,
                &mut slot_boundary_watch,
                &CONNECTION_TIMEOUT,
            )
            .await
            {
                connection_updater.send_if_modified(|connected| {
                    if connected.is_some() {
                        *connected = None;
                        true
                    } else {
                        false
                    }
                });

                error_count += 1;
                datapoint_warn!(
                    "pbs_stage-error",
                    ("count", error_count, i64),
                    ("error", err.to_string(), String),
                );
                sleep(CONNECTION_BACKOFF).await;
            }
        }

        try_join!(forwarder_jh, delayer_jh, slot_boundary_checker_jh).unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_and_stream(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        cluster_info: &Arc<ClusterInfo>,
        receiver: &mut UnboundedReceiver<PbsBatch>,
        connection_updater: &mut watch::Sender<Option<SubscriptionFilters>>,
        drop_packets_sender: &UnboundedSender<Vec<Signature>>,
        exit: &Arc<AtomicBool>,
        slot_boundary_watch: &mut watch::Receiver<SlotBoundaryStatus>,
        connection_timeout: &Duration,
    ) -> Result<(), PbsError> {
        let mut endpoint = Endpoint::from_shared(local_config.pbs_url.clone())
            .map_err(|_| {
                PbsError::PbsConnectionError(format!(
                    "invalid block engine url value: {}",
                    local_config.pbs_url
                ))
            })?
            .tcp_keepalive(Some(Duration::from_secs(60)));

        if local_config.pbs_url.starts_with("https") {
            endpoint = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|_| {
                    PbsError::PbsConnectionError(
                        "failed to set tls_config for block engine service".to_string(),
                    )
                })?;
        }

        debug!("connecting to block engine: {}", local_config.pbs_url);

        let channel = timeout(*connection_timeout, endpoint.connect())
            .await
            .map_err(|_| PbsError::PbsConnectionTimeout)?
            .map_err(|e| PbsError::PbsConnectionError(e.to_string()))?;

        let mut pbs_client = PbsValidatorClient::with_interceptor(
            channel,
            AuthInterceptor::new(local_config.uuid.clone(), cluster_info.keypair().pubkey()),
        );

        let subscription_filters = timeout(
            *connection_timeout,
            pbs_client.get_subscription_filters(SubscriptionFiltersRequest {}),
        )
        .await
        .map_err(|_| PbsError::MethodTimeout("pbs_subscription_filters".to_string()))?
        .map_err(|e| PbsError::MethodError(e.to_string()))?
        .into_inner()
        .try_into()?;

        Self::start_consuming(
            local_config,
            global_config,
            pbs_client,
            bundle_tx,
            receiver,
            connection_updater,
            subscription_filters,
            drop_packets_sender,
            exit,
            slot_boundary_watch,
            connection_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_consuming(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        mut client: PbsValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        receiver: &mut UnboundedReceiver<PbsBatch>,
        connection_updater: &mut watch::Sender<Option<SubscriptionFilters>>,
        subscription_filters: SubscriptionFilters,
        drop_packets_sender: &UnboundedSender<Vec<Signature>>,
        exit: &Arc<AtomicBool>,
        slot_boundary_watch: &mut watch::Receiver<SlotBoundaryStatus>,
        connection_timeout: &Duration,
    ) -> Result<(), PbsError> {
        const METRICS_TICK: Duration = Duration::from_secs(1);

        let (remote_sender, remote_receiver) = mpsc::unbounded_channel();
        let stream = UnboundedReceiverStream::new(remote_receiver);

        let mut bundles_stream = timeout(*connection_timeout, client.subscribe_sanitized(stream))
            .await
            .map_err(|_| PbsError::MethodTimeout("pbs_subscribe".to_string()))?
            .map_err(|e| PbsError::MethodError(e.to_string()))?
            .into_inner();

        let mut pbs_stats = PbsStageStats::default();
        let mut retry_bundles = Vec::new();
        let mut slot_boundary_status = *slot_boundary_watch.borrow_and_update();
        let mut metrics_tick = interval(METRICS_TICK);
        connection_updater
            .send(Some(subscription_filters))
            .map_err(|_| {
                PbsError::PbsConnectionError("connection watchers are close".to_string())
            })?;

        info!("connected to pbs stream");
        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                biased;

                maybe_slot_boundary_status = slot_boundary_watch.changed() => {
                    maybe_slot_boundary_status.map_err(|_| PbsError::SlotBoundaryCheckerError)?;
                    slot_boundary_status = *slot_boundary_watch.borrow_and_update();
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
                    Self::send_bundles_to_delayer(&bundles, drop_packets_sender)?;

                    // NOTE: bundles are sanitized in bundle_sanitizer module
                    bundle_tx.send(bundles).map_err(|_| PbsError::PacketForwardError)?;

                }

                Some(packet_batch) = receiver.recv() => {
                    Self::handle_packet_batch(packet_batch, &remote_sender, &mut pbs_stats)?;
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
        packet_batch: PbsBatch,
        remote_sender: &UnboundedSender<SanitizedTransactionRequest>,
        pbs_stats: &mut PbsStageStats,
    ) -> Result<(), PbsError> {
        if packet_batch.is_empty() {
            saturating_add_assign!(pbs_stats.num_empty_packet_batches, 1);
            return Ok(());
        }

        let transactions_with_simulation: Vec<_> = packet_batch
            .into_iter()
            .filter_map(|item| {
                let transaction = sanitized_to_proto_sanitized(item.transaction)?;
                let simulation = item
                    .simulation_result
                    .map(simulation_result_to_proto_simulation_result);
                Some(ProtoTransactionWithSimulationResult {
                    transaction: Some(transaction),
                    simulation,
                })
            })
            .collect();

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

    fn send_bundles_to_delayer(
        bundles: &[PacketBundle],
        drop_packets_sender: &UnboundedSender<Vec<Signature>>,
    ) -> Result<(), PbsError> {
        let signatures = bundles
            .iter()
            .flat_map(|bundle| {
                bundle
                    .batch
                    .iter()
                    .filter_map(|packet| extract_first_signature(packet).ok())
            })
            .collect();

        drop_packets_sender
            .send(signatures)
            .map_err(|_| PbsError::PacketForwardError)
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
