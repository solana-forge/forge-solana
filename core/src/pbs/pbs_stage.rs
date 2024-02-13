use {
    crate::{
        banking_trace::{BankingPacketBatch, BankingPacketReceiver, BankingPacketSender},
        packet_bundle::PacketBundle,
        pbs::{interceptor::AuthInterceptor, PbsError},
        proto_packet_to_packet,
    },
    crossbeam_channel::{SendError, Sender},
    forge_protos::proto::pbs::{
        pbs_validator_client::PbsValidatorClient, BundlesResponse,
        SanitizedTransaction as ProtoSanitizedTransaction, SanitizedTransactionRequest,
    },
    futures::StreamExt,
    prost_types::Timestamp,
    reqwest::header,
    solana_perf::{packet::PacketBatch, sigverify::PacketError},
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        packet::Packet,
        saturating_add_assign,
        short_vec::decode_shortu16_len,
        signature::Signature,
        transaction::{MessageHash, SanitizedTransaction},
    },
    std::{
        collections::HashSet,
        mem::size_of,
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

const REQUEST_TIMEOUT_MS: u64 = 300;

const BUNDLE_LIFE_TIME_IN_DELAYER_MS: u64 = 400;

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
        // Channel that trusted packets after SigVerify get piped through.
        sigverified_receiver: BankingPacketReceiver,
        // Channel that trusted packets get piped through.
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        bank_forks: Arc<RwLock<BankForks>>,
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
                    sigverified_receiver,
                    banking_packet_sender,
                    exit,
                    bank_forks,
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
        sigverified_receiver: BankingPacketReceiver,
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        bank_forks: Arc<RwLock<BankForks>>,
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

        let (pbs_sender, mut pbs_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (drop_packet_sender, bundle_receiver) = tokio::sync::mpsc::unbounded_channel();
        let delayer_join_handle = Self::start_delayer(
            forwarder_receiver,
            banking_packet_sender,
            pbs_sender,
            bundle_receiver,
            exit.clone(),
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
                &mut pbs_receiver,
                &drop_packet_sender,
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

        try_join!(forwarder_join_handle, delayer_join_handle).unwrap();
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
        bundle_receiver: UnboundedReceiver<Vec<Signature>>,
        exit: Arc<AtomicBool>,
    ) -> TokioJoinHandle<()> {
        tokio::spawn(Self::delay_packet_batches(
            sigverified_receiver,
            banking_packet_sender,
            pbs_sender,
            bundle_receiver,
            exit,
        ))
    }

    async fn connect_and_stream(
        local_config: &PbsConfig,
        global_config: &Arc<Mutex<PbsConfig>>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        receiver: &mut UnboundedReceiver<(BankingPacketBatch, Instant)>,
        drop_packet_sender: &UnboundedSender<Vec<Signature>>,
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
            AuthInterceptor::new(local_config.uuid.clone()),
        );

        Self::start_consuming(
            local_config,
            global_config,
            pbs_client,
            bundle_tx,
            receiver,
            drop_packet_sender,
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
        drop_packet_sender: &UnboundedSender<Vec<Signature>>,
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
                    Self::handle_maybe_bundles(maybe_bundles, bundle_tx, drop_packet_sender, &mut pbs_stats)?;
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
        delayer_sender: &UnboundedSender<Vec<Signature>>,
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

        Self::send_bundles_to_delayer(&bundles, delayer_sender)?;

        // NOTE: bundles are sanitized in bundle_sanitizer module
        bundle_sender
            .send(bundles)
            .map_err(|_| PbsError::PacketForwardError)
    }

    fn send_bundles_to_delayer(
        bundles: &[PacketBundle],
        delayer_sender: &UnboundedSender<Vec<Signature>>,
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

        delayer_sender
            .send(signatures)
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
        mut bundle_receiver: UnboundedReceiver<Vec<Signature>>,
        exit: Arc<AtomicBool>,
    ) {
        const DELAY_PACKET_BATCHES: Duration = Duration::from_millis(REQUEST_TIMEOUT_MS);

        let mut delayed_queue: DelayQueue<BankingPacketBatch> = DelayQueue::new();

        let mut bundles: HashSet<Signature> = HashSet::new();
        let mut bundles_expirations: DelayQueue<Signature> = DelayQueue::new();

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                biased;

                Some(signatures) = bundle_receiver.recv() => {
                    Self::handle_bundles_in_delayer(signatures, &mut bundles, &mut bundles_expirations);
                }

                Some(maybe_signature) = bundles_expirations.next() => {
                    match maybe_signature {
                        Ok(signature) => {
                            bundles.remove(&signature.into_inner());
                        }
                        Err(err) => {
                            warn!("delayed_queue timer error: {}", err.to_string());
                        }
                    }
                }

                Some(maybe_packet_batch) = delayed_queue.next() => {
                    match maybe_packet_batch {
                        Ok(packet_batch) => {
                            if let Err(_) = Self::handle_delayed_banking_packet_batch(packet_batch.into_inner(), &banking_packet_sender, &bundles) {
                                error!("banking packet receiver closed");
                                break;
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

    fn handle_delayed_banking_packet_batch(
        banking_packet_batch: BankingPacketBatch,
        banking_packet_sender: &BankingPacketSender,
        bundles: &HashSet<Signature>,
    ) -> Result<(), SendError<BankingPacketBatch>> {
        let exclude: Vec<_> = banking_packet_batch
            .0
            .iter()
            .enumerate()
            .filter_map(|(pos_outer, batch)| {
                let positions: Vec<_> = batch
                    .iter()
                    .enumerate()
                    .filter_map(|(pos_inner, packet)| {
                        let signature = extract_first_signature(packet).ok()?;
                        bundles.contains(&signature).then_some(pos_inner)
                    })
                    .collect();
                (!positions.is_empty()).then_some((pos_outer, positions))
            })
            .collect();

        if exclude.is_empty() {
            return banking_packet_sender.send(banking_packet_batch);
        }

        let (mut banking_packet_batch, stats) = banking_packet_batch.as_ref().clone();
        for (outer, inner) in exclude {
            let batch = &mut banking_packet_batch[outer];
            for pos in inner {
                batch[pos].meta_mut().set_discard(true);
            }
        }

        banking_packet_sender.send(BankingPacketBatch::new((banking_packet_batch, stats)))
    }

    fn handle_bundles_in_delayer(
        signatures: Vec<Signature>,
        bundles: &mut HashSet<Signature>,
        expirations: &mut DelayQueue<Signature>,
    ) {
        for signature in signatures {
            expirations.insert(
                signature.clone(),
                Duration::from_millis(BUNDLE_LIFE_TIME_IN_DELAYER_MS),
            );
            bundles.insert(signature);
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
        if let Err(e) = header::HeaderValue::from_str(&config.uuid) {
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
