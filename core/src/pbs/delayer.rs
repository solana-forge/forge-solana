use std::sync::RwLock;
use {
    crate::{
        banking_trace::{BankingPacketBatch, BankingPacketSender},
        pbs::{filters::SubscriptionFilters, grpc::PbsBatch, slot_boundary::SlotBoundaryStatus},
    },
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    tokio::{
        sync::{
            mpsc::{UnboundedReceiver, UnboundedSender},
            watch,
        },
        task::JoinHandle,
        time::Instant,
    },
};
use solana_runtime::bank_forks::BankForks;

pub type TimestampedBankingPacketBatch = (BankingPacketBatch, Instant);

pub struct PacketDelayer {
    receiver: UnboundedReceiver<TimestampedBankingPacketBatch>,
    banking: BankingPacketSender,
    pbs: UnboundedSender<PbsBatch>,
    slot_boundary_watch: watch::Receiver<SlotBoundaryStatus>,
    connection_watch: watch::Receiver<bool>,
    filters_watch: watch::Receiver<Option<SubscriptionFilters>>,
    bank_forks: Arc<RwLock<BankForks>>,
    exit: Arc<AtomicBool>,
}

async fn run_delayer(mut actor: PacketDelayer) {
    while !actor.exit.load(Ordering::Relaxed) {
        todo!()
    }
}

impl PacketDelayer {
    pub fn start(
        receiver: UnboundedReceiver<TimestampedBankingPacketBatch>,
        banking: BankingPacketSender,
        pbs: UnboundedSender<PbsBatch>,
        slot_boundary_watch: watch::Receiver<SlotBoundaryStatus>,
        connection_watch: watch::Receiver<bool>,
        filters_watch: watch::Receiver<Option<SubscriptionFilters>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let actor = PacketDelayer {
            receiver,
            banking,
            pbs,
            slot_boundary_watch,
            connection_watch,
            filters_watch,
            bank_forks,
            exit,
        };

        tokio::spawn(run_delayer(actor))
    }
}
