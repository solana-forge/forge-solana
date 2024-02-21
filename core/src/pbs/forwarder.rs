use {
    crate::{
        banking_trace::{BankingPacketReceiver, BankingPacketSender},
        pbs::{delayer::TimestampedBankingPacketBatch, filters::SubscriptionFilters},
    },
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    tokio::{
        sync::{mpsc::UnboundedSender, watch},
        task::{self, JoinHandle},
        time::Instant,
    },
};

pub struct PacketBatchesForwarder {
    receiver: BankingPacketReceiver,
    delayer: UnboundedSender<TimestampedBankingPacketBatch>,
    banking: BankingPacketSender,

    connection_watch: watch::Receiver<Option<SubscriptionFilters>>,
    exit: Arc<AtomicBool>,
}

// Forward packets from the sigverified_receiver to the banking_packet_sender if pbs isn't ready
fn run_packet_batch_forwarder(actor: PacketBatchesForwarder) {
    let PacketBatchesForwarder {
        receiver,
        delayer,
        banking,
        connection_watch,
        exit,
    } = actor;

    while !exit.load(Ordering::Relaxed) {
        let Ok(packet_batch) = receiver.recv() else {
            error!("sigverified packet receiver closed");
            break;
        };
        if connection_watch.borrow().is_some() {
            if delayer.send((packet_batch, Instant::now())).is_err() {
                error!("pbs stage packet receiver close");
                break;
            }
        } else if banking.send(packet_batch).is_err() {
            error!("banking packet sender closed");
            break;
        }
    }
}

impl PacketBatchesForwarder {
    pub fn start(
        receiver: BankingPacketReceiver,
        delayer: UnboundedSender<TimestampedBankingPacketBatch>,
        banking: BankingPacketSender,
        connection_watch: watch::Receiver<Option<SubscriptionFilters>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let actor = PacketBatchesForwarder {
            receiver,
            delayer,
            banking,
            connection_watch,
            exit,
        };

        task::spawn_blocking(move || run_packet_batch_forwarder(actor))
    }
}
