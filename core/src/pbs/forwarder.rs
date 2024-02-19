use tokio::time::Instant;
use {
    crate::banking_trace::{BankingPacketBatch, BankingPacketReceiver, BankingPacketSender},
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    tokio::{
        sync::mpsc::UnboundedSender,
        task::{self, JoinHandle},
    },
};

pub struct PacketBatchesForwarder {
    receiver: BankingPacketReceiver,
    pbs_sender: UnboundedSender<(BankingPacketBatch, Instant)>,
    banking_sender: BankingPacketSender,

    is_pbs_active: Arc<AtomicBool>,
    exit: Arc<AtomicBool>,
}

// Forward packets from the sigverified_receiver to the banking_packet_sender if pbs isn't ready
fn run_packet_batch_forwarder(actor: PacketBatchesForwarder) {
    let PacketBatchesForwarder {
        receiver,
        pbs_sender,
        banking_sender,
        is_pbs_active,
        exit,
    } = actor;

    while !exit.load(Ordering::Relaxed) {
        let Ok(packet_batch) = receiver.recv() else {
            error!("sigverified packet receiver closed");
            break;
        };
        if is_pbs_active.load(Ordering::Relaxed) {
            if pbs_sender.send((packet_batch, Instant::now())).is_err() {
                error!("pbs stage packet receiver close");
                break;
            }
        } else if banking_sender.send(packet_batch).is_err() {
            error!("banking packet sender closed");
            break;
        }
    }
}

impl PacketBatchesForwarder {
    pub fn new(
        receiver: BankingPacketReceiver,
        pbs_sender: UnboundedSender<BankingPacketBatch>,
        banking_sender: BankingPacketSender,
        is_pbs_active: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let actor = PacketBatchesForwarder {
            receiver,
            pbs_sender,
            banking_sender,
            is_pbs_active,
            exit,
        };

        task::spawn_blocking(move || run_packet_batch_forwarder(actor))
    }
}
