use {
    solana_poh::poh_recorder::PohRecorder,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
    tokio::{sync::watch, task::JoinHandle, time::interval},
};

const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

pub struct SlotBoundaryChecker {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    leader_status_updater: watch::Sender<SlotBoundaryStatus>,
    exit: Arc<AtomicBool>,
}

impl SlotBoundaryChecker {
    pub fn start(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        exit: Arc<AtomicBool>,
    ) -> (watch::Receiver<SlotBoundaryStatus>, JoinHandle<()>) {
        let (leader_status_updater, leader_status_watch) =
            watch::channel(SlotBoundaryStatus::StandBy);
        let actor = SlotBoundaryChecker {
            poh_recorder,
            leader_status_updater,
            exit,
        };
        (
            leader_status_watch,
            tokio::spawn(run_slot_boundary_checker(actor)),
        )
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum SlotBoundaryStatus {
    #[default]
    StandBy,
    InProgress,
}

async fn run_slot_boundary_checker(actor: SlotBoundaryChecker) {
    let SlotBoundaryChecker {
        poh_recorder,
        leader_status_updater,
        exit,
    } = actor;

    let mut check_tick = interval(SLOT_BOUNDARY_CHECK_PERIOD);
    while !exit.load(Ordering::Relaxed) {
        check_tick.tick().await;
        let recent = SlotBoundaryStatus::from(poh_recorder.as_ref());
        leader_status_updater.send_if_modified(|status| {
            if *status != recent {
                *status = recent;
                true
            } else {
                false
            }
        });
    }
}

impl From<&RwLock<PohRecorder>> for SlotBoundaryStatus {
    fn from(poh_recorder: &RwLock<PohRecorder>) -> Self {
        let poh_recorder = poh_recorder.read().unwrap();
        if poh_recorder
            .bank_start()
            .map(|bank_start| bank_start.should_working_bank_still_be_processing_txs())
            .unwrap_or_default()
        {
            Self::InProgress
        } else {
            Self::StandBy
        }
    }
}
