use for_event_bus::SimpleBus;
use for_event_bus::{
    BusError, EntryOfBus, Event, FromTick, IdentityOfMergeTick, IdentityOfRx, Merge, ToWorker,
};
use log::debug;
use std::any::TypeId;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    custom_utils::logger::logger_stdout_debug();
    let bus = SimpleBus::init();

    IntervalWorker::init(&bus).await;
    WorkerDispatcher::init(&bus).await;

    sleep(Duration::from_secs(8)).await;
}

#[derive(Debug, Event)]
struct AEvent {
    value: usize,
}

#[derive(Debug, Event)]
struct Close;

#[derive(Debug)]
enum IntervalSignal {
    AEvent(AEvent),
    Close,
    Tick,
}

impl Event for IntervalSignal {
    fn name() -> &'static str {
        "IntervalSignal"
    }
}

impl Merge for IntervalSignal {
    fn merge(event: for_event_bus::BusEvent) -> Result<Self, BusError> {
        let any = event.as_any();
        if let Ok(msg) = any.clone().downcast::<AEvent>() {
            Ok(Self::AEvent(AEvent { value: msg.value }))
        } else if any.downcast::<Close>().is_ok() {
            Ok(Self::Close)
        } else {
            Err(BusError::downcast_failed("AEvent|Close", event.type_name()))
        }
    }

    fn subscribe_types() -> Vec<(TypeId, &'static str)> {
        vec![
            (TypeId::of::<AEvent>(), AEvent::name()),
            (TypeId::of::<Close>(), Close::name()),
        ]
    }
}

impl FromTick for IntervalSignal {
    fn from_tick() -> Self {
        Self::Tick
    }
}

struct IntervalWorker {
    identity: IdentityOfMergeTick<IntervalSignal>,
}

impl ToWorker for IntervalWorker {
    fn name() -> String {
        "IntervalWorker".to_string()
    }
}

impl IntervalWorker {
    pub async fn init(bus: &EntryOfBus) {
        let identity = bus
            .merge_tick_login::<Self, IntervalSignal>(Duration::from_secs(1))
            .await
            .unwrap();
        Self { identity }.run();
    }

    fn run(mut self) {
        spawn(async move {
            loop {
                match self.identity.recv_signal().await {
                    Ok(Some(IntervalSignal::Tick)) => {
                        debug!("tick: do periodic job");
                    }
                    Ok(Some(IntervalSignal::AEvent(msg))) => {
                        debug!("recv AEvent value={}", msg.value);
                    }
                    Ok(Some(IntervalSignal::Close)) => {
                        debug!("recv Close, worker exit");
                        break;
                    }
                    Ok(None) => {
                        debug!("worker channel closed");
                        break;
                    }
                    Err(err) => {
                        debug!("recv_signal error: {err}");
                        break;
                    }
                }
            }
        });
    }
}

struct WorkerDispatcher {
    identity: IdentityOfRx,
}

impl ToWorker for WorkerDispatcher {
    fn name() -> String {
        "WorkerDispatcher".to_string()
    }
}

impl WorkerDispatcher {
    pub async fn init(bus: &EntryOfBus) {
        let identity = bus.login::<Self>().await.unwrap();
        Self { identity }.run();
    }

    fn run(self) {
        spawn(async move {
            for value in 0usize..5 {
                self.identity
                    .dispatch_event(AEvent { value })
                    .await
                    .unwrap();
                sleep(Duration::from_millis(700)).await;
            }
            self.identity.dispatch_event(Close).await.unwrap();
        });
    }
}
