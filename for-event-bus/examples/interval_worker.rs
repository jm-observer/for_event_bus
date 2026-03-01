use for_event_bus::SimpleBus;
use for_event_bus::{
    BusError, EntryOfBus, Event, IdentityOfMergeTick, IdentityOfRx, Merge, ToWorker,
};
use log::debug;
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

#[derive(Debug, Clone, Event)]
struct AEvent {
    value: usize,
}

#[derive(Debug, Clone, Event)]
struct Close;

#[derive(Debug, Merge)]
enum IntervalSignal {
    AEvent(AEvent),
    #[merge(skip)]
    Close(Close),
    #[merge(tick)]
    Tick,
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
        identity.subscribe_with_key::<Close>("Close").await.unwrap();
        Self { identity }.run();
    }

    fn run(mut self) {
        spawn(async move {
            loop {
                match self.identity.recv().await {
                    Ok(Some(IntervalSignal::Tick)) => {
                        debug!("tick: do periodic job");
                    }
                    Ok(Some(IntervalSignal::AEvent(msg))) => {
                        debug!("recv AEvent value={}", msg.value);
                    }
                    Ok(Some(IntervalSignal::Close(_))) => {
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
            self.identity.dispatch_with_key("Close", Close).await.unwrap();
        });
    }
}
