use for_event_bus::SimpleBus;
use for_event_bus::{
    EntryOfBus, Event, IdentityOfInterval, IdentityOfRx, IdentitySignal, ToWorker,
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

#[derive(Debug, Event)]
struct AEvent {
    value: usize,
}

#[derive(Debug, Event)]
struct Close;

struct IntervalWorker {
    identity: IdentityOfInterval,
}

impl ToWorker for IntervalWorker {
    fn name() -> String {
        "IntervalWorker".to_string()
    }
}

impl IntervalWorker {
    pub async fn init(bus: &EntryOfBus) {
        let identity = bus
            .interval_login::<Self>(Duration::from_secs(1))
            .await
            .unwrap();
        identity.subscribe::<AEvent>().await.unwrap();
        identity.subscribe::<Close>().await.unwrap();
        Self { identity }.run();
    }

    fn run(mut self) {
        spawn(async move {
            loop {
                match self.identity.recv_signal().await {
                    Ok(IdentitySignal::Tick) => {
                        debug!("tick: do periodic job");
                    }
                    Ok(IdentitySignal::Event(event)) => {
                        let any = event.as_any();
                        if let Ok(msg) = any.clone().downcast::<AEvent>() {
                            debug!("recv AEvent value={}", msg.value);
                        } else if any.downcast::<Close>().is_ok() {
                            debug!("recv Close, worker exit");
                            break;
                        }
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
