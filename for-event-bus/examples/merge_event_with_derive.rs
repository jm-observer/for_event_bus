use for_event_bus::{BusError, Event, IdentityOfRx, Merge, ToWorker, Worker};
use for_event_bus::{EntryOfBus, IdentityOfMerge, SimpleBus};
use log::debug;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // log
    custom_utils::logger::logger_stdout_debug();
    // init event bus
    let copy_of_bus = SimpleBus::init();
    // init worker and subscribe event
    {
        WorkerA::init(&copy_of_bus).await;
        // init worker and dispatcher event
        WorkerDispatcher::init(&copy_of_bus).await;
        sleep(Duration::from_secs(5)).await
    }
    sleep(Duration::from_secs(5)).await
}

#[derive(Debug, Clone, Event)]
struct AEvent;
#[derive(Debug, Clone, Event)]
struct Close;

#[derive(Debug, Clone, Merge, Event)]
enum MergeEvent {
    AEvent(AEvent),
    Close(Close),
}

#[derive(Worker)]
struct WorkerA {
    identity: IdentityOfMerge<MergeEvent>,
}

// impl ToWorker for Worker {
//     fn name() -> String {
//         "Worker".to_string()
//     }
// }

impl WorkerA {
    pub async fn init(bus: &EntryOfBus) {
        let identity = bus.merge_login::<WorkerA, MergeEvent>().await.unwrap();
        Self { identity }.run();
    }
    fn run(mut self) {
        spawn(async move {
            while let Ok(event) = self.identity.recv().await {
                debug!("{:?}", event);
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
        let identity = bus.login::<WorkerDispatcher>().await.unwrap();
        Self { identity }.run();
    }
    fn run(self) {
        spawn(async move {
            self.identity.dispatch_event(AEvent).await.unwrap();
            self.identity.dispatch_event(Close).await.unwrap();
        });
    }
}
