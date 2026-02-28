use for_event_bus::{upcast, Event, IdentityOfRx};
use for_event_bus::{BusError, Merge, ToWorker};
use for_event_bus::{EntryOfBus, SimpleBus};
use log::debug;
use std::any::TypeId;
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
        Worker::init(&copy_of_bus).await;
        // init worker and dispatcher event
        WorkerDispatcher::init(&copy_of_bus).await;
        sleep(Duration::from_secs(5)).await
    }
    sleep(Duration::from_secs(5000)).await
}

#[derive(Debug, Clone, Event)]
struct AEvent;
#[derive(Debug, Clone, Event)]
struct Close;

#[derive(Debug, Clone, Event)]
enum MergeEvent {
    AEvent(AEvent),
    Close(Close),
}

impl Merge for MergeEvent {
    fn merge(event: for_event_bus::BusEvent) -> Result<Self, BusError>
    where
        Self: Sized,
    {
        let event = upcast(event);
        if let Ok(a_event) = event.clone().downcast::<AEvent>() {
            Ok(Self::AEvent(a_event.as_ref().clone()))
        } else if let Ok(a_event) = event.clone().downcast::<Close>() {
            Ok(Self::Close(a_event.as_ref().clone()))
        } else {
            Err(BusError::DowncastErr)
        }
    }

    fn subscribe_types() -> Vec<(TypeId, &'static str)> {
        vec![
            (TypeId::of::<AEvent>(), AEvent::name()),
            (TypeId::of::<Close>(), Close::name()),
        ]
    }
}

struct Worker {
    identity: IdentityOfRx,
}

impl ToWorker for Worker {
    fn name() -> String {
        "Worker".to_string()
    }
}

impl Worker {
    pub async fn init(bus: &EntryOfBus) {
        let identity = bus
            .login_and_subscribe_merge::<Worker, MergeEvent>()
            .await
            .unwrap();
        Self { identity }.run();
    }
    fn run(mut self) {
        spawn(async move {
            while let Ok(event) = self.identity.recv_merge::<MergeEvent>().await {
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
        let identity = bus.login::<Self>().await.unwrap();
        Self { identity }.run();
    }
    fn run(self) {
        spawn(async move {
            self.identity.dispatch_event(AEvent).await.unwrap();
            self.identity.dispatch_event(Close).await.unwrap();
        });
    }
}
