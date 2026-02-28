use for_event_bus::{BusError, Event, IdentityOfRx, Merge, ToWorker, Worker};
use for_event_bus::{EntryOfBus, IdentityOfMerge, SimpleBus};
use log::debug;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;

const ORDER_KEY_1: &str = "order:1001";
const ORDER_KEY_2: &str = "order:1002";

#[tokio::main]
async fn main() {
    custom_utils::logger::logger_stdout_debug();
    let bus = SimpleBus::init();

    WorkerA::init(&bus, ORDER_KEY_1).await;
    WorkerDispatcher::init(&bus).await;

    sleep(Duration::from_secs(5)).await;
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

impl WorkerA {
    pub async fn init(bus: &EntryOfBus, key: &str) {
        let identity = bus.merge_login::<WorkerA, MergeEvent>().await.unwrap();
        // 增加 keyed 订阅：只有同 key 的事件才会进入这个 worker。
        identity.subscribe_with_key(key).await.unwrap();
        Self { identity }.run();
    }

    fn run(mut self) {
        spawn(async move {
            while let Ok(event) = self.identity.recv().await {
                debug!("recv keyed merge event: {:?}", event);
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
            self.identity
                .dispatch_with_key(ORDER_KEY_1, AEvent)
                .await
                .unwrap();
            self.identity.dispatch_event(AEvent).await.unwrap();
            self.identity
                .dispatch_with_key(ORDER_KEY_2, AEvent)
                .await
                .unwrap();
            self.identity
                .dispatch_with_key(ORDER_KEY_1, Close)
                .await
                .unwrap();
        });
    }
}
