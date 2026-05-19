use crate::worker::identity::{
    FromTick, IdentityCommon, IdentityOfMerge, IdentityOfMergeTick, IdentityOfRx, IdentityOfSimple,
    Merge,
};
use crate::worker::{ToWorker, WorkerId};
use crate::Event;
use log::{debug, error};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::spawn;
use tokio::sync::broadcast;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, watch};
use tokio::time::sleep;

/// 按路由键分发的 broadcast 注册表。数据面（publish/subscribe）直接读写它，
/// 不经过控制 task，因此没有中心串行点。写锁只在首次订阅某路由键时取一次。
pub(crate) type Registry = Arc<RwLock<HashMap<RouteKey, broadcast::Sender<BusEvent>>>>;

#[derive(Clone)]
pub struct BusEvent {
    type_id: TypeId,
    type_name: &'static str,
    payload: Arc<dyn Any + Send + Sync + 'static>,
}

impl BusEvent {
    pub fn new<T: Event>(event: T) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: T::name(),
            payload: Arc::new(event),
        }
    }

    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn as_any(&self) -> Arc<dyn Any + Send + Sync + 'static> {
        self.payload.clone()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BusError {
    /// legacy: kept for compatibility with older external code.
    ChannelErr,
    /// legacy: kept for compatibility with older external code.
    DowncastErr,
    ChannelClosed {
        stage: &'static str,
        worker: Option<String>,
    },
    DowncastFailed {
        expected: &'static str,
        actual: &'static str,
    },
}

impl BusError {
    pub fn channel_closed(stage: &'static str, worker: Option<&str>) -> Self {
        Self::ChannelClosed {
            stage,
            worker: worker.map(ToOwned::to_owned),
        }
    }

    pub fn downcast_failed(expected: &'static str, actual: &'static str) -> Self {
        Self::DowncastFailed { expected, actual }
    }
}

impl Display for BusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BusError::ChannelErr => write!(f, "channel error"),
            BusError::DowncastErr => write!(f, "downcast error"),
            BusError::ChannelClosed { stage, worker } => {
                if let Some(worker) = worker {
                    write!(f, "channel closed at {stage}, worker={worker}")
                } else {
                    write!(f, "channel closed at {stage}")
                }
            }
            BusError::DowncastFailed { expected, actual } => {
                write!(f, "downcast failed: expected={expected}, actual={actual}")
            }
        }
    }
}

impl std::error::Error for BusError {}

impl From<oneshot::error::RecvError> for BusError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::channel_closed("oneshot_recv", None)
    }
}

impl<T> From<SendError<T>> for BusError {
    fn from(_: SendError<T>) -> Self {
        Self::channel_closed("bus_data_send", None)
    }
}

pub(crate) enum BusData {
    /// Worker 登录，请求创建身份对象并返回给调用方。
    Login(oneshot::Sender<IdentityCommon>, String, bool),
    /// Worker 订阅某个路由键（仅控制面记账，供 trace/cleanup 使用）。
    Subscribe(WorkerId, RouteKey, &'static str),
    /// Worker 下线，移除记账。
    Drop(WorkerId),
    /// 定时输出订阅关系快照。
    Trace,
    /// 清空瞬时 worker（kill），但 bus 保持运行。
    Cleanup(oneshot::Sender<()>),
    /// 优雅关闭：kill 所有 worker 后回执。
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RouteKey {
    Type(TypeId),
    TypeWithKey(TypeId, String),
}

/// 把 RouteKey 对应的 broadcast Sender 取出（不存在则返回 None）。dispatch 走这条只读路径。
pub(crate) fn lookup_sender(
    registry: &Registry,
    route_key: &RouteKey,
) -> Option<broadcast::Sender<BusEvent>> {
    registry.read().unwrap().get(route_key).cloned()
}

/// 取出或创建 RouteKey 对应的 broadcast Sender，并返回一个新的订阅 Receiver。subscribe 走这条路径。
pub(crate) fn subscribe_sender(
    registry: &Registry,
    route_key: RouteKey,
    cap: usize,
) -> broadcast::Receiver<BusEvent> {
    let mut map = registry.write().unwrap();
    let sender = map
        .entry(route_key)
        .or_insert_with(|| broadcast::channel(cap.max(1)).0);
    sender.subscribe()
}

#[derive(Clone)]
pub struct EntryOfBus {
    tx: UnboundedSender<BusData>,
}

impl EntryOfBus {
    async fn raw_login(&self, name: String, persistent: bool) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name, persistent))?;
        Ok(rx.await?.into())
    }

    pub async fn login_with_name(&self, name: impl Into<String>) -> Result<IdentityOfRx, BusError> {
        self.raw_login(name.into(), false).await
    }

    pub async fn login<W: ToWorker>(&self) -> Result<IdentityOfRx, BusError> {
        self.raw_login(W::name(), false).await
    }

    pub async fn persistent_login_with_name(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfRx, BusError> {
        self.raw_login(name.into(), true).await
    }

    pub async fn persistent_login<W: ToWorker>(&self) -> Result<IdentityOfRx, BusError> {
        self.raw_login(W::name(), true).await
    }

    pub async fn login_and_subscribe<W: ToWorker, T: Event + 'static>(
        &self,
    ) -> Result<IdentityOfRx, BusError> {
        let id = self.login::<W>().await?;
        id.subscribe::<T>().await?;
        Ok(id)
    }

    pub async fn login_and_subscribe_with_name<T: Event + 'static>(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfRx, BusError> {
        let id = self.login_with_name(name).await?;
        id.subscribe::<T>().await?;
        Ok(id)
    }

    pub async fn login_and_subscribe_merge<W: ToWorker, T: Event + Merge>(
        &self,
    ) -> Result<IdentityOfRx, BusError> {
        let id = self.login::<W>().await?;
        id.subscribe_merge::<T>().await?;
        Ok(id)
    }

    pub async fn simple_login<W: ToWorker, T: Event + 'static>(
        &self,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let id = self.login::<W>().await?;
        id.subscribe::<T>().await?;
        Ok(id.into_simple())
    }

    pub async fn simple_login_with_name<T: Event + 'static>(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let id = self.login_with_name(name).await?;
        id.subscribe::<T>().await?;
        Ok(id.into_simple())
    }

    pub async fn simple_login_with_name_and_key<T: Event + 'static>(
        &self,
        key: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let id = self.login_with_name(name).await?;
        id.subscribe_with_key::<T>(key).await?;
        Ok(id.into_simple())
    }

    pub async fn merge_login<W: ToWorker, T: Event + Merge>(
        &self,
    ) -> Result<IdentityOfMerge<T>, BusError> {
        let id = self.login::<W>().await?;
        id.subscribe_merge::<T>().await?;
        Ok(id.into_merge())
    }

    pub async fn merge_login_with_name<T: Event + Merge>(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfMerge<T>, BusError> {
        let id = self.login_with_name(name).await?;
        id.subscribe_merge::<T>().await?;
        Ok(id.into_merge())
    }

    pub async fn merge_tick_login<W: ToWorker, T: Merge + FromTick>(
        &self,
        duration: Duration,
    ) -> Result<IdentityOfMergeTick<T>, BusError> {
        let id = self.login::<W>().await?;
        id.subscribe_merge::<T>().await?;
        Ok(id.into_merge_tick(duration))
    }

    pub async fn merge_tick_login_with_name<T: Merge + FromTick>(
        &self,
        name: impl Into<String>,
        duration: Duration,
    ) -> Result<IdentityOfMergeTick<T>, BusError> {
        let id = self.login_with_name(name).await?;
        id.subscribe_merge::<T>().await?;
        Ok(id.into_merge_tick(duration))
    }

    pub async fn login_and_subscribe_merge_with_name<T: Merge>(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfRx, BusError> {
        let id = self.login_with_name(name).await?;
        id.subscribe_merge::<T>().await?;
        Ok(id)
    }

    /// 优雅关闭总线：kill 所有 worker，停止控制循环。
    pub async fn shutdown(&self) -> Result<(), BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Shutdown(tx))?;
        rx.await?;
        Ok(())
    }

    /// 清空瞬时 worker（kill）但不中断总线；persistent worker 与已注册路由保留。
    pub async fn cleanup(&self) -> Result<(), BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Cleanup(tx))?;
        rx.await?;
        Ok(())
    }
}

struct WorkerCtl {
    name: Arc<String>,
    persistent: bool,
    kill: watch::Sender<bool>,
    subs: HashSet<RouteKey>,
}

pub struct Bus<const CAP: usize> {
    rx: UnboundedReceiver<BusData>,
    tx: UnboundedSender<BusData>,
    workers: HashMap<WorkerId, WorkerCtl>,
    registry: Registry,
}

impl<const CAP: usize> Drop for Bus<CAP> {
    fn drop(&mut self) {
        debug!("bus drop");
    }
}

impl<const CAP: usize> Bus<CAP> {
    pub fn init() -> EntryOfBus {
        let (tx, rx) = unbounded_channel();
        let registry: Registry = Default::default();
        let entry = EntryOfBus { tx: tx.clone() };
        Self {
            rx,
            tx: tx.clone(),
            workers: Default::default(),
            registry,
        }
        .run(tx);
        entry
    }

    /// 控制循环：只处理登录、订阅记账、下线、cleanup/shutdown、trace。
    /// 事件分发（publish/recv）不经过这里。
    fn run(mut self, tx: UnboundedSender<BusData>) {
        spawn(async move {
            spawn(async move {
                let time = Duration::from_secs(30);
                loop {
                    sleep(time).await;
                    if tx.send(BusData::Trace).is_err() {
                        return;
                    }
                }
            });
            while let Some(event) = self.rx.recv().await {
                match event {
                    BusData::Login(ack, name, persistent) => {
                        let (kill_tx, kill_rx) = watch::channel(false);
                        let (inbox_tx, rx_event) = channel(CAP);
                        let id = WorkerId::init(name);
                        let ctl = WorkerCtl {
                            name: id.name_arc(),
                            persistent,
                            kill: kill_tx,
                            subs: HashSet::new(),
                        };
                        self.workers.insert(id.clone(), ctl);
                        let identity = IdentityCommon {
                            id,
                            rx_event,
                            inbox_tx,
                            tx_data: self.tx_for_identity(),
                            registry: self.registry.clone(),
                            kill_rx,
                            cap: CAP,
                        };
                        if ack.send(identity).is_err() {
                            error!("login fail: ack send fail");
                        }
                    }
                    BusData::Subscribe(worker_id, route_key, name) => {
                        debug!("{} subscribe {route_key:?} {}", worker_id, name);
                        if let Some(worker) = self.workers.get_mut(&worker_id) {
                            worker.subs.insert(route_key);
                        }
                    }
                    BusData::Drop(worker_id) => {
                        debug!("{} Drop", worker_id);
                        self.workers.remove(&worker_id);
                    }
                    BusData::Trace => {
                        let mut by_route: HashMap<&RouteKey, Vec<&str>> = HashMap::new();
                        for (_, ctl) in self.workers.iter() {
                            for rk in ctl.subs.iter() {
                                by_route.entry(rk).or_default().push(ctl.name.as_str());
                            }
                        }
                        for (rk, names) in by_route {
                            debug!("subscriber of {rk:?}: {}", names.join(","));
                        }
                    }
                    BusData::Cleanup(ack) => {
                        self.kill_transient();
                        let _ = ack.send(());
                    }
                    BusData::Shutdown(ack) => {
                        for (_, ctl) in self.workers.drain() {
                            let _ = ctl.kill.send(true);
                        }
                        self.registry.write().unwrap().clear();
                        let _ = ack.send(());
                        break;
                    }
                }
            }
        });
    }

    fn tx_for_identity(&self) -> UnboundedSender<BusData> {
        // IdentityOfRx 仅用它做下线记账与订阅记账。
        self.tx.clone()
    }

    fn kill_transient(&mut self) {
        let dead: Vec<WorkerId> = self
            .workers
            .iter()
            .filter_map(|(id, ctl)| {
                if ctl.persistent {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect();
        for id in dead {
            if let Some(ctl) = self.workers.remove(&id) {
                let _ = ctl.kill.send(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[derive(Debug)]
    struct ShutdownEvent;

    impl Event for ShutdownEvent {
        fn name() -> &'static str {
            "ShutdownEvent"
        }
    }

    #[tokio::test]
    async fn shutdown_closes_worker_channels() {
        let bus = Bus::<8>::init();
        let mut worker = bus
            .login_with_name("shutdown-worker".to_string())
            .await
            .unwrap();

        worker.subscribe::<ShutdownEvent>().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        bus.shutdown().await.unwrap();

        let event = worker.recv_event().await;
        assert!(event.is_none(), "expected channel closed");
    }

    #[derive(Debug)]
    struct CleanupEvent;

    impl Event for CleanupEvent {
        fn name() -> &'static str {
            "CleanupEvent"
        }
    }

    #[tokio::test]
    async fn cleanup_drops_old_workers_but_bus_stays_usable() {
        let bus = Bus::<8>::init();
        let mut old_worker = bus.login_with_name("old-worker".to_string()).await.unwrap();
        old_worker.subscribe::<CleanupEvent>().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        bus.cleanup().await.unwrap();

        let event = old_worker.recv_event().await;
        assert!(event.is_none(), "expected old worker channel closed");

        let mut new_worker = bus.login_with_name("new-worker".to_string()).await.unwrap();
        new_worker.subscribe::<CleanupEvent>().await.unwrap();
        let dispatcher = bus.login_with_name("dispatcher".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        dispatcher.dispatch_event(CleanupEvent).await.unwrap();
        let recv_result = timeout(Duration::from_millis(200), new_worker.recv_event()).await;
        let event = match recv_result {
            Ok(Some(event)) => event,
            Ok(None) => panic!("unexpected closed channel after cleanup"),
            Err(_) => panic!("new worker did not receive event after cleanup"),
        };
        assert_eq!(event.type_name(), "CleanupEvent");
    }

    #[tokio::test]
    async fn persistent_worker_survives_cleanup() {
        let bus = Bus::<8>::init();
        let mut persistent = bus
            .persistent_login_with_name("persistent-worker".to_string())
            .await
            .unwrap();
        persistent.subscribe::<CleanupEvent>().await.unwrap();

        bus.cleanup().await.unwrap();

        // cleanup 之后依然可以继续新增订阅
        persistent.subscribe::<ShutdownEvent>().await.unwrap();

        let dispatcher = bus
            .login_with_name("dispatcher-2".to_string())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        dispatcher.dispatch_event(CleanupEvent).await.unwrap();
        dispatcher.dispatch_event(ShutdownEvent).await.unwrap();

        let first = timeout(Duration::from_millis(200), persistent.recv_event())
            .await
            .unwrap()
            .expect("persistent worker channel closed unexpectedly");
        let second = timeout(Duration::from_millis(200), persistent.recv_event())
            .await
            .unwrap()
            .expect("persistent worker channel closed unexpectedly");
        let got = [first.type_name(), second.type_name()];
        assert!(got.contains(&"CleanupEvent"));
        assert!(got.contains(&"ShutdownEvent"));
    }
}
