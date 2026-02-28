use crate::bus::sub_bus::{EntryOfSubBus, SubBus};
use crate::worker::identity::{
    IdentityCommon, IdentityOfInterval, IdentityOfRx, IdentityOfSimple, Merge,
};
use crate::worker::{CopyOfWorker, SubscribeKey, ToWorker, WorkerId};
use crate::{Event, IdentityOfMerge};
use log::{debug, error};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::sleep;

mod sub_bus;

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
    // SimpleLogin(oneshot::Sender<IdentityCommon>, String),
    /// Worker 订阅某个路由键（TypeId 或 key + TypeId）。
    Subscribe(WorkerId, RouteKey, &'static str),
    /// Worker 发送事件，Bus 按路由键分发。
    DispatchEvent(WorkerId, RouteKey, BusEvent),
    /// Worker 下线，Bus 负责移除并清理订阅关系。
    Drop(WorkerId),
    /// 定时输出订阅关系快照。
    Trace,
    /// 清空关联关系（worker/subbus），但 bus 保持运行。
    Cleanup(oneshot::Sender<()>),
    /// 优雅关闭：清理 worker/subbus 后回执。
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RouteKey {
    Type(TypeId),
    TypeWithKey(TypeId, String),
}

#[derive(Clone)]
pub struct EntryOfBus {
    tx: UnboundedSender<BusData>,
}

impl EntryOfBus {
    pub async fn login_with_name(&self, name: impl Into<String>) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name.into(), false))?;
        Ok(rx.await?.into())
    }

    pub async fn login<W: ToWorker>(&self) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name(), false))?;
        Ok(rx.await?.into())
    }

    pub async fn persistent_login_with_name(&self, name: impl Into<String>) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name.into(), true))?;
        Ok(rx.await?.into())
    }

    pub async fn persistent_login<W: ToWorker>(&self) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name(), true))?;
        Ok(rx.await?.into())
    }

    pub async fn interval_login<W: ToWorker>(
        &self,
        duration: Duration,
    ) -> Result<IdentityOfInterval, BusError> {
        let id = self.login::<W>().await?;
        Ok(id.with_interval(duration))
    }

    pub async fn interval_login_with_name(
        &self,
        name: impl Into<String>,
        duration: Duration,
    ) -> Result<IdentityOfInterval, BusError> {
        let id = self.login_with_name(name.into()).await?;
        Ok(id.with_interval(duration))
    }

    pub async fn simple_login<W: ToWorker, T: Event>(
        &self,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name(), false))?;
        let rx: IdentityOfSimple<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    pub async fn simple_login_with_name<T: Event>(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name.into(), false))?;
        let rx: IdentityOfSimple<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    pub async fn merge_login<W: ToWorker, T: Event + Merge>(
        &self,
    ) -> Result<IdentityOfMerge<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name(), false))?;
        let rx: IdentityOfMerge<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    pub async fn merge_login_with_name<T: Event + Merge>(
        &self,
        name: impl Into<String>,
    ) -> Result<IdentityOfMerge<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name.into(), false))?;
        let rx: IdentityOfMerge<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    /// 优雅关闭总线：
    /// 1) 停止所有 subbus 循环
    /// 2) 释放 worker 发送端，关闭 worker 事件通道
    /// 3) 停止 bus 控制循环
    pub async fn shutdown(&self) -> Result<(), BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Shutdown(tx))?;
        rx.await?;
        Ok(())
    }

    /// 清空 bus 内部关联关系但不中断总线：
    /// 1) 停止所有 subbus 循环
    /// 2) 释放现存 worker 发送端（旧 worker 事件通道会关闭）
    /// 3) bus 继续接受新登录/新订阅
    pub async fn cleanup(&self) -> Result<(), BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Cleanup(tx))?;
        rx.await?;
        Ok(())
    }
}

pub struct Bus<const CAP: usize> {
    rx: UnboundedReceiver<BusData>,
    tx: UnboundedSender<BusData>,
    workers: HashMap<WorkerId, CopyOfWorker>,
    sub_buses: HashMap<RouteKey, EntryOfSubBus>,
}

impl<const CAP: usize> Drop for Bus<CAP> {
    fn drop(&mut self) {
        debug!("bus drop");
    }
}

impl<const CAP: usize> Bus<CAP> {
    pub fn init() -> EntryOfBus {
        let (tx, rx) = unbounded_channel();
        Self {
            rx,
            tx: tx.clone(),
            workers: Default::default(),
            sub_buses: Default::default(),
        }
        .run();
        EntryOfBus { tx }
    }

    /// Bus 的控制循环：
    /// 1) 处理登录和订阅关系维护
    /// 2) 处理事件路由（按 TypeId -> SubBus）
    /// 3) 处理 worker 下线后的清理
    /// 4) 定时触发 trace 输出
    fn run(mut self) {
        spawn(async move {
            let tx = self.tx.clone();
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
                    BusData::Login(tx, name, persistent) => {
                        // 为每个 worker 创建一个独立接收通道（rx_event），
                        // 并将身份对象回传给登录方。
                        let (identity_rx, copy_of_worker) = self.init_worker(name, persistent);
                        self.workers.insert(copy_of_worker.id(), copy_of_worker);
                        if tx.send(identity_rx).is_err() {
                            error!("login fail: tx ack fail");
                        }
                    }
                    BusData::Drop(worker_id) => {
                        self.remove_worker(&worker_id).await;
                    }
                    BusData::DispatchEvent(worker_id, route_key, event) => {
                        if let Some(sub_buses) = self.sub_buses.get(&route_key) {
                            debug!("{} dispatch {}", worker_id, sub_buses.name());
                            sub_buses.send_event(event).await;
                        } else {
                            debug!(
                                "{} dispatch route_key {:?} that no one subscribe",
                                worker_id, route_key
                            );
                        }
                    }
                    BusData::Subscribe(worker_id, route_key, name) => {
                        debug!("{} subscribe {}", worker_id, name);
                        if let Some(worker) = self.workers.get_mut(&worker_id) {
                            match &route_key {
                                RouteKey::Type(typeid) => worker.subscribe_event(*typeid),
                                RouteKey::TypeWithKey(typeid, key) => {
                                    worker.subscribe_event_with_key(*typeid, key.clone())
                                }
                            }
                            if let Some(sub_buses) = self.sub_buses.get_mut(&route_key) {
                                sub_buses.send_subscribe(worker.init_subscriber()).await;
                            } else {
                                let typeid = match route_key {
                                    RouteKey::Type(typeid) | RouteKey::TypeWithKey(typeid, _) => {
                                        typeid
                                    }
                                };
                                let mut copy = SubBus::<CAP>::init(typeid, name);
                                copy.send_subscribe(worker.init_subscriber()).await;
                                self.sub_buses.insert(route_key, copy);
                            }
                        }
                    }
                    BusData::Trace => {
                        for (_, sub_bus) in self.sub_buses.iter() {
                            sub_bus.send_trace().await;
                        }
                    }
                    BusData::Cleanup(tx) => {
                        self.cleanup_associations().await;
                        let _ = tx.send(());
                    }
                    BusData::Shutdown(tx) => {
                        self.cleanup_associations().await;
                        let _ = tx.send(());
                        break;
                    }
                }
            }
        });
    }

    async fn cleanup_associations(&mut self) {
        let transient_workers: Vec<WorkerId> = self
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                if worker.persistent() {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect();

        for worker_id in transient_workers {
            self.remove_worker(&worker_id).await;
        }
    }

    // fn init_worker(&self) -> (IdentityOfRx, CopyOfWorker) {
    //     let (tx_event, rx_event) = unbounded_channel();
    //     let id = WorkerId::default();
    //     (
    //         IdentityOfRx::init(id, rx_event, self.tx.clone()),
    //         CopyOfWorker::init(id, tx_event),
    //     )
    // }
    fn init_worker(&self, name: String, persistent: bool) -> (IdentityCommon, CopyOfWorker) {
        // 每个 worker 拥有自己的事件队列，便于独立消费与背压控制。
        let (tx_event, rx_event) = channel(CAP);
        let id = WorkerId::init(name);
        (
            IdentityCommon {
                id: id.clone(),
                rx_event,
                tx_data: self.tx.clone(),
            },
            CopyOfWorker::init(id, tx_event, persistent),
        )
    }

    async fn remove_worker(&mut self, worker_id: &WorkerId) {
        debug!("{} Drop", worker_id);
        if let Some(worker) = self.workers.remove(worker_id) {
            for subscribe_key in worker.subscribe_keys() {
                let route_key = match subscribe_key {
                    SubscribeKey::Type(ty_id) => RouteKey::Type(*ty_id),
                    SubscribeKey::TypeWithKey(ty_id, key) => {
                        RouteKey::TypeWithKey(*ty_id, key.clone())
                    }
                };
                let should_remove = if let Some(sub_bus) = self.sub_buses.get_mut(&route_key) {
                    sub_bus.send_unsubscribe(worker_id.clone()).await == 0
                } else {
                    false
                };
                if should_remove {
                    if let Some(sub_bus) = self.sub_buses.remove(&route_key) {
                        // 该路由键已无订阅者，释放对应 SubBus。
                        sub_bus.send_drop().await;
                    }
                }
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

        let err = match worker.recv_event().await {
            Ok(_) => panic!("expected channel closed error"),
            Err(err) => err,
        };
        match err {
            BusError::ChannelClosed { stage, worker } => {
                assert_eq!(stage, "worker_recv_event");
                assert_eq!(worker.as_deref(), Some("shutdown-worker"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
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

        let err = match old_worker.recv_event().await {
            Ok(_) => panic!("expected old worker channel closed"),
            Err(err) => err,
        };
        match err {
            BusError::ChannelClosed { stage, worker } => {
                assert_eq!(stage, "worker_recv_event");
                assert_eq!(worker.as_deref(), Some("old-worker"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let mut new_worker = bus.login_with_name("new-worker".to_string()).await.unwrap();
        new_worker.subscribe::<CleanupEvent>().await.unwrap();
        let dispatcher = bus.login_with_name("dispatcher".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        dispatcher.dispatch_event(CleanupEvent).await.unwrap();
        let recv_result = timeout(Duration::from_millis(200), new_worker.recv_event()).await;
        let event = match recv_result {
            Ok(Ok(event)) => event,
            Ok(Err(err)) => panic!("unexpected bus error after cleanup: {err}"),
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
            .unwrap();
        let second = timeout(Duration::from_millis(200), persistent.recv_event())
            .await
            .unwrap()
            .unwrap();
        let got = [first.type_name(), second.type_name()];
        assert!(got.contains(&"CleanupEvent"));
        assert!(got.contains(&"ShutdownEvent"));
    }
}
