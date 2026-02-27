use crate::bus::sub_bus::{EntryOfSubBus, SubBus};
use crate::worker::identity::{IdentityCommon, IdentityOfRx, IdentityOfSimple, Merge};
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
    Login(oneshot::Sender<IdentityCommon>, String),
    // SimpleLogin(oneshot::Sender<IdentityCommon>, String),
    /// Worker 订阅某个路由键（TypeId 或 key + TypeId）。
    Subscribe(WorkerId, RouteKey, &'static str),
    /// Worker 发送事件，Bus 按路由键分发。
    DispatchEvent(WorkerId, RouteKey, BusEvent),
    /// Worker 下线，Bus 负责移除并清理订阅关系。
    Drop(WorkerId),
    /// 定时输出订阅关系快照。
    Trace,
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
    pub async fn login_with_name(&self, name: String) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name))?;
        Ok(rx.await?.into())
    }

    pub async fn login<W: ToWorker>(&self) -> Result<IdentityOfRx, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name()))?;
        Ok(rx.await?.into())
    }
    pub async fn simple_login<W: ToWorker, T: Event>(
        &self,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name()))?;
        let rx: IdentityOfSimple<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    pub async fn simple_login_with_name<T: Event>(
        &self,
        name: String,
    ) -> Result<IdentityOfSimple<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name))?;
        let rx: IdentityOfSimple<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    pub async fn merge_login<W: ToWorker, T: Event + Merge>(
        &self,
    ) -> Result<IdentityOfMerge<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, W::name()))?;
        let rx: IdentityOfMerge<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
    }

    pub async fn merge_login_with_name<T: Event + Merge>(
        &self,
        name: String,
    ) -> Result<IdentityOfMerge<T>, BusError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BusData::Login(tx, name))?;
        let rx: IdentityOfMerge<T> = rx.await?.into();
        rx.subscribe().await?;
        Ok(rx)
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
                    BusData::Login(tx, name) => {
                        // 为每个 worker 创建一个独立接收通道（rx_event），
                        // 并将身份对象回传给登录方。
                        let (identity_rx, copy_of_worker) = self.init_worker(name);
                        self.workers.insert(copy_of_worker.id(), copy_of_worker);
                        if tx.send(identity_rx).is_err() {
                            error!("login fail: tx ack fail");
                        }
                    }
                    BusData::Drop(worker_id) => {
                        debug!("{} Drop", worker_id);
                        if let Some(worker) = self.workers.remove(&worker_id) {
                            for subscribe_key in worker.subscribe_keys() {
                                let route_key = match subscribe_key {
                                    SubscribeKey::Type(ty_id) => RouteKey::Type(*ty_id),
                                    SubscribeKey::TypeWithKey(ty_id, key) => {
                                        RouteKey::TypeWithKey(*ty_id, key.clone())
                                    }
                                };
                                let should_remove = if let Some(sub_bus) =
                                    self.sub_buses.get_mut(&route_key)
                                {
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
                        } else {
                            // todo
                        }
                    }
                    BusData::DispatchEvent(worker_id, route_key, event) => {
                        if let Some(sub_buses) = self.sub_buses.get(&route_key) {
                            debug!("{} dispatch {}", worker_id, sub_buses.name());
                            sub_buses.send_event(event).await;
                        } else {
                            debug!(
                                "{} dispatch type_id {:?} that no one subscribe",
                                worker_id,
                                event.type_id()
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
                }
            }
        });
    }

    // fn init_worker(&self) -> (IdentityOfRx, CopyOfWorker) {
    //     let (tx_event, rx_event) = unbounded_channel();
    //     let id = WorkerId::default();
    //     (
    //         IdentityOfRx::init(id, rx_event, self.tx.clone()),
    //         CopyOfWorker::init(id, tx_event),
    //     )
    // }
    fn init_worker(&self, name: String) -> (IdentityCommon, CopyOfWorker) {
        // 每个 worker 拥有自己的事件队列，便于独立消费与背压控制。
        let (tx_event, rx_event) = channel(CAP);
        let id = WorkerId::init(name);
        (
            IdentityCommon {
                id: id.clone(),
                rx_event,
                tx_data: self.tx.clone(),
            },
            CopyOfWorker::init(id, tx_event),
        )
    }
}
