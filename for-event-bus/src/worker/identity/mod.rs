use crate::bus::{
    lookup_sender, subscribe_sender, BusData, BusError, BusEvent, Registry, RouteKey,
};
use crate::worker::WorkerId;
use log::{debug, warn};
use std::any::TypeId;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::broadcast;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::sync::watch;

mod interval;
mod merge;
mod merge_tick;
mod simple;

use crate::Event;
pub use interval::FromTick;
pub use merge::{IdentityOfMerge, Merge, MergeContains, MergeSkip};
pub use merge_tick::IdentityOfMergeTick;
pub use simple::IdentityOfSimple;

/// 把某个路由键的 broadcast 接收端泵入 worker 自己的 mpsc inbox。
/// inbox 满时 `send().await` 提供背压：泵停止消费 broadcast，
/// 该接收端被 broadcast 标记为 Lagged 并丢最旧——只影响这一个慢消费者，
/// 自愈、不会无限刷日志，无需手动剔除。
fn spawn_forwarder(
    mut brx: broadcast::Receiver<BusEvent>,
    inbox: Sender<BusEvent>,
    mut kill: watch::Receiver<bool>,
) {
    spawn(async move {
        if *kill.borrow() {
            return;
        }
        loop {
            tokio::select! {
                res = kill.changed() => {
                    if res.is_err() || *kill.borrow() {
                        break;
                    }
                }
                ev = brx.recv() => match ev {
                    Ok(ev) => {
                        if inbox.send(ev).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("subscriber lagged, dropped {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    });
}

#[derive(Clone)]
pub struct IdentityOfTx {
    id: WorkerId,
    registry: Registry,
}

impl IdentityOfTx {
    fn publish(&self, route_key: RouteKey, event: BusEvent) {
        match lookup_sender(&self.registry, &route_key) {
            Some(sender) => {
                // Err 仅代表当前无接收者，语义等同“无人订阅”，丢弃即可。
                let _ = sender.send(event);
            }
            None => {
                debug!("{} dispatch {route_key:?} that no one subscribe", self.id);
            }
        }
    }

    pub async fn dispatch_event<T: Event>(&self, event: T) -> Result<(), BusError> {
        self.publish(RouteKey::Type(TypeId::of::<T>()), BusEvent::new(event));
        Ok(())
    }

    pub async fn dispatch_with_key<T: Event>(
        &self,
        key: impl Into<String>,
        event: T,
    ) -> Result<(), BusError> {
        self.publish(
            RouteKey::TypeWithKey(TypeId::of::<T>(), key.into()),
            BusEvent::new(event),
        );
        Ok(())
    }
}

/// 通用的 worker 身份标识，可以订阅多种事件。
pub struct IdentityOfRx {
    pub id: WorkerId,
    pub(crate) rx_event: Receiver<BusEvent>,
    pub(crate) inbox_tx: Sender<BusEvent>,
    pub(crate) tx_data: UnboundedSender<BusData>,
    pub(crate) registry: Registry,
    pub(crate) kill_rx: watch::Receiver<bool>,
    pub(crate) cap: usize,
}

pub struct IdentityCommon {
    pub(crate) id: WorkerId,
    pub(crate) rx_event: Receiver<BusEvent>,
    pub(crate) inbox_tx: Sender<BusEvent>,
    pub(crate) tx_data: UnboundedSender<BusData>,
    pub(crate) registry: Registry,
    pub(crate) kill_rx: watch::Receiver<bool>,
    pub(crate) cap: usize,
}

impl Drop for IdentityOfRx {
    fn drop(&mut self) {
        if self.tx_data.send(BusData::Drop(self.id.clone())).is_err() {
            debug!("{} send BusData::Drop fail", self.id);
        }
    }
}

impl From<IdentityCommon> for IdentityOfRx {
    fn from(value: IdentityCommon) -> Self {
        let IdentityCommon {
            id,
            rx_event,
            inbox_tx,
            tx_data,
            registry,
            kill_rx,
            cap,
        } = value;
        Self {
            id,
            rx_event,
            inbox_tx,
            tx_data,
            registry,
            kill_rx,
            cap,
        }
    }
}

impl IdentityOfRx {
    pub fn tx(&self) -> IdentityOfTx {
        IdentityOfTx {
            id: self.id.clone(),
            registry: self.registry.clone(),
        }
    }

    pub fn into_simple<T>(self) -> IdentityOfSimple<T> {
        IdentityOfSimple::new(self)
    }

    pub fn into_merge<T: Merge>(self) -> IdentityOfMerge<T> {
        IdentityOfMerge::new(self)
    }

    pub fn into_merge_tick<T: Merge + FromTick>(
        self,
        duration: Duration,
    ) -> IdentityOfMergeTick<T> {
        IdentityOfMergeTick::new(self, duration)
    }

    pub async fn recv_event(&mut self) -> Option<BusEvent> {
        if *self.kill_rx.borrow() {
            return None;
        }
        tokio::select! {
            _ = self.kill_rx.changed() => None,
            ev = self.rx_event.recv() => ev,
        }
    }

    pub async fn recv<T: Event>(&mut self) -> Result<Option<Arc<T>>, BusError> {
        match self.recv_event().await {
            Some(event) => {
                let any_event = event.as_any();
                if let Ok(msg) = any_event.downcast::<T>() {
                    Ok(Some(msg))
                } else {
                    Err(BusError::downcast_failed(T::name(), event.type_name()))
                }
            }
            None => Ok(None),
        }
    }

    pub fn try_recv_event(&mut self) -> Result<Option<BusEvent>, BusError> {
        match self.rx_event.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(err) => match err {
                TryRecvError::Empty => Ok(None),
                TryRecvError::Disconnected => Err(BusError::channel_closed(
                    "worker_try_recv_event",
                    Some(self.id.name()),
                )),
            },
        }
    }

    pub fn try_recv<T: Event>(&mut self) -> Result<Option<Arc<T>>, BusError> {
        match self.try_recv_event()? {
            Some(event) => {
                let any_event = event.as_any();
                if let Ok(msg) = any_event.downcast::<T>() {
                    Ok(Some(msg))
                } else {
                    Err(BusError::downcast_failed(T::name(), event.type_name()))
                }
            }
            None => Ok(None),
        }
    }

    pub async fn recv_merge<T: Merge + Event>(&mut self) -> Result<Option<T>, BusError> {
        let Some(event) = self.recv_event().await else {
            return Ok(None);
        };
        Ok(Some(T::merge(event)?))
    }

    pub fn try_recv_merge<T: Merge + Event>(&mut self) -> Result<Option<T>, BusError> {
        match self.try_recv_event()? {
            None => Ok(None),
            Some(event) => Ok(Some(T::merge(event)?)),
        }
    }

    fn register(&self, route_key: RouteKey, name: &'static str) {
        let brx = subscribe_sender(&self.registry, route_key.clone(), self.cap);
        spawn_forwarder(brx, self.inbox_tx.clone(), self.kill_rx.clone());
        // 仅控制面记账，供 trace/cleanup 使用，失败不影响数据面。
        let _ = self
            .tx_data
            .send(BusData::Subscribe(self.id.clone(), route_key, name));
    }

    pub async fn subscribe_merge<T: Merge>(&self) -> Result<(), BusError> {
        for (type_id, name) in T::subscribe_types() {
            self.register(RouteKey::Type(type_id), name);
        }
        Ok(())
    }

    pub async fn subscribe<T: Event + 'static>(&self) -> Result<(), BusError> {
        self.register(RouteKey::Type(TypeId::of::<T>()), T::name());
        Ok(())
    }

    pub async fn dispatch_event<T: Event>(&self, event: T) -> Result<(), BusError> {
        self.tx().dispatch_event(event).await
    }

    pub async fn subscribe_with_key<T: Event + 'static>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError> {
        self.register(
            RouteKey::TypeWithKey(TypeId::of::<T>(), key.into()),
            T::name(),
        );
        Ok(())
    }

    pub async fn dispatch_with_key<T: Event>(
        &self,
        key: impl Into<String>,
        event: T,
    ) -> Result<(), BusError> {
        self.tx().dispatch_with_key(key, event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;

    #[derive(Debug)]
    struct A;

    #[derive(Debug)]
    struct B;

    impl Event for A {
        fn name() -> &'static str {
            "A"
        }
    }

    impl Event for B {
        fn name() -> &'static str {
            "B"
        }
    }

    #[tokio::test]
    async fn recv_reports_expected_and_actual_type_on_downcast_failure() {
        let bus = Bus::<4>::init();
        let mut id = bus.login_with_name("test-worker".to_string()).await.unwrap();
        id.subscribe::<B>().await.unwrap();
        let tx = bus.login_with_name("dispatcher".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        tx.dispatch_event(B).await.unwrap();
        let err = id.recv::<A>().await.unwrap_err();
        assert_eq!(
            err,
            BusError::DowncastFailed {
                expected: "A",
                actual: "B",
            }
        );
    }

    #[tokio::test]
    async fn try_recv_event_empty_when_no_event() {
        let bus = Bus::<4>::init();
        let mut id = bus
            .login_with_name("idle-worker".to_string())
            .await
            .unwrap();
        id.subscribe::<A>().await.unwrap();
        assert!(matches!(id.try_recv_event(), Ok(None)));
    }
}
