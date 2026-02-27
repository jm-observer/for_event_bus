use crate::bus::{BusData, BusError, BusEvent, RouteKey};
use crate::worker::WorkerId;
use log::debug;
use std::any::TypeId;
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

mod merge;
mod simple;

use crate::Event;
pub use merge::{IdentityOfMerge, Merge};
pub use simple::IdentityOfSimple;

#[derive(Clone)]
pub struct IdentityOfTx {
    id: WorkerId,
    tx_data: UnboundedSender<BusData>,
}

impl IdentityOfTx {
    pub async fn subscribe<T: Event + 'static>(&self) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::Subscribe(
            self.id.clone(),
            RouteKey::Type(TypeId::of::<T>()),
            T::name(),
        ))?)
    }

    pub async fn dispatch_event<T: Event>(&self, event: T) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::DispatchEvent(
            self.id.clone(),
            RouteKey::Type(TypeId::of::<T>()),
            BusEvent::new(event),
        ))?)
    }

    pub async fn subscribe_with_key<T: Event + 'static>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::Subscribe(
            self.id.clone(),
            RouteKey::TypeWithKey(TypeId::of::<T>(), key.into()),
            T::name(),
        ))?)
    }

    pub async fn dispatch_with_key<T: Event>(
        &self,
        key: impl Into<String>,
        event: T,
    ) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::DispatchEvent(
            self.id.clone(),
            RouteKey::TypeWithKey(TypeId::of::<T>(), key.into()),
            BusEvent::new(event),
        ))?)
    }
}

/// 通用的worker身份标识，可以订阅多种事件
pub struct IdentityOfRx {
    pub id: WorkerId,
    pub rx_event: Receiver<BusEvent>,
    pub(crate) tx_data: UnboundedSender<BusData>,
}

pub struct IdentityCommon {
    pub(crate) id: WorkerId,
    pub(crate) rx_event: Receiver<BusEvent>,
    pub(crate) tx_data: UnboundedSender<BusData>,
}

impl Drop for IdentityOfRx {
    fn drop(&mut self) {
        // 控制面改为 unbounded 后，Drop 通知不受容量限制。
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
            tx_data,
        } = value;
        Self {
            id,
            rx_event,
            tx_data,
        }
    }
}

impl IdentityOfRx {
    // pub fn init(
    //     id: WorkerId,
    //     rx_event: Receiver<Event>,
    //     tx_data: Sender<BusData>,
    // ) -> Self {
    //     Self {
    //         id,
    //         rx_event,
    //         tx_data,
    //     }
    // }

    pub fn tx(&self) -> IdentityOfTx {
        IdentityOfTx {
            id: self.id.clone(),
            tx_data: self.tx_data.clone(),
        }
    }

    // pub async fn recv_event<T: Send + Sync + 'static>(&mut self) -> Option<T> {
    //     self.rx_event.recv().await
    // }
    pub fn rx_event_mut(&mut self) -> &mut Receiver<BusEvent> {
        &mut self.rx_event
    }

    pub async fn recv_event(&mut self) -> Result<BusEvent, BusError> {
        self.rx_event
            .recv()
            .await
            .ok_or_else(|| BusError::channel_closed("worker_recv_event", Some(self.id.name())))
    }
    pub async fn recv<T: Event>(&mut self) -> Result<Arc<T>, BusError> {
        match self.recv_event().await {
            Ok(event) => {
                let any_event = event.as_any();
                if let Ok(msg) = any_event.downcast::<T>() {
                    Ok(msg)
                } else {
                    Err(BusError::downcast_failed(T::name(), event.type_name()))
                }
            }
            Err(e) => Err(e),
        }

        // let event = self.recv_event().await?;
        // let any_event: Arc<dyn Any + Send + Sync> = Arc::new(&*event);
        // if let Ok(msg) = any_event.downcast::<T>() {
        //     return Ok(msg);
        // } else {
        //     Err(BusError::DowncastErr)
        // }
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

    // pub fn subscribe(&self, type_id: TypeId) -> Result<(), BusError> {
    //     Ok(self.tx_data.send(BusData::Subscribe(self.id, type_id))?)
    // }

    // pub fn subscribe<T: Any>(&self, worker: &T) -> Result<(), BusError> {
    //     Ok(self
    //         .tx_data
    //         .send(BusData::Subscribe(self.id, worker.type_id()))?)
    // }
    pub async fn subscribe<T: Event + 'static>(&self) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::Subscribe(
            self.id.clone(),
            RouteKey::Type(TypeId::of::<T>()),
            T::name(),
        ))?)
    }

    pub async fn dispatch_event<T: Event>(&self, event: T) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::DispatchEvent(
            self.id.clone(),
            RouteKey::Type(TypeId::of::<T>()),
            BusEvent::new(event),
        ))?)
    }

    pub async fn subscribe_with_key<T: Event + 'static>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::Subscribe(
            self.id.clone(),
            RouteKey::TypeWithKey(TypeId::of::<T>(), key.into()),
            T::name(),
        ))?)
    }

    pub async fn dispatch_with_key<T: Event>(
        &self,
        key: impl Into<String>,
        event: T,
    ) -> Result<(), BusError> {
        Ok(self.tx_data.send(BusData::DispatchEvent(
            self.id.clone(),
            RouteKey::TypeWithKey(TypeId::of::<T>(), key.into()),
            BusEvent::new(event),
        ))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let (tx_data, _rx_data) = tokio::sync::mpsc::unbounded_channel::<BusData>();
        let (tx_event, rx_event) = tokio::sync::mpsc::channel::<BusEvent>(1);
        let worker = WorkerId::init("test-worker".to_string());
        let mut identity = IdentityOfRx {
            id: worker,
            rx_event,
            tx_data,
        };

        tx_event.send(BusEvent::new(B)).await.unwrap();
        let err = identity.recv::<A>().await.unwrap_err();
        assert_eq!(
            err,
            BusError::DowncastFailed {
                expected: "A",
                actual: "B",
            }
        );
    }

    #[test]
    fn try_recv_event_reports_channel_closed_with_worker() {
        let (tx_data, _rx_data) = tokio::sync::mpsc::unbounded_channel::<BusData>();
        let (tx_event, rx_event) = tokio::sync::mpsc::channel::<BusEvent>(1);
        drop(tx_event);
        let mut identity = IdentityOfRx {
            id: WorkerId::init("closed-worker".to_string()),
            rx_event,
            tx_data,
        };

        let err = match identity.try_recv_event() {
            Ok(_) => panic!("expected channel closed error"),
            Err(err) => err,
        };
        match err {
            BusError::ChannelClosed { stage, worker } => {
                assert_eq!(stage, "worker_try_recv_event");
                assert_eq!(worker.as_deref(), Some("closed-worker"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
