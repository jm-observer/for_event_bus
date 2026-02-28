use crate::bus::BusEvent;

pub trait Merge {
    fn merge(event: BusEvent) -> Result<Self, BusError>
    where
        Self: Sized;

    fn subscribe_types() -> Vec<(TypeId, &'static str)>;
}

use crate::bus::BusError;
use crate::worker::identity::{IdentityCommon, IdentityOfRx, IdentityOfTx};

use crate::Event;
use std::any::TypeId;
use std::marker::PhantomData;

/// 简单的worker身份标识，只订阅一种事件
pub struct IdentityOfMerge<T: Merge> {
    pub(crate) id: IdentityOfRx,
    pub(crate) phantom: PhantomData<T>,
}

impl<T: Merge> From<IdentityCommon> for IdentityOfMerge<T> {
    fn from(value: IdentityCommon) -> Self {
        let IdentityCommon {
            id,
            rx_event,
            tx_data,
        } = value;
        let id = IdentityOfRx {
            id,
            rx_event,
            tx_data,
        };
        Self {
            id,
            phantom: Default::default(),
        }
    }
}

impl<T: Merge + Event> IdentityOfMerge<T> {
    pub fn tx(&self) -> IdentityOfTx {
        self.id.tx()
    }

    pub async fn recv(&mut self) -> Result<T, BusError> {
        self.id.recv_merge::<T>().await
    }

    pub fn try_recv(&mut self) -> Result<Option<T>, BusError> {
        self.id.try_recv_merge::<T>()
    }

    pub(crate) async fn subscribe(&self) -> Result<(), BusError> {
        self.id.subscribe_merge::<T>().await
    }

    pub async fn subscribe_with_key<E: Event>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError> {
        self.id.subscribe_with_key::<E>(key).await?;
        Ok(())
    }

    pub async fn dispatch_event<E: Event>(&self, event: E) -> Result<(), BusError> {
        self.id.dispatch_event(event).await
    }

    pub async fn dispatch_with_key<E: Event>(
        &self,
        key: impl Into<String>,
        event: E,
    ) -> Result<(), BusError> {
        self.id.dispatch_with_key(key, event).await
    }
}
