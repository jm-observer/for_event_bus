use crate::bus::{BusData, BusEvent, RouteKey};

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
        IdentityOfTx {
            id: self.id.id.clone(),
            tx_data: self.id.tx_data.clone(),
        }
    }
    pub async fn recv(&mut self) -> Result<T, BusError> {
        let event = self.id.recv_event().await?;
        T::merge(event)
    }

    pub fn try_recv(&mut self) -> Result<Option<T>, BusError> {
        let event = self.id.try_recv_event()?;
        match event {
            None => Ok(None),
            Some(event) => Ok(Some(T::merge(event)?)),
        }
    }
    pub(crate) async fn subscribe(&self) -> Result<(), BusError> {
        for (type_id, name) in T::subscribe_types() {
            self.id.tx_data.send(BusData::Subscribe(
                self.id.id.clone(),
                RouteKey::Type(type_id),
                name,
            ))?;
        }
        Ok(())
    }

    pub async fn subscribe_with_key<E: Event>(&self, key: impl Into<String>) -> Result<(), BusError> {
        self.id.subscribe_with_key::<E>(key).await?;
        Ok(())
    }

    pub async fn dispatch_event<E: Event>(&self, event: E) -> Result<(), BusError> {
        Ok(self.id.dispatch_event(event).await?)
    }

    pub async fn dispatch_with_key<E: Event>(
        &self,
        key: impl Into<String>,
        event: E,
    ) -> Result<(), BusError> {
        Ok(self.id.dispatch_with_key(key, event).await?)
    }
}
