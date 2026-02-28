use crate::bus::BusError;
use crate::worker::identity::{IdentityCommon, IdentityOfRx, IdentityOfTx};

use crate::Event;
use std::marker::PhantomData;
use std::sync::Arc;

/// 简单的worker身份标识，只订阅一种事件
pub struct IdentityOfSimple<T> {
    pub(crate) id: IdentityOfRx,
    pub(crate) phantom: PhantomData<T>,
}

impl<T> From<IdentityCommon> for IdentityOfSimple<T> {
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

impl<T: Event> IdentityOfSimple<T> {
    pub fn tx(&self) -> IdentityOfTx {
        IdentityOfTx {
            id: self.id.id.clone(),
            tx_data: self.id.tx_data.clone(),
        }
    }
    pub async fn recv(&mut self) -> Result<Arc<T>, BusError> {
        self.id.recv::<T>().await
    }

    pub fn try_recv(&mut self) -> Result<Option<Arc<T>>, BusError> {
        self.id.try_recv::<T>()
    }
    pub(crate) async fn subscribe(&self) -> Result<(), BusError> {
        Ok(self.id.subscribe::<T>().await?)
    }

    pub async fn subscribe_with_key<E: Event + 'static>(&self, key: impl Into<String>) -> Result<(), BusError> {
        Ok(self.id.subscribe_with_key::<E>(key).await?)
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
