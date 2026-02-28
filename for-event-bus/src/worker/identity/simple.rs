use crate::bus::BusError;
use crate::worker::identity::{IdentityOfRx, IdentityOfTx};
use crate::Event;
use std::marker::PhantomData;
use std::sync::Arc;

/// Simple 语义身份：只关心单一事件类型 T。
pub struct IdentityOfSimple<T> {
    pub(crate) id: IdentityOfRx,
    pub(crate) phantom: PhantomData<T>,
}

impl<T> IdentityOfSimple<T> {
    pub(crate) fn new(id: IdentityOfRx) -> Self {
        Self {
            id,
            phantom: PhantomData,
        }
    }

    pub fn tx(&self) -> IdentityOfTx {
        self.id.tx()
    }

    pub fn into_inner(self) -> IdentityOfRx {
        self.id
    }
}

impl<T: Event + 'static> IdentityOfSimple<T> {
    pub async fn recv(&mut self) -> Result<Option<Arc<T>>, BusError> {
        self.id.recv::<T>().await
    }

    pub fn try_recv(&mut self) -> Result<Option<Arc<T>>, BusError> {
        self.id.try_recv::<T>()
    }

    pub async fn subscribe(&self) -> Result<(), BusError> {
        self.id.subscribe::<T>().await
    }

    pub async fn subscribe_with_key<E: Event + 'static>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError> {
        self.id.subscribe_with_key::<E>(key).await
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
