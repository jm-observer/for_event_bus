use crate::bus::BusError;
use crate::worker::identity::{
    FromTick, IdentityOfMerge, IdentityOfMergeTick, IdentityOfRx, IdentityOfTx, Merge,
    MergeContains,
};
use crate::Event;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

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

    pub async fn into_merge<U>(self) -> Result<IdentityOfMerge<U>, BusError>
    where
        U: Merge + MergeContains<T>,
    {
        let id = self.id;
        id.subscribe_merge::<U>().await?;
        Ok(id.into_merge())
    }

    /// todo
    pub async fn into_merge_tick<U>(
        self,
        duration: Duration,
    ) -> Result<IdentityOfMergeTick<U>, BusError>
    where
        U: Merge + Event + FromTick + MergeContains<T>,
    {
        let id = self.id;
        id.subscribe_merge::<U>().await?;
        Ok(id.into_merge_tick(duration))
    }
}

impl<T: Event + 'static> IdentityOfSimple<T> {
    pub async fn recv(&mut self) -> Result<Option<Arc<T>>, BusError> {
        self.id.recv::<T>().await
    }

    pub fn try_recv(&mut self) -> Result<Option<Arc<T>>, BusError> {
        self.id.try_recv::<T>()
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
