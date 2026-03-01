use crate::bus::{BusError, BusEvent};
use crate::worker::identity::{FromTick, IdentityOfMergeTick, IdentityOfRx, IdentityOfTx};
use crate::Event;
use std::any::TypeId;
use std::marker::PhantomData;
use std::time::Duration;

pub trait Merge {
    fn merge(event: BusEvent) -> Result<Self, BusError>
    where
        Self: Sized;

    fn subscribe_types() -> Vec<(TypeId, &'static str)>;
}

/// 标记某个 Merge 枚举是否包含并支持子事件类型 E。
pub trait MergeContains<E> {}

/// 标记某个 Merge 枚举是否将子事件类型 E 配置为 `#[merge(skip)]`。
pub trait MergeSkip<E> {}

/// Merge 语义身份：把多种 BusEvent 聚合为一个业务事件 T。
pub struct IdentityOfMerge<T: Merge> {
    pub(crate) id: IdentityOfRx,
    pub(crate) phantom: PhantomData<T>,
}

impl<T: Merge> IdentityOfMerge<T> {
    pub(crate) fn new(id: IdentityOfRx) -> Self {
        Self {
            id,
            phantom: PhantomData,
        }
    }

    pub fn tx(&self) -> IdentityOfTx {
        self.id.tx()
    }

    pub async fn recv(&mut self) -> Result<Option<T>, BusError>
    where
        T: Event,
    {
        self.id.recv_merge::<T>().await
    }

    pub fn try_recv(&mut self) -> Result<Option<T>, BusError>
    where
        T: Event,
    {
        self.id.try_recv_merge::<T>()
    }

    pub async fn subscribe_with_key<E: Event + 'static>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError>
    where
        T: MergeSkip<E>,
    {
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

    pub fn into_merge_tick(self, duration: Duration) -> IdentityOfMergeTick<T>
    where
        T: Event + FromTick,
    {
        self.id.into_merge_tick(duration)
    }
}
