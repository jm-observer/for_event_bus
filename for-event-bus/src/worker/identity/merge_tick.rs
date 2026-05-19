use crate::bus::BusError;
use crate::worker::identity::{
    FromTick, IdentityOfRx, IdentityOfTx, Merge, MergeSkip,
};
use crate::Event;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::{interval_at, Instant, Interval};

/// Merge + Tick 语义身份：事件和定时 tick 统一投影为 T。
pub struct IdentityOfMergeTick<T: Merge + FromTick> {
    pub(crate) id: IdentityOfRx,
    pub(crate) interval: Option<Interval>,
    pub(crate) duration: Duration,
    pub(crate) phantom: PhantomData<T>,
}

impl<T: Merge + FromTick> IdentityOfMergeTick<T> {
    pub(crate) fn new(id: IdentityOfRx, duration: Duration) -> Self {
        Self {
            id,
            interval: None,
            duration,
            phantom: PhantomData,
        }
    }

    pub fn tx(&self) -> IdentityOfTx {
        self.id.tx()
    }

    pub async fn recv(&mut self) -> Result<Option<T>, BusError> {
        let interval = self
            .interval
            .get_or_insert_with(|| interval_at(Instant::now() + self.duration, self.duration));
        tokio::select! {
            event = self.id.recv_event() => match event {
                Some(event) => Ok(Some(T::merge(event)?)),
                None => Ok(None),
            },
            _ = interval.tick() => Ok(Some(T::from_tick())),
        }
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

    /// 更新 tick 周期，下次 recv() 时生效
    pub fn set_duration(&mut self, duration: Duration) {
        self.duration = duration;
        self.interval = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{Bus, BusEvent};
    use std::any::TypeId;

    #[derive(Debug)]
    struct Ping;

    impl Event for Ping {
        fn name() -> &'static str {
            "Ping"
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum IntervalTestSignal {
        Ping,
        Tick,
    }

    impl Event for IntervalTestSignal {
        fn name() -> &'static str {
            "IntervalTestSignal"
        }
    }

    impl Merge for IntervalTestSignal {
        fn merge(event: BusEvent) -> Result<Self, BusError> {
            let any = event.as_any();
            if any.downcast::<Ping>().is_ok() {
                Ok(Self::Ping)
            } else {
                Err(BusError::downcast_failed("Ping", event.type_name()))
            }
        }

        fn subscribe_types() -> Vec<(TypeId, &'static str)> {
            vec![(TypeId::of::<Ping>(), Ping::name())]
        }
    }

    impl FromTick for IntervalTestSignal {
        fn from_tick() -> Self {
            Self::Tick
        }
    }

    #[tokio::test]
    async fn recv_signal_can_receive_event() {
        let bus = Bus::<4>::init();
        let id = bus
            .login_with_name("interval-worker-event".to_string())
            .await
            .unwrap();
        id.subscribe_merge::<IntervalTestSignal>().await.unwrap();
        let mut merge_tick =
            IdentityOfMergeTick::<IntervalTestSignal>::new(id, Duration::from_secs(5));
        let tx = bus
            .login_with_name("ping-dispatcher".to_string())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        tx.dispatch_event(Ping).await.unwrap();
        let signal = merge_tick.recv().await.unwrap();
        assert_eq!(signal, Some(IntervalTestSignal::Ping));
    }

    #[tokio::test]
    async fn recv_signal_can_receive_tick() {
        let bus = Bus::<4>::init();
        let id = bus
            .login_with_name("interval-worker-tick".to_string())
            .await
            .unwrap();
        let mut merge_tick =
            IdentityOfMergeTick::<IntervalTestSignal>::new(id, Duration::from_millis(10));

        let signal = merge_tick.recv().await.unwrap();
        assert_eq!(signal, Some(IntervalTestSignal::Tick));
    }
}
