use crate::bus::BusError;
use crate::worker::identity::{FromTick, IdentityOfMerge, IdentityOfRx, IdentityOfTx, Merge};
use crate::Event;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::{interval_at, Instant, Interval};

/// Merge + Tick 语义身份：事件和定时 tick 统一投影为 T。
pub struct IdentityOfMergeTick<T: Merge + Event + FromTick> {
    pub(crate) id: IdentityOfRx,
    pub(crate) interval: Interval,
    pub(crate) phantom: PhantomData<T>,
}

impl<T: Merge + Event + FromTick> IdentityOfMergeTick<T> {
    pub(crate) fn new(id: IdentityOfRx, duration: Duration) -> Self {
        Self {
            id,
            interval: interval_at(Instant::now() + duration, duration),
            phantom: PhantomData,
        }
    }

    pub fn tx(&self) -> IdentityOfTx {
        self.id.tx()
    }

    pub async fn recv_signal(&mut self) -> Result<Option<T>, BusError> {
        tokio::select! {
            event = self.id.rx_event.recv() => match event {
                Some(event) => Ok(Some(T::merge(event)?)),
                None => Ok(None),
            },
            _ = self.interval.tick() => Ok(Some(T::from_tick())),
        }
    }

    pub async fn recv_merge(&mut self) -> Result<Option<T>, BusError> {
        self.id.recv_merge::<T>().await
    }

    pub fn try_recv_merge(&mut self) -> Result<Option<T>, BusError> {
        self.id.try_recv_merge::<T>()
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

    pub fn into_merge(self) -> IdentityOfMerge<T> {
        IdentityOfMerge::new(self.id)
    }

    pub fn into_inner(self) -> IdentityOfRx {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{BusData, BusEvent};
    use crate::worker::WorkerId;
    use std::any::TypeId;
    use tokio::sync::mpsc;

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
        let (tx_data, _rx_data) = mpsc::unbounded_channel::<BusData>();
        let (tx_event, rx_event) = mpsc::channel::<BusEvent>(1);
        let id = WorkerId::init("interval-worker-event".to_string());
        let identity = IdentityOfRx {
            id,
            rx_event,
            tx_data,
        };
        let mut merge_tick = IdentityOfMergeTick::<IntervalTestSignal>::new(
            identity,
            Duration::from_secs(5),
        );

        tx_event.send(BusEvent::new(Ping)).await.unwrap();
        let signal = merge_tick.recv_signal().await.unwrap();
        assert_eq!(signal, Some(IntervalTestSignal::Ping));
    }

    #[tokio::test]
    async fn recv_signal_can_receive_tick() {
        let (tx_data, _rx_data) = mpsc::unbounded_channel::<BusData>();
        let (_tx_event, rx_event) = mpsc::channel::<BusEvent>(1);
        let id = WorkerId::init("interval-worker-tick".to_string());
        let identity = IdentityOfRx {
            id,
            rx_event,
            tx_data,
        };
        let mut merge_tick = IdentityOfMergeTick::<IntervalTestSignal>::new(
            identity,
            Duration::from_millis(10),
        );

        let signal = merge_tick.recv_signal().await.unwrap();
        assert_eq!(signal, Some(IntervalTestSignal::Tick));
    }
}
