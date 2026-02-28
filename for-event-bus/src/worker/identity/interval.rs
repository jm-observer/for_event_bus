use crate::bus::{BusError, BusEvent};
use crate::worker::identity::{IdentityCommon, IdentityOfRx, IdentityOfTx};
use crate::worker::WorkerId;
use crate::Event;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval_at, Instant, Interval};

/// interval identity 收到的信号：要么是总线事件，要么是定时 tick。
pub enum IdentitySignal {
    Event(BusEvent),
    Tick,
}

/// 带 interval 能力的 worker 身份标识。
pub struct IdentityOfInterval {
    pub(crate) id: IdentityOfRx,
    pub(crate) interval: Interval,
}

impl IdentityOfInterval {
    pub fn new(id: IdentityOfRx, duration: Duration) -> Self {
        Self {
            id,
            interval: interval_at(Instant::now() + duration, duration),
        }
    }

    pub fn tx(&self) -> IdentityOfTx {
        self.id.tx()
    }

    pub fn worker_id(&self) -> WorkerId {
        self.id.id.clone()
    }

    pub fn interval_mut(&mut self) -> &mut Interval {
        &mut self.interval
    }

    pub async fn recv_signal(&mut self) -> Result<IdentitySignal, BusError> {
        tokio::select! {
            event = self.id.rx_event.recv() => {
                event
                    .map(IdentitySignal::Event)
                    .ok_or_else(|| BusError::channel_closed("worker_recv_signal", Some(self.id.id.name())))
            }
            _ = self.interval.tick() => Ok(IdentitySignal::Tick),
        }
    }

    pub async fn recv<T: Event>(&mut self) -> Result<Arc<T>, BusError> {
        self.id.recv::<T>().await
    }

    pub fn try_recv<T: Event>(&mut self) -> Result<Option<Arc<T>>, BusError> {
        self.id.try_recv::<T>()
    }

    pub async fn subscribe<T: Event + 'static>(&self) -> Result<(), BusError> {
        self.id.subscribe::<T>().await
    }

    pub async fn subscribe_with_key<T: Event + 'static>(
        &self,
        key: impl Into<String>,
    ) -> Result<(), BusError> {
        self.id.subscribe_with_key::<T>(key).await
    }

    pub async fn dispatch_event<T: Event>(&self, event: T) -> Result<(), BusError> {
        self.id.dispatch_event(event).await
    }

    pub async fn dispatch_with_key<T: Event>(
        &self,
        key: impl Into<String>,
        event: T,
    ) -> Result<(), BusError> {
        self.id.dispatch_with_key(key, event).await
    }

    pub fn into_inner(self) -> IdentityOfRx {
        self.id
    }
}

impl From<(IdentityCommon, Duration)> for IdentityOfInterval {
    fn from(value: (IdentityCommon, Duration)) -> Self {
        let (common, duration) = value;
        let id: IdentityOfRx = common.into();
        Self::new(id, duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::BusData;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct Ping;

    impl Event for Ping {
        fn name() -> &'static str {
            "Ping"
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
        let mut interval_id = IdentityOfInterval::new(identity, Duration::from_secs(5));

        tx_event.send(BusEvent::new(Ping)).await.unwrap();
        let signal = interval_id.recv_signal().await.unwrap();

        match signal {
            IdentitySignal::Event(event) => assert_eq!(event.type_name(), "Ping"),
            IdentitySignal::Tick => panic!("expected event"),
        }
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
        let mut interval_id = IdentityOfInterval::new(identity, Duration::from_millis(10));

        let signal = interval_id.recv_signal().await.unwrap();
        match signal {
            IdentitySignal::Tick => {}
            IdentitySignal::Event(_) => panic!("expected tick"),
        }
    }
}
