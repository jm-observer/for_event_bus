use std::any::TypeId;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;

use crate::bus::BusEvent;
use tokio::sync::mpsc::Sender;

pub mod identity;

static ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Eq, PartialEq, Clone, Hash)]
pub struct WorkerId {
    id: usize,
    name: Arc<String>,
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.name, self.id)
    }
}

impl WorkerId {
    pub fn init(name: String) -> Self {
        let id = ID.fetch_add(1, Ordering::Release);
        Self {
            id,
            name: Arc::new(name),
        }
    }
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

pub(crate) struct Worker {
    id: WorkerId,
    tx: Sender<BusEvent>,
}

impl Worker {
    pub fn id(&self) -> WorkerId {
        self.id.clone()
    }
    pub async fn try_send(&self, event: BusEvent) -> Result<(), TrySendError<BusEvent>> {
        self.tx.try_send(event)
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum SubscribeKey {
    Type(TypeId),
    TypeWithKey(TypeId, String),
}

pub(crate) struct CopyOfWorker {
    id: WorkerId,
    tx_event: Sender<BusEvent>,
    subscribe_keys: HashSet<SubscribeKey>,
}
impl CopyOfWorker {
    pub fn init(id: WorkerId, tx_event: Sender<BusEvent>) -> Self {
        Self {
            id,
            tx_event,
            subscribe_keys: Default::default(),
        }
    }
    pub fn id(&self) -> WorkerId {
        self.id.clone()
    }

    pub fn init_subscriber(&self) -> Worker {
        Worker {
            id: self.id.clone(),
            tx: self.tx_event.clone(),
        }
    }
    pub fn subscribe_event(&mut self, ty_id: TypeId) {
        self.subscribe_keys.insert(SubscribeKey::Type(ty_id));
    }
    pub fn subscribe_event_with_key(&mut self, ty_id: TypeId, key: String) {
        self.subscribe_keys
            .insert(SubscribeKey::TypeWithKey(ty_id, key));
    }
    pub fn subscribe_keys(&self) -> std::collections::hash_set::Iter<'_, SubscribeKey> {
        self.subscribe_keys.iter()
    }
}

// #[async_trait]
// pub trait Worker {
//     // type EventType: Send + Sync + 'static = ();
//
//     fn identity_tx(&self) -> &IdentityOfTx;
//
//     fn subscribe<T: ?Sized + 'static>(&self) -> Result<(), BusError> {
//         self.identity_tx().subscribe::<T>()
//     }
//
//     fn dispatch_event<T: Any + Send + Sync + 'static>(&mut self, event: T) -> Result<(), BusError> {
//         let identity = self.identity_tx();
//         identity.dispatch_event(event)
//     }
//
//     /*
//     async fn recv(&mut self) -> Option<Arc<Self::EventType>> {
//         while let Some(event) = self.identity_rx_mut().recv_event().await {
//             if let Ok(msg) = event.downcast::<Self::EventType>() {
//                 return Some(msg);
//             }
//         }
//         None
//     }
//     */
// }

pub trait ToWorker {
    fn name() -> String;
}
