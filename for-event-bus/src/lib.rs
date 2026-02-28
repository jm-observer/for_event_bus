// #![feature(associated_type_defaults)]
// #![feature(trait_upcasting)]

mod bus;
mod worker;

pub use bus::{Bus, BusError, EntryOfBus};
use std::any::Any;
use std::sync::Arc;
pub use worker::{
    identity::{
        IdentityOfInterval, IdentityOfMerge, IdentityOfRx, IdentityOfSimple, IdentityOfTx,
        IdentitySignal, Merge,
    },
    ToWorker,
};

pub use crate::bus::BusEvent;
#[cfg(feature = "derive")]
pub use for_event_bus_derive::{Event, Merge, Worker};

pub type SimpleBus = Bus<1000>;

pub trait Event: Any + Send + Sync + 'static {
    fn name() -> &'static str
    where
        Self: Sized;
}

impl Event for () {
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "()"
    }
}

pub fn upcast(event: BusEvent) -> Arc<dyn Any + Send + Sync + 'static> {
    event.as_any()
}
