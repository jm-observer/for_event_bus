use crate::bus::{BusError, BusEvent};
use std::any::TypeId;

pub trait Merge {
    fn merge(event: BusEvent) -> Result<Self, BusError>
    where
        Self: Sized;

    fn subscribe_types() -> Vec<(TypeId, &'static str)>;
}
