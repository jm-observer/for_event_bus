use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
        let id = ID.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
            name: Arc::new(name),
        }
    }
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
    pub(crate) fn name_arc(&self) -> Arc<String> {
        self.name.clone()
    }
}

pub trait ToWorker {
    fn name() -> String;
}
