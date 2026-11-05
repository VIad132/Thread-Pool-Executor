//! Легкий менеджер пулу потоків (Thread Pool Executor).
//!
//! Використовує лише стандартну бібліотеку Rust, тому однаково
//! збирається та працює у Windows, macOS і Linux.

pub mod error;
pub mod monitor;
pub mod pool;
pub mod task;
pub mod worker;

pub use error::PoolError;
pub use monitor::{Metrics, MetricsSnapshot};
pub use pool::ThreadPool;
pub use task::{TaskId, TaskState};
