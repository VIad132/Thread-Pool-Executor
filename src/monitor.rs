use crate::task::{TaskId, TaskState};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// Знімок агрегованих метрик пулу в один момент часу.
///
/// Лічильники зчитуються окремо, тому знімок не є строго атомарним,
/// проте після завершення всіх задач він завжди узгоджений.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetricsSnapshot {
    /// Задачі, що очікують у черзі.
    pub queued: usize,
    /// Задачі, що виконуються просто зараз.
    pub active: usize,
    /// Успішно завершені задачі.
    pub completed: usize,
    /// Задачі, що завершилися панікою.
    pub failed: usize,
    /// Максимальна кількість одночасно виконуваних задач.
    pub peak_active: usize,
}

impl fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "у черзі: {} | виконується: {} | виконано: {} | з помилкою: {} | пік паралельності: {}",
            self.queued, self.active, self.completed, self.failed, self.peak_active
        )
    }
}

/// Спільний монітор стану пулу.
///
/// Агреговані лічильники — атомарні (без блокувань), детальні стани
/// окремих задач — у реєстрі під м'ютексом. Умовна змінна дозволяє
/// чекати на момент, коли пул стає вільним, без активного очікування.
#[derive(Default)]
pub struct Metrics {
    queued: AtomicUsize,
    active: AtomicUsize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    peak_active: AtomicUsize,
    states: Mutex<HashMap<TaskId, TaskState>>,
    idle_gate: Mutex<()>,
    idle_cv: Condvar,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Реєстр станів; отруєний м'ютекс (паніка іншого потоку) ігноруємо,
    /// бо дані реєстру залишаються цілісними.
    fn states(&self) -> MutexGuard<'_, HashMap<TaskId, TaskState>> {
        self.states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Задача прийнята пулом.
    pub(crate) fn on_submit(&self, id: TaskId) {
        self.states().insert(id, TaskState::Pending);
        self.queued.fetch_add(1, Ordering::SeqCst);
    }

    /// Відкат реєстрації, якщо передати задачу в канал не вдалося.
    pub(crate) fn on_submit_failed(&self, id: TaskId) {
        self.states().remove(&id);
        self.queued.fetch_sub(1, Ordering::SeqCst);
        self.notify_idle();
    }

    /// Потік-виконавець взяв задачу з черги.
    ///
    /// Спочатку збільшуємо `active`, потім зменшуємо `queued`, щоб задача
    /// у жодний момент не «зникала» з обліку і `is_idle` не давав хибного результату.
    pub(crate) fn on_start(&self, id: TaskId) {
        let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_active.fetch_max(now, Ordering::SeqCst);
        self.queued.fetch_sub(1, Ordering::SeqCst);
        self.states().insert(id, TaskState::Running);
    }

    /// Задачу завершено (успішно або панікою).
    pub(crate) fn on_finish(&self, id: TaskId, success: bool) {
        let state = if success {
            TaskState::Completed
        } else {
            TaskState::Failed
        };
        self.states().insert(id, state);
        if success {
            self.completed.fetch_add(1, Ordering::SeqCst);
        } else {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.notify_idle();
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            queued: self.queued.load(Ordering::SeqCst),
            active: self.active.load(Ordering::SeqCst),
            completed: self.completed.load(Ordering::SeqCst),
            failed: self.failed.load(Ordering::SeqCst),
            peak_active: self.peak_active.load(Ordering::SeqCst),
        }
    }

    /// Поточний стан конкретної задачі (`None`, якщо такого id немає).
    pub fn task_state(&self, id: TaskId) -> Option<TaskState> {
        self.states().get(&id).copied()
    }

    fn is_idle(&self) -> bool {
        // Порядок важливий: спочатку queued, потім active (див. on_start).
        self.queued.load(Ordering::SeqCst) == 0 && self.active.load(Ordering::SeqCst) == 0
    }

    fn notify_idle(&self) {
        // Беремо м'ютекс перед notify, щоб сповіщення не потрапило
        // між перевіркою умови та засинанням у wait_idle.
        let _gate = self
            .idle_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.idle_cv.notify_all();
    }

    /// Блокує виклик, доки в пулі не залишиться ні задач у черзі, ні виконуваних.
    /// Повертає `true`, якщо пул став вільним до завершення таймауту.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let gate = self
            .idle_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (_gate, result) = self
            .idle_cv
            .wait_timeout_while(gate, timeout, |_| !self.is_idle())
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        !result.timed_out() || self.is_idle()
    }
}
