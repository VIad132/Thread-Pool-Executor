use crate::error::PoolError;
use crate::monitor::{Metrics, MetricsSnapshot};
use crate::task::{Job, TaskId, TaskState};
use crate::worker::Worker;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Пул потоків фіксованого розміру зі спільною чергою задач (FIFO).
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
    metrics: Arc<Metrics>,
    next_id: AtomicUsize,
    size: usize,
}

impl ThreadPool {
    /// Створює пул із `size` потоків-виконавців.
    ///
    /// # Errors
    /// * `PoolError::ZeroSize` — якщо `size == 0`;
    /// * `PoolError::ThreadSpawn` — якщо ОС не змогла створити потік
    ///   (вже запущені потоки при цьому коректно зупиняються).
    pub fn new(size: usize) -> Result<ThreadPool, PoolError> {
        if size == 0 {
            return Err(PoolError::ZeroSize);
        }

        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let metrics = Arc::new(Metrics::new());

        let mut pool = ThreadPool {
            workers: Vec::with_capacity(size),
            sender: Some(sender),
            metrics: Arc::clone(&metrics),
            next_id: AtomicUsize::new(0),
            size,
        };

        for id in 0..size {
            match Worker::spawn(id, Arc::clone(&receiver), Arc::clone(&metrics)) {
                Ok(worker) => pool.workers.push(worker),
                Err(e) => {
                    pool.shutdown();
                    return Err(PoolError::ThreadSpawn(e));
                }
            }
        }

        Ok(pool)
    }

    /// Створює пул за кількістю логічних процесорів (або з одним потоком,
    /// якщо визначити її не вдалося).
    pub fn with_default_size() -> Result<ThreadPool, PoolError> {
        let size = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        ThreadPool::new(size)
    }

    /// Додає задачу до черги й повертає її ідентифікатор.
    ///
    /// # Errors
    /// `PoolError::ShutDown`, якщо пул уже завершив роботу.
    pub fn execute<F>(&self, f: F) -> Result<TaskId, PoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        let sender = self.sender.as_ref().ok_or(PoolError::ShutDown)?;

        let id = TaskId(self.next_id.fetch_add(1, Ordering::SeqCst));
        self.metrics.on_submit(id);

        let job = Job {
            id,
            task: Box::new(f),
        };
        if sender.send(job).is_err() {
            self.metrics.on_submit_failed(id);
            return Err(PoolError::ShutDown);
        }
        Ok(id)
    }

    /// Кількість потоків-виконавців, заданих при створенні.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Знімок агрегованих метрик.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Поточний стан задачі за її ідентифікатором.
    pub fn task_state(&self, id: TaskId) -> Option<TaskState> {
        self.metrics.task_state(id)
    }

    /// Чекає, доки всі прийняті задачі буде виконано.
    /// `true` — пул став вільним, `false` — вичерпано таймаут.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        self.metrics.wait_idle(timeout)
    }

    /// Коректне завершення: нові задачі не приймаються, усі задачі, що вже
    /// в черзі, виконуються, після чого потоки приєднуються (`join`).
    /// Виклик можна безпечно повторювати.
    pub fn shutdown(&mut self) {
        // Закриття каналу: recv() у робітників поверне Err лише після
        // того, як буде вичерпано всі повідомлення, що лишилися в черзі.
        drop(self.sender.take());

        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                if handle.join().is_err() {
                    eprintln!("потік-виконавець {} завершився аварійно", worker.id);
                }
            }
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
