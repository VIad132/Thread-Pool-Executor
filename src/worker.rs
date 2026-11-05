use crate::monitor::Metrics;
use crate::task::Job;
use std::any::Any;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// Потік-виконавець пулу.
pub struct Worker {
    pub id: usize,
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    /// Запускає потік ОС з ім'ям `pool-worker-<id>`.
    /// Помилку створення потоку повертає викликачу замість паніки.
    pub fn spawn(
        id: usize,
        receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
        metrics: Arc<Metrics>,
    ) -> io::Result<Worker> {
        let handle = thread::Builder::new()
            .name(format!("pool-worker-{id}"))
            .spawn(move || worker_loop(id, receiver, metrics))?;

        Ok(Worker {
            id,
            thread: Some(handle),
        })
    }
}

fn worker_loop(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>, metrics: Arc<Metrics>) {
    loop {
        // М'ютекс утримується лише на час очікування наступної задачі;
        // виконання задачі відбувається вже без блокування.
        let guard = receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let received = guard.recv();
        drop(guard);

        let job = match received {
            Ok(job) => job,
            // Усі Sender закрито й черга порожня — пул завершує роботу.
            Err(_) => break,
        };

        let task_id = job.id;
        metrics.on_start(task_id);

        // Паніка в користувацькій задачі не повинна зупиняти потік.
        let result = panic::catch_unwind(AssertUnwindSafe(job.task));

        match result {
            Ok(()) => metrics.on_finish(task_id, true),
            Err(payload) => {
                eprintln!(
                    "[worker-{id}] задача {task_id} завершилася панікою: {}; потік продовжує роботу",
                    panic_message(&*payload)
                );
                metrics.on_finish(task_id, false);
            }
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "невідома причина".to_string()
    }
}
