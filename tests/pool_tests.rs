use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use thread_pool_executor::{PoolError, TaskState, ThreadPool};

const LONG: Duration = Duration::from_secs(10);

#[test]
fn zero_size_is_rejected() {
    assert!(matches!(ThreadPool::new(0), Err(PoolError::ZeroSize)));
}

#[test]
fn pool_reports_its_size() {
    let pool = ThreadPool::new(3).unwrap();
    assert_eq!(pool.size(), 3);
}

#[test]
fn all_tasks_are_executed() {
    let pool = ThreadPool::new(4).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..20 {
        let counter = Arc::clone(&counter);
        pool.execute(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    }

    assert!(pool.wait_idle(LONG));
    assert_eq!(counter.load(Ordering::SeqCst), 20);
    let stats = pool.metrics();
    assert_eq!(stats.completed, 20);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.active, 0);
    assert_eq!(stats.queued, 0);
}

#[test]
fn concurrency_never_exceeds_pool_size() {
    let pool = ThreadPool::new(3).unwrap();
    let current = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));

    for _ in 0..12 {
        let current = Arc::clone(&current);
        let maximum = Arc::clone(&maximum);
        pool.execute(move || {
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(20));
            current.fetch_sub(1, Ordering::SeqCst);
        })
        .unwrap();
    }

    assert!(pool.wait_idle(LONG));
    let observed = maximum.load(Ordering::SeqCst);
    assert!(observed >= 1 && observed <= 3, "спостережена паралельність: {observed}");
    assert!(pool.metrics().peak_active <= 3);
}

#[test]
fn panic_does_not_kill_the_worker() {
    // Один потік: якщо він загине після паніки, друга задача ніколи не виконається.
    let pool = ThreadPool::new(1).unwrap();
    let flag = Arc::new(AtomicUsize::new(0));

    let bad = pool.execute(|| panic!("навмисна паніка для тесту")).unwrap();
    let flag_clone = Arc::clone(&flag);
    let good = pool
        .execute(move || {
            flag_clone.store(1, Ordering::SeqCst);
        })
        .unwrap();

    assert!(pool.wait_idle(LONG));
    assert_eq!(flag.load(Ordering::SeqCst), 1);
    assert_eq!(pool.task_state(bad), Some(TaskState::Failed));
    assert_eq!(pool.task_state(good), Some(TaskState::Completed));

    let stats = pool.metrics();
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.active, 0);
}

#[test]
fn task_states_follow_the_lifecycle() {
    let pool = ThreadPool::new(1).unwrap();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let first = pool
        .execute(move || {
            let _ = release_rx.recv();
        })
        .unwrap();
    let second = pool.execute(|| {}).unwrap();

    // Чекаємо, поки єдиний потік реально почне першу задачу.
    let mut spins = 0;
    while pool.task_state(first) != Some(TaskState::Running) {
        thread::sleep(Duration::from_millis(5));
        spins += 1;
        assert!(spins < 1000, "перша задача так і не перейшла у стан Running");
    }
    assert_eq!(pool.task_state(second), Some(TaskState::Pending));
    assert_eq!(pool.metrics().queued, 1);
    assert_eq!(pool.metrics().active, 1);

    release_tx.send(()).unwrap();
    assert!(pool.wait_idle(LONG));
    assert_eq!(pool.task_state(first), Some(TaskState::Completed));
    assert_eq!(pool.task_state(second), Some(TaskState::Completed));
}

#[test]
fn graceful_shutdown_drains_the_queue() {
    let mut pool = ThreadPool::new(2).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        pool.execute(move || {
            thread::sleep(Duration::from_millis(10));
            counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    }

    pool.shutdown();

    assert_eq!(counter.load(Ordering::SeqCst), 10);
    let stats = pool.metrics();
    assert_eq!(stats.completed, 10);
    assert_eq!(stats.active, 0);
    assert_eq!(stats.queued, 0);
}

#[test]
fn execute_after_shutdown_is_an_error() {
    let mut pool = ThreadPool::new(2).unwrap();
    pool.shutdown();
    pool.shutdown(); // повторний виклик безпечний

    let result = pool.execute(|| {});
    assert!(matches!(result, Err(PoolError::ShutDown)));
    assert_eq!(pool.metrics().queued, 0);
}

#[test]
fn many_producers_share_one_pool() {
    let pool = Arc::new(ThreadPool::new(4).unwrap());
    let counter = Arc::new(AtomicUsize::new(0));

    let producers: Vec<_> = (0..4)
        .map(|_| {
            let pool = Arc::clone(&pool);
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                for _ in 0..50 {
                    let counter = Arc::clone(&counter);
                    pool.execute(move || {
                        counter.fetch_add(1, Ordering::SeqCst);
                    })
                    .unwrap();
                }
            })
        })
        .collect();

    for producer in producers {
        producer.join().unwrap();
    }

    assert!(pool.wait_idle(LONG));
    assert_eq!(counter.load(Ordering::SeqCst), 200);
    assert_eq!(pool.metrics().completed, 200);
}
