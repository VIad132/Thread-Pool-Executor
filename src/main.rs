use std::env;
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};
use thread_pool_executor::{PoolError, ThreadPool};

const MAX_THREADS: usize = 1024;
const MAX_TASKS: usize = 1_000_000;
const MAX_DELAY_MS: u64 = 60_000;
const SHOWN_TASKS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Run,
    Bench,
}

#[derive(Debug)]
struct Config {
    command: Command,
    threads: usize,
    tasks: usize,
    delay_ms: u64,
    panic_every: usize,
}

fn print_usage() {
    println!("Thread Pool Executor — демонстраційний CLI");
    println!();
    println!("Використання:");
    println!("  thread_pool_executor [run|bench] [параметри]");
    println!();
    println!("Команди:");
    println!("  run     запустити демонстрацію з моніторингом (за замовчуванням)");
    println!("  bench   виміряти час виконання для пулів із 1, 2, 4 і 8 потоків");
    println!();
    println!("Параметри:");
    println!("  --threads N      кількість потоків у пулі, 1..={MAX_THREADS} (за замовчуванням 4)");
    println!("  --tasks N        кількість задач, 1..={MAX_TASKS} (за замовчуванням 8)");
    println!("  --delay-ms MS    тривалість імітації роботи однієї задачі, 0..={MAX_DELAY_MS} мс (за замовчуванням 200)");
    println!("  --panic-every K  кожна K-та задача завершується панікою, 0 — без паніки (за замовчуванням 3)");
    println!("  -h, --help       показати цю довідку");
}

fn parse_value<T: std::str::FromStr>(args: &[String], i: usize, name: &str) -> Result<T, String> {
    let raw = args
        .get(i)
        .ok_or_else(|| format!("для {name} потрібно вказати значення"))?;
    raw.parse::<T>()
        .map_err(|_| format!("некоректне значення '{raw}' для {name}: очікується невід'ємне ціле число"))
}

/// `Ok(None)` означає, що користувач попросив довідку.
fn parse_args(args: &[String]) -> Result<Option<Config>, String> {
    let mut cfg = Config {
        command: Command::Run,
        threads: 4,
        tasks: 8,
        delay_ms: 200,
        panic_every: 3,
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" | "help" => return Ok(None),
            "run" => cfg.command = Command::Run,
            "bench" => cfg.command = Command::Bench,
            "--threads" => {
                i += 1;
                cfg.threads = parse_value(args, i, "--threads")?;
            }
            "--tasks" => {
                i += 1;
                cfg.tasks = parse_value(args, i, "--tasks")?;
            }
            "--delay-ms" => {
                i += 1;
                cfg.delay_ms = parse_value(args, i, "--delay-ms")?;
            }
            "--panic-every" => {
                i += 1;
                cfg.panic_every = parse_value(args, i, "--panic-every")?;
            }
            other => return Err(format!("невідомий аргумент: {other}")),
        }
        i += 1;
    }

    if cfg.threads == 0 || cfg.threads > MAX_THREADS {
        return Err(format!("--threads має бути в межах 1..={MAX_THREADS}"));
    }
    if cfg.tasks == 0 || cfg.tasks > MAX_TASKS {
        return Err(format!("--tasks має бути в межах 1..={MAX_TASKS}"));
    }
    if cfg.delay_ms > MAX_DELAY_MS {
        return Err(format!("--delay-ms має бути в межах 0..={MAX_DELAY_MS}"));
    }
    Ok(Some(cfg))
}

fn expected_failures(tasks: usize, panic_every: usize) -> usize {
    if panic_every == 0 {
        0
    } else {
        tasks / panic_every
    }
}

/// Демонстрація: повертає `Ok(true)`, якщо підсумкові лічильники збіглися з очікуваними.
fn run_demo(cfg: &Config) -> Result<bool, PoolError> {
    let mut pool = ThreadPool::new(cfg.threads)?;
    println!("=== Thread Pool Executor ===");
    println!(
        "Пул створено: {} потоків, задач: {}, тривалість задачі: {} мс, паніка кожної {}-ї",
        pool.size(),
        cfg.tasks,
        cfg.delay_ms,
        cfg.panic_every
    );

    let delay = Duration::from_millis(cfg.delay_ms);
    let mut ids = Vec::with_capacity(cfg.tasks);
    let started = Instant::now();

    for n in 1..=cfg.tasks {
        let should_panic = cfg.panic_every > 0 && n % cfg.panic_every == 0;
        let id = pool.execute(move || {
            let worker = thread::current().name().unwrap_or("?").to_string();
            println!("[задача {n}] старт ({worker})");
            thread::sleep(delay);
            if should_panic {
                panic!("навмисна паніка задачі {n}");
            }
            println!("[задача {n}] завершено");
        })?;
        ids.push(id);
    }

    loop {
        println!("-> монітор: {}", pool.metrics());
        if pool.wait_idle(Duration::from_millis(100)) {
            break;
        }
    }

    println!("Стани перших {} задач:", SHOWN_TASKS.min(ids.len()));
    for id in ids.iter().take(SHOWN_TASKS) {
        match pool.task_state(*id) {
            Some(state) => println!("   задача {id}: {state}"),
            None => println!("   задача {id}: невідомо"),
        }
    }

    println!("Graceful shutdown...");
    pool.shutdown();
    let fin = pool.metrics();
    println!("Усі потоки завершили роботу за {:.0?}.", started.elapsed());
    println!("Підсумок: {fin}");

    let failed = expected_failures(cfg.tasks, cfg.panic_every);
    let ok = fin.failed == failed
        && fin.completed == cfg.tasks - failed
        && fin.active == 0
        && fin.queued == 0
        && fin.peak_active <= cfg.threads;
    if ok {
        println!("Самоперевірка пройдена: виконано {}, з помилкою {}.", fin.completed, fin.failed);
    }
    Ok(ok)
}

/// Вимірює час виконання однакового набору задач для різних розмірів пулу.
fn run_bench(cfg: &Config) -> Result<bool, PoolError> {
    const RUNS: u32 = 3;
    let sizes = [1usize, 2, 4, 8];
    let delay = Duration::from_millis(cfg.delay_ms);

    println!(
        "Бенчмарк: {} задач по {} мс, {} запуски на конфігурацію",
        cfg.tasks, cfg.delay_ms, RUNS
    );
    println!("{:>8} | {:>14} | {:>16}", "потоків", "середній, мс", "теоретичний, мс");

    for &size in &sizes {
        let mut total = Duration::ZERO;
        for _ in 0..RUNS {
            let pool = ThreadPool::new(size)?;
            let start = Instant::now();
            for _ in 0..cfg.tasks {
                pool.execute(move || thread::sleep(delay))?;
            }
            pool.wait_idle(Duration::from_secs(3600));
            total += start.elapsed();
        }
        let average_ms = total.as_secs_f64() * 1000.0 / f64::from(RUNS);
        let rounds = (cfg.tasks + size - 1) / size;
        let ideal_ms = rounds as u64 * cfg.delay_ms;
        println!("{:>8} | {:>14.1} | {:>16}", size, average_ms, ideal_ms);
    }
    Ok(true)
}

fn main() -> ExitCode {
    let args: Vec<String> = match env::args_os()
        .skip(1)
        .map(|a| a.into_string())
        .collect::<Result<Vec<String>, _>>()
    {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Помилка: аргументи командного рядка мають бути коректним UTF-8");
            return ExitCode::from(2);
        }
    };

    let cfg = match parse_args(&args) {
        Ok(Some(cfg)) => cfg,
        Ok(None) => {
            print_usage();
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("Помилка: {message}");
            eprintln!("Скористайтеся --help для довідки.");
            return ExitCode::from(2);
        }
    };

    let result = match cfg.command {
        Command::Run => run_demo(&cfg),
        Command::Bench => run_bench(&cfg),
    };

    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("Помилка: підсумкові лічильники не збігаються з очікуваними");
            ExitCode::from(1)
        }
        Err(e) => {
            eprintln!("Помилка пулу потоків: {e}");
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn defaults_are_applied() {
        let cfg = parse_args(&[]).unwrap().unwrap();
        assert_eq!(cfg.threads, 4);
        assert_eq!(cfg.tasks, 8);
        assert_eq!(cfg.delay_ms, 200);
        assert_eq!(cfg.panic_every, 3);
        assert_eq!(cfg.command, Command::Run);
    }

    #[test]
    fn values_are_parsed() {
        let cfg = parse_args(&args(&["bench", "--threads", "2", "--tasks", "5"]))
            .unwrap()
            .unwrap();
        assert_eq!(cfg.command, Command::Bench);
        assert_eq!(cfg.threads, 2);
        assert_eq!(cfg.tasks, 5);
    }

    #[test]
    fn invalid_input_is_rejected() {
        assert!(parse_args(&args(&["--threads", "0"])).is_err());
        assert!(parse_args(&args(&["--threads", "abc"])).is_err());
        assert!(parse_args(&args(&["--threads", "-3"])).is_err());
        assert!(parse_args(&args(&["--threads"])).is_err());
        assert!(parse_args(&args(&["--tasks", "0"])).is_err());
        assert!(parse_args(&args(&["--delay-ms", "999999"])).is_err());
        assert!(parse_args(&args(&["--unknown"])).is_err());
    }

    #[test]
    fn help_is_detected() {
        assert!(parse_args(&args(&["--help"])).unwrap().is_none());
    }

    #[test]
    fn expected_failures_are_counted() {
        assert_eq!(expected_failures(8, 3), 2);
        assert_eq!(expected_failures(8, 0), 0);
    }
}
