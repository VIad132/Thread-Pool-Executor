# Thread Pool Executor

Легкий менеджер пулу потоків мовою Rust (курсова робота з дисципліни «Системне програмування», варіант 29).
Використовує лише стандартну бібліотеку, тому однаково працює у Windows, macOS і Linux.

## Можливості
- фіксована кількість потоків-виконавців (обмеження одночасних задач);
- додавання задач у будь-який момент (`execute` повертає `TaskId`);
- моніторинг: стани задач `Pending / Running / Completed / Failed` та лічильники пулу;
- ізоляція панік у задачах (`catch_unwind`) — потік продовжує працювати;
- коректне завершення (`shutdown` / `Drop`): черга дообробляється, потоки приєднуються;
- обробка помилок: `PoolError` (нульовий розмір, збій створення потоку, пул завершено), валідація CLI.

## Встановлення
Потрібен Rust 1.74+ (https://rustup.rs).

```bash
git clone <URL_РЕПОЗИТОРІЮ>
cd thread_pool_executor
cargo build --release
```

## Запуск
```bash
cargo run --release                       # демонстрація (4 потоки, 8 задач, паніка кожної 3-ї)
cargo run --release -- run --threads 2 --tasks 20 --delay-ms 100 --panic-every 0
cargo run --release -- bench --tasks 16 --delay-ms 100
cargo run --release -- --help
```

## Тести
```bash
cargo test
```

## Структура
```
src/lib.rs      публічний інтерфейс бібліотеки
src/pool.rs     ThreadPool: створення, execute, shutdown, wait_idle
src/worker.rs   Worker: цикл потоку, catch_unwind
src/task.rs     Task, TaskId, TaskState, Job
src/monitor.rs  Metrics: атомарні лічильники, реєстр станів, wait_idle
src/error.rs    PoolError
src/main.rs     CLI (run / bench)
tests/          інтеграційні тести
```
