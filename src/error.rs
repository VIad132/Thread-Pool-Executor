use std::fmt;
use std::io;

/// Помилки, які можуть виникнути під час роботи пулу потоків.
#[derive(Debug)]
pub enum PoolError {
    /// Спроба створити пул без жодного потоку-виконавця.
    ZeroSize,
    /// Операційна система не змогла створити потік.
    ThreadSpawn(io::Error),
    /// Пул уже завершив роботу і не приймає нових задач.
    ShutDown,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolError::ZeroSize => write!(f, "розмір пулу має бути більшим за нуль"),
            PoolError::ThreadSpawn(e) => write!(f, "не вдалося створити потік ОС: {e}"),
            PoolError::ShutDown => {
                write!(f, "пул завершив роботу, нові задачі не приймаються")
            }
        }
    }
}

impl std::error::Error for PoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PoolError::ThreadSpawn(e) => Some(e),
            _ => None,
        }
    }
}
