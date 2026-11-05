use std::fmt;

/// Задача — довільне замикання, яке можна безпечно передати в інший потік.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// Унікальний ідентифікатор задачі, який повертає `ThreadPool::execute`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub usize);

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Стан задачі в її життєвому циклі.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Задача прийнята й очікує в черзі.
    Pending,
    /// Задачу виконує один із потоків-виконавців.
    Running,
    /// Задача завершилась успішно.
    Completed,
    /// Задача завершилась панікою.
    Failed,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            TaskState::Pending => "Pending",
            TaskState::Running => "Running",
            TaskState::Completed => "Completed",
            TaskState::Failed => "Failed",
        };
        write!(f, "{text}")
    }
}

/// Елемент черги: ідентифікатор разом із самою задачею.
pub struct Job {
    pub id: TaskId,
    pub task: Task,
}
