use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;

/// Пул токенов для сервиса. Ротация round-robin.
pub struct TokenPool {
    tokens: Vec<String>,
    index: AtomicUsize,
}

impl TokenPool {
    /// Создаёт пул из переменной окружения. Токены через запятую.
    fn from_env(var: &str) -> Self {
        let tokens: Vec<String> = std::env::var(var)
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Self {
            tokens,
            index: AtomicUsize::new(0),
        }
    }

    /// Возвращает следующий токен (round-robin). None если пул пуст.
    pub fn next(&self) -> Option<&str> {
        if self.tokens.is_empty() {
            return None;
        }
        let idx = self.index.fetch_add(1, Ordering::Relaxed) % self.tokens.len();
        Some(&self.tokens[idx])
    }

    /// Есть ли хотя бы один токен.
    pub fn is_available(&self) -> bool {
        !self.tokens.is_empty()
    }

    /// Количество токенов в пуле.
    pub fn count(&self) -> usize {
        self.tokens.len()
    }
}

// Глобальные пулы
pub static VK: LazyLock<TokenPool> = LazyLock::new(|| TokenPool::from_env("VK_TOKEN"));
pub static SC: LazyLock<TokenPool> = LazyLock::new(|| TokenPool::from_env("SC_OAUTH_TOKEN"));
pub static GENIUS: LazyLock<TokenPool> = LazyLock::new(|| TokenPool::from_env("GENIUS_TOKEN"));

/// Логирует состояние пулов при старте.
pub fn log_status() {
    log_pool("VK_TOKEN", &VK);
    log_pool("SC_OAUTH_TOKEN", &SC);
    log_pool("GENIUS_TOKEN", &GENIUS);
}

fn log_pool(name: &str, pool: &TokenPool) {
    let count = pool.count();
    if count == 0 {
        log::info!("{name}: не задан");
    } else if count == 1 {
        log::info!("{name}: 1 токен");
    } else {
        log::info!("{name}: {count} токенов (ротация)");
    }
}
