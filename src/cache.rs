use std::sync::LazyLock;

use rusqlite::{Connection, params};
use tokio::sync::Mutex;

/// Кешированный трек (file_id из Telegram).
#[derive(Debug, Clone)]
pub struct CachedTrack {
    pub file_id: String,
    pub artist: String,
    pub title: String,
    pub duration_sec: Option<u32>,
    pub source: String,
}

/// Потокобезопасное соединение с SQLite.
static DB: LazyLock<Mutex<Option<Connection>>> = LazyLock::new(|| Mutex::new(None));

const DB_PATH: &str = "track_cache.db";

/// Инициализирует БД и создаёт таблицу.
pub async fn init() -> anyhow::Result<()> {
    let conn = tokio::task::spawn_blocking(|| {
        let conn = Connection::open(DB_PATH)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS track_cache (
                query       TEXT PRIMARY KEY,
                file_id     TEXT NOT NULL,
                artist      TEXT NOT NULL,
                title       TEXT NOT NULL,
                duration_sec INTEGER,
                source      TEXT NOT NULL,
                created_at  INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_created ON track_cache(created_at);

            CREATE TABLE IF NOT EXISTS user_settings (
                user_id     INTEGER PRIMARY KEY,
                show_lyrics INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS search_cache (
                query       TEXT NOT NULL,
                source      TEXT NOT NULL,
                results     TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                PRIMARY KEY (query, source)
            );
            CREATE INDEX IF NOT EXISTS idx_search_created ON search_cache(created_at);",
        )?;

        Ok::<Connection, anyhow::Error>(conn)
    })
    .await??;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM track_cache", [], |r| r.get(0))?;
    log::info!("Кеш треков: {count} записей в БД");

    *DB.lock().await = Some(conn);

    // Очищаем устаревший кеш поиска
    cleanup_search_cache().await;

    Ok(())
}

/// Ищет трек в кеше по нормализованному запросу.
pub async fn get(query: &str) -> Option<CachedTrack> {
    let key = normalize(query);
    let db = DB.lock().await;
    let conn = db.as_ref()?;

    conn.query_row(
        "SELECT file_id, artist, title, duration_sec, source FROM track_cache WHERE query = ?1",
        params![key],
        |row| {
            Ok(CachedTrack {
                file_id: row.get(0)?,
                artist: row.get(1)?,
                title: row.get(2)?,
                duration_sec: row.get(3)?,
                source: row.get(4)?,
            })
        },
    )
    .ok()
}

/// Сохраняет file_id трека в кеш.
pub async fn save(
    query: &str,
    file_id: &str,
    artist: &str,
    title: &str,
    duration_sec: Option<u32>,
    source: &str,
) {
    let key = normalize(query);
    let file_id = file_id.to_string();
    let artist = artist.to_string();
    let title = title.to_string();
    let source = source.to_string();
    let now = epoch_secs() as i64;

    let db = DB.lock().await;
    let Some(conn) = db.as_ref() else { return };

    let result = conn.execute(
        "INSERT OR REPLACE INTO track_cache (query, file_id, artist, title, duration_sec, source, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![key, file_id, artist, title, duration_sec.map(|d| d as i64), source, now],
    );

    if let Err(e) = result {
        log::warn!("Ошибка записи в кеш: {e}");
    }
}

/// Количество записей в кеше.
pub async fn count() -> usize {
    let db = DB.lock().await;
    let Some(conn) = db.as_ref() else { return 0 };
    conn.query_row("SELECT COUNT(*) FROM track_cache", [], |r| r.get::<_, i64>(0))
        .unwrap_or(0) as usize
}

/// Настройки пользователя.
#[derive(Debug, Clone)]
pub struct UserSettings {
    pub show_lyrics: bool,
}

impl Default for UserSettings {
    fn default() -> Self {
        Self { show_lyrics: false }
    }
}

/// Получает настройки пользователя (default если не заданы).
pub async fn get_user_settings(user_id: u64) -> UserSettings {
    let db = DB.lock().await;
    let Some(conn) = db.as_ref() else {
        return UserSettings::default();
    };

    conn.query_row(
        "SELECT show_lyrics FROM user_settings WHERE user_id = ?1",
        params![user_id as i64],
        |row| {
            Ok(UserSettings {
                show_lyrics: row.get::<_, i64>(0)? != 0,
            })
        },
    )
    .unwrap_or_default()
}

/// Переключает настройку show_lyrics и возвращает новое значение.
pub async fn toggle_lyrics(user_id: u64) -> bool {
    let db = DB.lock().await;
    let Some(conn) = db.as_ref() else { return false };

    // UPSERT: вставляем или переключаем
    let result = conn.execute(
        "INSERT INTO user_settings (user_id, show_lyrics) VALUES (?1, 1)
         ON CONFLICT(user_id) DO UPDATE SET show_lyrics = 1 - show_lyrics",
        params![user_id as i64],
    );

    if let Err(e) = result {
        log::warn!("Ошибка toggle_lyrics: {e}");
        return false;
    }

    // Читаем текущее значение
    conn.query_row(
        "SELECT show_lyrics FROM user_settings WHERE user_id = ?1",
        params![user_id as i64],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0) != 0
}

// ====== Кеш поисковых запросов (30 дней TTL) ======

const SEARCH_TTL_SECS: i64 = 30 * 24 * 3600; // 30 дней

/// Кешированный результат поиска.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CachedSearchResult {
    pub artist: String,
    pub title: String,
    pub duration_sec: Option<u32>,
    pub platform: String,
}

/// Получает кешированные результаты поиска.
pub async fn get_search(query: &str, source: &str) -> Option<Vec<CachedSearchResult>> {
    let key = normalize(query);
    let source = source.to_string();
    let cutoff = epoch_secs() as i64 - SEARCH_TTL_SECS;
    let db = DB.lock().await;
    let conn = db.as_ref()?;

    let json: String = conn.query_row(
        "SELECT results FROM search_cache WHERE query = ?1 AND source = ?2 AND created_at > ?3",
        params![key, source, cutoff],
        |row| row.get(0),
    ).ok()?;

    serde_json::from_str(&json).ok()
}

/// Сохраняет результаты поиска в кеш.
pub async fn save_search(query: &str, source: &str, results: &[CachedSearchResult]) {
    let key = normalize(query);
    let source = source.to_string();
    let now = epoch_secs() as i64;
    let json = match serde_json::to_string(results) {
        Ok(j) => j,
        Err(_) => return,
    };

    let db = DB.lock().await;
    let Some(conn) = db.as_ref() else { return };

    let _ = conn.execute(
        "INSERT OR REPLACE INTO search_cache (query, source, results, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![key, source, json, now],
    );
}

/// Очищает устаревшие записи поиска.
pub async fn cleanup_search_cache() {
    let cutoff = epoch_secs() as i64 - SEARCH_TTL_SECS;
    let db = DB.lock().await;
    let Some(conn) = db.as_ref() else { return };
    let deleted = conn.execute(
        "DELETE FROM search_cache WHERE created_at <= ?1",
        params![cutoff],
    ).unwrap_or(0);
    if deleted > 0 {
        log::info!("Очищено {deleted} устаревших записей из кеша поиска");
    }
}

/// Нормализует запрос для ключа: lowercase, trim, убираем лишние пробелы.
fn normalize(query: &str) -> String {
    query
        .trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
