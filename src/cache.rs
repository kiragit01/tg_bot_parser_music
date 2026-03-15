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
            CREATE INDEX IF NOT EXISTS idx_created ON track_cache(created_at);",
        )?;

        // Чистим старые записи (> 30 дней) при старте
        let cutoff = epoch_secs().saturating_sub(30 * 24 * 3600);
        conn.execute("DELETE FROM track_cache WHERE created_at < ?1", params![cutoff as i64])?;

        Ok::<Connection, anyhow::Error>(conn)
    })
    .await??;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM track_cache", [], |r| r.get(0))?;
    log::info!("Кеш треков: {count} записей в БД");

    *DB.lock().await = Some(conn);
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
