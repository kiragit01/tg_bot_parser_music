use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use teloxide::prelude::*;
use teloxide::types::{ChatId, InputFile};
use tokio::fs;
use tokio::process::Command;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::models::Track;
use crate::vk;
use crate::ym;

/// Глобальный таймстамп, до которого нельзя слать (rate limit).
static RATE_LIMIT_UNTIL: AtomicU64 = AtomicU64::new(0);

const LIBS_DIR: &str = "libs";
const OUTPUT_DIR: &str = "downloads";
const COOKIES_FILE: &str = "cookies.txt";

fn ytdlp_bin() -> PathBuf {
    PathBuf::from(LIBS_DIR).join(if cfg!(windows) { "yt-dlp.exe" } else { "yt-dlp" })
}

fn ffmpeg_bin() -> PathBuf {
    PathBuf::from(LIBS_DIR).join(if cfg!(windows) { "ffmpeg.exe" } else { "ffmpeg" })
}

fn has_cookies() -> bool {
    PathBuf::from(COOKIES_FILE).exists()
}

fn vk_token() -> Option<String> {
    std::env::var("VK_TOKEN").ok().filter(|t| !t.is_empty())
}

fn sc_oauth_token() -> Option<String> {
    std::env::var("SC_OAUTH_TOKEN").ok().filter(|t| !t.is_empty())
}

/// Скачивает бинарники yt-dlp и ffmpeg при первом запуске.
pub async fn init() -> Result<()> {
    fs::create_dir_all(LIBS_DIR).await?;
    fs::create_dir_all(OUTPUT_DIR).await?;

    if !ytdlp_bin().exists() || !ffmpeg_bin().exists() {
        log::info!("Скачиваю yt-dlp и ffmpeg...");
        let _dl = yt_dlp::Downloader::with_new_binaries(LIBS_DIR, OUTPUT_DIR)
            .await
            .context("Не удалось скачать yt-dlp/ffmpeg")?;
        log::info!("Бинарники скачаны");
    }

    // Обновляем yt-dlp до последней версии (фиксы SoundCloud client_id и т.д.)
    log::info!("Обновляю yt-dlp...");
    let update = Command::new(ytdlp_bin())
        .arg("-U")
        .output()
        .await;
    match update {
        Ok(out) => {
            let msg = String::from_utf8_lossy(&out.stdout);
            log::info!("yt-dlp update: {}", msg.trim());
        }
        Err(e) => log::warn!("Не удалось обновить yt-dlp: {e}"),
    }

    let output = Command::new(ytdlp_bin())
        .arg("--version")
        .output()
        .await
        .context("Не удалось запустить yt-dlp")?;
    log::info!("yt-dlp версия: {}", String::from_utf8_lossy(&output.stdout).trim());

    if vk_token().is_some() {
        log::info!("VK_TOKEN найден — VK как основной источник");
    }
    if sc_oauth_token().is_some() {
        log::info!("SC_OAUTH_TOKEN найден — SoundCloud с авторизацией");
    }

    Ok(())
}

/// Скачивает через yt-dlp с указанным поисковым префиксом.
/// `search_prefix` — "ytsearch1" или "scsearch1" (без двоеточия).
async fn download_via_ytdlp(track: &Track, output_path: &PathBuf, search_prefix: &str) -> Result<()> {
    if output_path.exists() {
        fs::remove_file(output_path).await.ok();
    }

    let query = track.search_query();
    if query.trim().is_empty() {
        bail!("{search_prefix}: пустой запрос");
    }

    let mut args = vec![
        "--default-search".to_string(), search_prefix.to_string(),
        "-x".to_string(),
        "--audio-format".to_string(), "mp3".to_string(),
        "--audio-quality".to_string(), "5".to_string(),
        "--ffmpeg-location".to_string(), ffmpeg_bin().to_string_lossy().to_string(),
        "--no-playlist".to_string(),
        "--no-warnings".to_string(),
        "--no-check-certificates".to_string(),
        "-o".to_string(), output_path.to_string_lossy().to_string(),
    ];

    if has_cookies() {
        args.push("--cookies".to_string());
        args.push(COOKIES_FILE.to_string());
    }

    // SoundCloud OAuth токен для авторизации (решает 401 ошибки)
    if search_prefix.starts_with("sc") {
        if let Some(token) = sc_oauth_token() {
            args.push("--add-header".to_string());
            args.push(format!("Authorization:OAuth {token}"));
        }
    }

    // Передаём запрос как обычный текст, --default-search превратит его в поиск
    args.push(query.clone());

    let output = Command::new(ytdlp_bin())
        .args(&args)
        .output()
        .await
        .context("Не удалось запустить yt-dlp")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{search_prefix}: {}", stderr.trim());
    }

    if !output_path.exists() {
        bail!("{search_prefix}: файл не создан для {query}");
    }

    // Проверяем размер файла — превью обычно < 500KB
    let meta = fs::metadata(output_path).await?;
    if meta.len() < 500_000 && search_prefix.starts_with("sc") {
        fs::remove_file(output_path).await.ok();
        bail!("SC: похоже на 30с превью для {query} ({}KB), пропускаю", meta.len() / 1024);
    }

    Ok(())
}

/// Автоматический выбор источника: YM → SC → VK → YT.
async fn download_auto(track: &Track, output_path: &PathBuf) -> Result<&'static str> {
    // 1. Яндекс.Музыка (оригинальное качество, без цензуры)
    if ym::is_available() {
        match ym::search_and_download(track, output_path).await {
            Ok(()) => return Ok("YM"),
            Err(e) => log::debug!("YM не сработал для {}: {e:#}", track.search_query()),
        }
    }

    // 2. SoundCloud
    match download_via_ytdlp(track, output_path, "scsearch1").await {
        Ok(()) => return Ok("SC"),
        Err(e) => log::debug!("SC не сработал для {}: {e:#}", track.search_query()),
    }

    // 3. VK (если токен есть)
    if let Some(token) = vk_token() {
        match download_from_vk(&token, track, output_path).await {
            Ok(()) => return Ok("VK"),
            Err(e) => log::debug!("VK не сработал для {}: {e:#}", track.search_query()),
        }
    }

    // 4. YouTube (финальный fallback)
    download_via_ytdlp(track, output_path, "ytsearch1").await?;
    Ok("YT")
}

/// Ищет трек в VK, скачивает mp3 напрямую.
async fn download_from_vk(token: &str, track: &Track, output_path: &PathBuf) -> Result<()> {
    let audio = vk::search_track(token, track).await?;
    let bytes = vk::download_audio(&audio.url).await?;

    if bytes.is_empty() {
        bail!("VK: пустой файл для {}", track.search_query());
    }

    fs::write(output_path, &bytes).await.context("Запись mp3")?;
    Ok(())
}

/// Источник скачивания.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Автоматический выбор: YM → SC → VK → YT
    Auto,
    /// Яндекс.Музыка (оригинальное качество)
    YandexMusic,
    /// Только SoundCloud
    SoundCloud,
    /// Только YouTube
    YouTube,
    /// Только VK
    Vk,
}

impl Source {
    pub fn label(self) -> &'static str {
        match self {
            Source::Auto => "авто",
            Source::YandexMusic => "Яндекс.Музыка",
            Source::SoundCloud => "SoundCloud",
            Source::YouTube => "YouTube",
            Source::Vk => "VK",
        }
    }
}

pub fn ym_available() -> bool {
    ym::is_available()
}

pub fn vk_available() -> bool {
    vk_token().is_some()
}

/// Ищет трек, скачивает и отправляет в Telegram.
/// Приоритет по умолчанию: SC → VK → YT.
pub async fn find_and_send_track(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
) -> Result<()> {
    find_and_send_track_with_source(bot, chat_id, track, Source::Auto, CancellationToken::new()).await
}

pub async fn find_and_send_track_with_source(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
    source: Source,
    cancel: CancellationToken,
) -> Result<()> {
    let filename = format!(
        "{} - {}.mp3",
        sanitize_filename(&track.artist),
        sanitize_filename(&track.title)
    );
    let output_path = PathBuf::from(OUTPUT_DIR).join(&filename);

    let result = match source {
        Source::Auto => download_auto(track, &output_path).await,
        Source::YandexMusic => ym::search_and_download(track, &output_path)
            .await
            .map(|()| "YM"),
        Source::SoundCloud => download_via_ytdlp(track, &output_path, "scsearch1")
            .await
            .map(|()| "SC"),
        Source::YouTube => download_via_ytdlp(track, &output_path, "ytsearch1")
            .await
            .map(|()| "YT"),
        Source::Vk => {
            let token = vk_token().context("VK_TOKEN не задан")?;
            download_from_vk(&token, track, &output_path)
                .await
                .map(|()| "VK")
        }
    };

    let source_tag = result?;

    // Проверяем отмену после скачивания, но до отправки
    if cancel.is_cancelled() {
        fs::remove_file(&output_path).await.ok();
        bail!("Отменено");
    }

    let audio_bytes = fs::read(&output_path)
        .await
        .context("Не удалось прочитать скачанный файл")?;

    if audio_bytes.len() > 50 * 1024 * 1024 {
        fs::remove_file(&output_path).await.ok();
        bail!("Файл слишком большой для Telegram (>50 MB)");
    }

    // Проверяем отмену перед отправкой в Telegram
    if cancel.is_cancelled() {
        fs::remove_file(&output_path).await.ok();
        bail!("Отменено");
    }

    // Обложка — пробуем скачать
    let thumb = fetch_thumbnail(track).await;

    send_audio_with_rate_limit(bot, chat_id, track, audio_bytes, &filename, thumb).await?;

    fs::remove_file(&output_path).await.ok();

    log::info!("[{source_tag}] Отправлен: {}", track.search_query());
    Ok(())
}

/// Скачивает обложку трека (ЯМ → yt-dlp thumbnail).
async fn fetch_thumbnail(track: &Track) -> Option<Vec<u8>> {
    // Пробуем ЯМ
    if ym::is_available() {
        if let Ok(results) = ym::search_tracks(&track.search_query(), 1).await {
            if let Some(r) = results.first() {
                if let Some(ref cover) = r.cover_url {
                    if let Ok(resp) = reqwest::get(cover).await {
                        if let Ok(bytes) = resp.bytes().await {
                            if !bytes.is_empty() {
                                return Some(bytes.to_vec());
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Отправка аудио с ожиданием rate limit.
async fn send_audio_with_rate_limit(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
    audio_bytes: Vec<u8>,
    filename: &str,
    thumbnail: Option<Vec<u8>>,
) -> Result<()> {
    for attempt in 0..3 {
        wait_for_rate_limit().await;

        let input_file = InputFile::memory(audio_bytes.clone()).file_name(filename.to_string());

        let mut req = bot.send_audio(chat_id, input_file)
            .title(&track.title)
            .performer(&track.artist);

        if let Some(ref thumb_bytes) = thumbnail {
            req = req.thumbnail(InputFile::memory(thumb_bytes.clone()).file_name("cover.jpg"));
        }

        match req.await
        {
            Ok(_) => return Ok(()),
            Err(teloxide::RequestError::RetryAfter(seconds)) => {
                let secs = seconds.seconds();
                log::warn!(
                    "Rate limit: ждём {secs}с перед повтором для {}",
                    track.search_query()
                );
                let until = epoch_secs() + secs as u64;
                RATE_LIMIT_UNTIL.fetch_max(until, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(secs as u64)).await;
            }
            Err(e) => {
                if attempt == 2 {
                    bail!("Не удалось отправить аудио в Telegram: {e}");
                }
                log::warn!("Ошибка отправки (попытка {}): {e}", attempt + 1);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    bail!("Не удалось отправить после 3 попыток")
}

async fn wait_for_rate_limit() {
    let until = RATE_LIMIT_UNTIL.load(Ordering::Relaxed);
    let now = epoch_secs();
    if until > now {
        let wait = until - now;
        log::info!("Ожидание rate limit: {wait}с");
        tokio::time::sleep(Duration::from_secs(wait)).await;
    }
}

fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// С одной повторной попыткой.
pub async fn find_and_send_with_retry(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
) -> Result<()> {
    find_and_send_with_retry_source(bot, chat_id, track, Source::Auto, CancellationToken::new()).await
}

pub async fn find_and_send_with_retry_source(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
    source: Source,
    cancel: CancellationToken,
) -> Result<()> {
    match find_and_send_track_with_source(bot, chat_id, track, source, cancel.clone()).await {
        Ok(()) => Ok(()),
        Err(first_err) => {
            if cancel.is_cancelled() {
                bail!("Отменено");
            }
            log::warn!("Попытка 1 не удалась для {}: {first_err:#}", track.search_query());
            find_and_send_track_with_source(bot, chat_id, track, source, cancel)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Не удалось скачать {} после 2 попыток: {e:#}",
                        track.search_query()
                    )
                })
        }
    }
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}
