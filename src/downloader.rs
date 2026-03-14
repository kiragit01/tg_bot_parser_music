use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InputFile};
use tokio::fs;
use tokio::process::Command;

use crate::models::Track;
use crate::vk;

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

    let output = Command::new(ytdlp_bin())
        .arg("--version")
        .output()
        .await
        .context("Не удалось запустить yt-dlp")?;
    log::info!("yt-dlp версия: {}", String::from_utf8_lossy(&output.stdout).trim());

    if vk_token().is_some() {
        log::info!("VK_TOKEN найден — VK Music как основной источник");
    } else {
        log::warn!("VK_TOKEN не задан — только YouTube (без цензуры не гарантировано). Добавь VK_TOKEN в .env");
    }

    Ok(())
}

/// Ищет трек на YouTube через yt-dlp (fallback).
async fn download_from_youtube(track: &Track, output_path: &PathBuf) -> Result<()> {
    if output_path.exists() {
        fs::remove_file(output_path).await.ok();
    }

    let query = track.search_query();

    let mut args = vec![
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

    args.push(format!("ytsearch1:{query}"));

    let output = Command::new(ytdlp_bin())
        .args(&args)
        .output()
        .await
        .context("Не удалось запустить yt-dlp")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("YouTube: {}", stderr.trim());
    }

    if !output_path.exists() {
        bail!("YouTube: файл не создан для {query}");
    }

    Ok(())
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

/// Ищет трек, скачивает и отправляет в Telegram.
/// Приоритет: VK → YouTube.
pub async fn find_and_send_track(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
) -> Result<()> {
    let filename = format!(
        "{} - {}.mp3",
        sanitize_filename(&track.artist),
        sanitize_filename(&track.title)
    );
    let output_path = PathBuf::from(OUTPUT_DIR).join(&filename);

    // 1. VK (основной)
    let mut source = "?";
    if let Some(token) = vk_token() {
        match download_from_vk(&token, track, &output_path).await {
            Ok(()) => {
                source = "VK";
            }
            Err(e) => {
                log::debug!("VK не сработал для {}: {e:#}", track.search_query());
            }
        }
    }

    // 2. YouTube (fallback)
    if source == "?" {
        download_from_youtube(track, &output_path).await?;
        source = "YT";
    }

    let audio_bytes = fs::read(&output_path)
        .await
        .context("Не удалось прочитать скачанный файл")?;

    if audio_bytes.len() > 50 * 1024 * 1024 {
        fs::remove_file(&output_path).await.ok();
        bail!("Файл слишком большой для Telegram (>50 MB)");
    }

    let input_file = InputFile::memory(audio_bytes).file_name(filename);

    bot.send_audio(chat_id, input_file)
        .title(&track.title)
        .performer(&track.artist)
        .await
        .context("Не удалось отправить аудио в Telegram")?;

    fs::remove_file(&output_path).await.ok();

    log::info!("[{source}] Отправлен: {}", track.search_query());
    Ok(())
}

/// С одной повторной попыткой.
pub async fn find_and_send_with_retry(
    bot: &Bot,
    chat_id: ChatId,
    track: &Track,
) -> Result<()> {
    match find_and_send_track(bot, chat_id, track).await {
        Ok(()) => Ok(()),
        Err(first_err) => {
            log::warn!("Попытка 1 не удалась для {}: {first_err:#}", track.search_query());
            find_and_send_track(bot, chat_id, track)
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
