use std::sync::Arc;

use futures::stream::{self, StreamExt};
use teloxide::prelude::*;
use teloxide::utils::command::BotCommands;
use tokio::sync::Mutex;

use crate::commands::Command;
use crate::downloader;
use crate::models::{Playlist, Track};
use crate::yandex;

type HandlerResult = ResponseResult<()>;

/// Последний распарсенный плейлист (для /downloadall).
static LAST_PLAYLIST: Mutex<Option<Playlist>> = Mutex::const_new(None);

const MAX_CONCURRENT_DOWNLOADS: usize = 3;
const PROGRESS_EVERY: usize = 10;

/// Единая точка входа для всех сообщений.
pub async fn process_message(bot: Bot, msg: Message) -> HandlerResult {
    let Some(text) = msg.text() else {
        return Ok(());
    };

    // Пробуем распарсить как команду
    let me = bot.get_me().await?;
    if let Ok(cmd) = Command::parse(text, me.username()) {
        return handle_command(&bot, &msg, cmd).await;
    }

    // Ссылка на ЯМ или iframe-код
    if yandex::is_yandex_music_url(text) {
        handle_parse_playlist(&bot, &msg, text).await?;
        return Ok(());
    }

    // Многострочный список треков
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains(" - ") || l.contains(" — "))
        .collect();
    if lines.len() > 1 {
        handle_text_tracklist(&bot, &msg, &lines).await?;
        return Ok(());
    }

    // Один трек
    if text.contains(" - ") || text.contains(" — ") {
        handle_get_track(&bot, &msg, text).await?;
        return Ok(());
    }

    bot.send_message(
        msg.chat.id,
        "🤔 Не понял. Отправь:\n\
         • Ссылку на плейлист ЯМ\n\
         • Iframe-код (кнопка «Код для вставки»)\n\
         • Текстовый список треков (Исполнитель - Название)\n\
         • /help для справки",
    )
    .await?;

    Ok(())
}

async fn handle_command(bot: &Bot, msg: &Message, cmd: Command) -> HandlerResult {
    match cmd {
        Command::Start => {
            bot.send_message(
                msg.chat.id,
                "👋 Привет! Я помогу перенести треки из Яндекс.Музыки.\n\n\
                 📌 Как использовать:\n\
                 1. Отправь ссылку на плейлист ЯМ\n\
                 2. Или iframe-код (кнопка «Код для вставки»)\n\
                 3. Или текстовый список треков (Исполнитель - Название)\n\n\
                 🔍 Один трек: /get Исполнитель - Название\n\n\
                 Треки скачиваются из YouTube через yt-dlp.",
            )
            .await?;
        }

        Command::Help => {
            bot.send_message(msg.chat.id, Command::descriptions().to_string())
                .await?;
        }

        Command::Parse(url) => {
            handle_parse_playlist(bot, msg, &url).await?;
        }

        Command::Get(query) => {
            handle_get_track(bot, msg, &query).await?;
        }

        Command::DownloadAll => {
            handle_download_all(bot, msg).await?;
        }

        Command::Status => {
            bot.send_message(msg.chat.id, "📊 Нет активных задач.")
                .await?;
        }
    }

    Ok(())
}

async fn handle_parse_playlist(bot: &Bot, msg: &Message, text: &str) -> HandlerResult {
    bot.send_message(msg.chat.id, "⏳ Парсю плейлист...").await?;

    match yandex::parse_playlist(text).await {
        Ok(playlist) => {
            if playlist.is_empty() {
                bot.send_message(msg.chat.id, "😕 Плейлист пуст или не удалось извлечь треки.")
                    .await?;
                return Ok(());
            }

            let count = playlist.len();
            bot.send_message(msg.chat.id, format!("✅ Найдено треков: {count}"))
                .await?;

            let pages = playlist.format_pages(20);
            for page in &pages {
                bot.send_message(msg.chat.id, page).await?;
            }

            *LAST_PLAYLIST.lock().await = Some(playlist);

            bot.send_message(
                msg.chat.id,
                format!(
                    "📥 Скачать все {count} треков — /downloadall\n\
                     Один трек: /get Исполнитель - Название"
                ),
            )
            .await?;
        }
        Err(e) => {
            log::error!("Ошибка парсинга: {e:#}");
            bot.send_message(msg.chat.id, format!("❌ {e}")).await?;
        }
    }

    Ok(())
}

async fn handle_download_all(bot: &Bot, msg: &Message) -> HandlerResult {
    let playlist = LAST_PLAYLIST.lock().await.clone();

    let Some(playlist) = playlist else {
        bot.send_message(
            msg.chat.id,
            "❌ Нет сохранённого плейлиста. Сначала отправь ссылку или iframe-код.",
        )
        .await?;
        return Ok(());
    };

    let count = playlist.len();
    bot.send_message(
        msg.chat.id,
        format!("📥 Скачиваю {count} треков (по {MAX_CONCURRENT_DOWNLOADS} параллельно)..."),
    )
    .await?;

    download_tracks_parallel(bot, msg.chat.id, &playlist.tracks).await
}

async fn handle_text_tracklist(bot: &Bot, msg: &Message, lines: &[&str]) -> HandlerResult {
    let tracks: Vec<Track> = lines
        .iter()
        .filter_map(|line| yandex::parse_track_text(line))
        .collect();

    if tracks.is_empty() {
        bot.send_message(msg.chat.id, "😕 Не удалось распознать треки.")
            .await?;
        return Ok(());
    }

    let count = tracks.len();
    bot.send_message(
        msg.chat.id,
        format!("✅ {count} треков. Скачиваю (по {MAX_CONCURRENT_DOWNLOADS} параллельно)..."),
    )
    .await?;

    download_tracks_parallel(bot, msg.chat.id, &tracks).await
}

async fn download_tracks_parallel(
    bot: &Bot,
    chat_id: ChatId,
    tracks: &[Track],
) -> HandlerResult {
    let total = tracks.len();
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(Mutex::new(Vec::<String>::new()));

    stream::iter(tracks.iter())
        .for_each_concurrent(MAX_CONCURRENT_DOWNLOADS, |track| {
            let bot = bot.clone();
            let done = done.clone();
            let failed = failed.clone();

            async move {
                match downloader::find_and_send_with_retry(&bot, chat_id, track).await {
                    Ok(()) => {}
                    Err(e) => {
                        log::warn!("Не скачан: {} — {e:#}", track.search_query());
                        failed.lock().await.push(track.display());
                    }
                }

                let completed = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if completed % PROGRESS_EVERY == 0 || completed == total {
                    let _ = bot
                        .send_message(chat_id, format!("📊 Прогресс: {completed}/{total}"))
                        .await;
                }
            }
        })
        .await;

    let failed_list = failed.lock().await;
    let ok_count = total - failed_list.len();

    if failed_list.is_empty() {
        bot.send_message(chat_id, format!("🎉 Готово! Все {total} треков скачаны."))
            .await?;
    } else {
        let mut summary = format!("🎉 Готово! Скачано: {ok_count}/{total}\n\n❌ Не найдены:\n");
        for (i, name) in failed_list.iter().enumerate().take(50) {
            summary.push_str(&format!("{}. {}\n", i + 1, name));
        }
        if failed_list.len() > 50 {
            summary.push_str(&format!("... и ещё {}\n", failed_list.len() - 50));
        }
        bot.send_message(chat_id, summary).await?;
    }

    Ok(())
}

async fn handle_get_track(bot: &Bot, msg: &Message, query: &str) -> HandlerResult {
    let track = if let Some(t) = yandex::parse_track_text(query) {
        t
    } else {
        Track::new("", query.trim())
    };

    bot.send_message(msg.chat.id, format!("🔍 Ищу: {}", track.search_query()))
        .await?;

    match downloader::find_and_send_with_retry(bot, msg.chat.id, &track).await {
        Ok(()) => {
            bot.send_message(msg.chat.id, "✅ Отправлен!").await?;
        }
        Err(e) => {
            bot.send_message(msg.chat.id, format!("❌ {e}")).await?;
        }
    }

    Ok(())
}
