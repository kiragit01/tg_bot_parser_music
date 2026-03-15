use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use teloxide::prelude::*;
use teloxide::types::{
    InlineKeyboardButton, InlineKeyboardMarkup,
    InlineQueryResult, InlineQueryResultArticle, InputMessageContent, InputMessageContentText,
};
use teloxide::utils::command::BotCommands;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::commands::Command;
use crate::downloader::{self, Source};
use crate::models::{Playlist, Track};
use crate::yandex;

type HandlerResult = ResponseResult<()>;

/// Плейлисты per-chat.
static PLAYLISTS: Mutex<Option<HashMap<ChatId, Playlist>>> = Mutex::const_new(None);

/// Токены отмены per-chat.
static CANCEL_TOKENS: Mutex<Option<HashMap<ChatId, CancellationToken>>> = Mutex::const_new(None);

/// Глобальный лимит одновременных скачиваний.
static GLOBAL_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| {
    Semaphore::new(global_max_concurrent())
});

async fn set_cancel_token(chat_id: ChatId) -> CancellationToken {
    let token = CancellationToken::new();
    let mut map = CANCEL_TOKENS.lock().await;
    let map = map.get_or_insert_with(HashMap::new);
    // Отменяем предыдущую задачу если была
    if let Some(old) = map.insert(chat_id, token.clone()) {
        old.cancel();
    }
    token
}

async fn cancel_download(chat_id: ChatId) -> bool {
    let mut map = CANCEL_TOKENS.lock().await;
    if let Some(map) = map.as_mut() {
        if let Some(token) = map.remove(&chat_id) {
            token.cancel();
            return true;
        }
    }
    false
}

async fn clear_cancel_token(chat_id: ChatId) {
    let mut map = CANCEL_TOKENS.lock().await;
    if let Some(map) = map.as_mut() {
        map.remove(&chat_id);
    }
}

async fn save_playlist(chat_id: ChatId, playlist: Playlist) {
    let mut map = PLAYLISTS.lock().await;
    let map = map.get_or_insert_with(HashMap::new);
    map.insert(chat_id, playlist);
}

async fn get_playlist(chat_id: ChatId) -> Option<Playlist> {
    PLAYLISTS.lock().await.as_ref()?.get(&chat_id).cloned()
}

const PROGRESS_EVERY: usize = 10;

/// Общий лимит параллельных скачиваний на всех пользователей.
fn global_max_concurrent() -> usize {
    std::env::var("MAX_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

fn html_escape_str(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Форматирует информацию о пользователе для логов.
fn user_tag(msg: &Message) -> String {
    let chat = &msg.chat;
    let user_info = msg
        .from
        .as_ref()
        .map(|u| {
            let name = match &u.last_name {
                Some(last) => format!("{} {}", u.first_name, last),
                None => u.first_name.clone(),
            };
            match &u.username {
                Some(uname) => format!("{name} (@{uname})"),
                None => name,
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    let chat_info = if chat.is_group() || chat.is_supergroup() {
        format!(
            " в группе «{}» [{}]",
            chat.title().unwrap_or("?"),
            chat.id
        )
    } else {
        format!(" [лс {}]", chat.id)
    };

    format!("{user_info}{chat_info}")
}

/// Единая точка входа для всех сообщений.
pub async fn process_message(bot: Bot, msg: Message) -> HandlerResult {
    let Some(text) = msg.text() else {
        return Ok(());
    };

    // Пробуем распарсить как команду
    let me = bot.get_me().await?;
    if let Ok(cmd) = Command::parse(text, me.username()) {
        log::info!("[CMD] {} -> {:?}", user_tag(&msg), cmd);
        return handle_command(&bot, &msg, cmd).await;
    }

    let is_group = msg.chat.is_group() || msg.chat.is_supergroup();

    // Ссылка на ЯМ или iframe-код
    if yandex::is_yandex_music_url(text) {
        log::info!("[PARSE] {} отправил ссылку ЯМ", user_tag(&msg));
        handle_parse_playlist(&bot, &msg, text).await?;
        return Ok(());
    }

    // В группах не реагируем на обычный текст (только команды и ссылки)
    if is_group {
        return Ok(());
    }

    // Многострочный список треков (только в личке)
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains(" - ") || l.contains(" — "))
        .collect();
    if lines.len() > 1 {
        log::info!("[LIST] {} отправил список из {} треков", user_tag(&msg), lines.len());
        handle_text_tracklist(&bot, &msg, &lines).await?;
        return Ok(());
    }

    // Один трек
    if text.contains(" - ") || text.contains(" — ") {
        log::info!("[TRACK] {} запросил: {}", user_tag(&msg), text);
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

        Command::Download(range) => {
            handle_download_range(bot, msg, &range).await?;
        }

        Command::Stop => {
            if cancel_download(msg.chat.id).await {
                log::info!("[STOP] {} остановил скачивание", user_tag(&msg));
                bot.send_message(msg.chat.id, "⏹ Скачивание остановлено.")
                    .await?;
            } else {
                bot.send_message(msg.chat.id, "ℹ️ Нет активного скачивания.")
                    .await?;
            }
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

            let pages = playlist.format_pages(50);
            for page in &pages {
                bot.send_message(msg.chat.id, page)
                    .parse_mode(teloxide::types::ParseMode::Html)
                    .await?;
            }

            save_playlist(msg.chat.id, playlist).await;

            let keyboard = source_keyboard("all");
            bot.send_message(
                msg.chat.id,
                format!(
                    "📥 Скачать все {count} треков — выбери источник:\n\
                     Или один трек: /get Исполнитель - Название"
                ),
            )
            .reply_markup(keyboard)
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
    let Some(playlist) = get_playlist(msg.chat.id).await else {
        bot.send_message(
            msg.chat.id,
            "❌ Нет сохранённого плейлиста. Сначала отправь ссылку или iframe-код.",
        )
        .await?;
        return Ok(());
    };

    let count = playlist.len();
    let keyboard = source_keyboard("all");
    bot.send_message(
        msg.chat.id,
        format!("📥 Скачать все {count} треков — выбери источник:"),
    )
    .reply_markup(keyboard)
    .await?;

    Ok(())
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
        format!("✅ {count} треков. Скачиваю...\nОстановить: /stop"),
    )
    .await?;

    let bot_clone = bot.clone();
    let chat_id = msg.chat.id;
    tokio::spawn(async move {
        if let Err(e) = download_tracks_parallel(&bot_clone, chat_id, &tracks).await {
            log::error!("Ошибка скачивания: {e:#}");
            let _ = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}")).await;
        }
    });

    Ok(())
}

async fn download_tracks_parallel(
    bot: &Bot,
    chat_id: ChatId,
    tracks: &[Track],
) -> HandlerResult {
    download_tracks_parallel_source(bot, chat_id, tracks, Source::Auto).await
}

async fn download_tracks_parallel_source(
    bot: &Bot,
    chat_id: ChatId,
    tracks: &[Track],
    source: Source,
) -> HandlerResult {
    let total = tracks.len();
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = Arc::new(Mutex::new(Vec::<String>::new()));
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_token = set_cancel_token(chat_id).await;

    let mut handles = Vec::new();

    for track in tracks.iter() {
        let bot = bot.clone();
        let done = done.clone();
        let failed = failed.clone();
        let track = track.clone();
        let cancel = cancel_token.clone();
        let cancelled = cancelled.clone();

        let handle = tokio::spawn(async move {
            // Проверяем отмену перед захватом семафора
            if cancel.is_cancelled() {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }

            let _permit = tokio::select! {
                p = GLOBAL_SEMAPHORE.acquire() => p.unwrap(),
                _ = cancel.cancelled() => {
                    cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };

            if cancel.is_cancelled() {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }

            match downloader::find_and_send_with_retry_source(&bot, chat_id, &track, source, cancel.clone()).await {
                Ok(()) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
                Err(e) => {
                    // Не считаем отменённые треки как «не найденные»
                    if cancel.is_cancelled() {
                        cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                    log::warn!("Не скачан: {} — {e:#}", track.search_query());
                    failed.lock().await.push(track.display());
                }
            }

            drop(_permit);

            // Не шлём прогресс если уже отменено
            if cancel.is_cancelled() {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }

            let completed = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if completed % PROGRESS_EVERY == 0 || completed == total {
                let _ = bot
                    .send_message(chat_id, format!("📊 Прогресс: {completed}/{total}"))
                    .await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    clear_cancel_token(chat_id).await;

    let was_cancelled = cancelled.load(std::sync::atomic::Ordering::Relaxed);
    let failed_list = failed.lock().await;
    let completed = done.load(std::sync::atomic::Ordering::Relaxed);
    let ok_count = completed - failed_list.len();

    if was_cancelled {
        let summary = format!("⏹ Остановлено. Скачано: {ok_count}/{total}");
        if !failed_list.is_empty() {
            let fail_count = failed_list.len();
            let items: String = failed_list
                .iter()
                .enumerate()
                .map(|(i, name)| format!("{}. {}", i + 1, html_escape_str(name)))
                .collect::<Vec<_>>()
                .join("\n");
            let msg = format!(
                "{summary}\n\n❌ Не найдены ({fail_count}):\n\
                 <blockquote expandable>{items}</blockquote>"
            );
            bot.send_message(chat_id, msg)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        } else {
            bot.send_message(chat_id, summary).await?;
        }
    } else if failed_list.is_empty() {
        bot.send_message(chat_id, format!("🎉 Готово! Все {total} треков скачаны."))
            .await?;
    } else {
        let fail_count = failed_list.len();
        let items: String = failed_list
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{}. {}", i + 1, html_escape_str(name)))
            .collect::<Vec<_>>()
            .join("\n");

        let summary = format!(
            "🎉 Готово! Скачано: {ok_count}/{total}\n\n\
             ❌ Не найдены ({fail_count}):\n\
             <blockquote expandable>{items}</blockquote>"
        );
        bot.send_message(chat_id, summary)
            .parse_mode(teloxide::types::ParseMode::Html)
            .await?;
    }

    Ok(())
}

/// Парсит строку диапазонов: "30-60", "1,5,10", "30-60,70-99", "1, 3, 5-10"
fn parse_track_indices(input: &str, total: usize) -> Vec<usize> {
    let mut indices = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (start.trim().parse::<usize>(), end.trim().parse::<usize>()) {
                for i in s..=e {
                    if i >= 1 && i <= total {
                        indices.push(i - 1); // 0-based
                    }
                }
            }
        } else if let Ok(n) = part.parse::<usize>() {
            if n >= 1 && n <= total {
                indices.push(n - 1);
            }
        }
    }
    indices.sort();
    indices.dedup();
    indices
}

async fn handle_download_range(bot: &Bot, msg: &Message, range: &str) -> HandlerResult {
    if range.trim().is_empty() {
        let hint = match get_playlist(msg.chat.id).await {
            Some(pl) => format!(
                "📥 Укажи номера треков:\n\
                 /download 1-20 — треки 1-20\n\
                 /download 30-60 — треки 30-60\n\
                 /download 1,5,10 — конкретные\n\
                 /download 30-60,70-99 — несколько диапазонов\n\n\
                 Доступно: 1-{}", pl.len()
            ),
            None => "❌ Сначала отправь ссылку на плейлист ЯМ, потом /download <номера>".to_string(),
        };
        bot.send_message(msg.chat.id, hint).await?;
        return Ok(());
    }

    let Some(playlist) = get_playlist(msg.chat.id).await else {
        bot.send_message(
            msg.chat.id,
            "❌ Нет сохранённого плейлиста. Сначала отправь ссылку или iframe-код.",
        )
        .await?;
        return Ok(());
    };

    let indices = parse_track_indices(range, playlist.len());

    if indices.is_empty() {
        bot.send_message(
            msg.chat.id,
            format!(
                "❌ Неверный диапазон. Доступно: 1-{}\n\
                 Примеры: /download 30-60 или /download 1,5,10 или /download 30-60,70-99",
                playlist.len()
            ),
        )
        .await?;
        return Ok(());
    }

    let count = indices.len();
    let range_clean = range.trim().to_string();

    let keyboard = source_keyboard(&format!("range:{range_clean}"));
    bot.send_message(
        msg.chat.id,
        format!("📥 Скачать {count} треков — выбери источник:"),
    )
    .reply_markup(keyboard)
    .await?;

    Ok(())
}

async fn handle_get_track(bot: &Bot, msg: &Message, query: &str) -> HandlerResult {
    let query = query.trim();
    if query.is_empty() {
        bot.send_message(msg.chat.id, "🔍 Укажи трек: /get Исполнитель - Название")
            .await?;
        return Ok(());
    }

    let track = if let Some(t) = yandex::parse_track_text(query) {
        t
    } else {
        Track::new("", query)
    };

    let search_q = track.search_query();
    bot.send_message(msg.chat.id, format!("🔍 Ищу: {search_q}"))
        .await?;

    // Собираем результаты со всех доступных платформ
    let mut text = String::new();
    let mut buttons: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    let mut idx = 0usize;

    // ЯМ
    if downloader::ym_available() {
        if let Ok(results) = crate::ym::search_tracks(&search_q, 3).await {
            for r in &results {
                idx += 1;
                let dur = format_duration_ms(r.duration_ms);
                let album_str = r.album.as_deref().map(|a| format!(" — {a}")).unwrap_or_default();
                text.push_str(&format!(
                    "{}. 🟡 {} — {}{} {}\n",
                    idx,
                    html_escape_str(&r.artist),
                    html_escape_str(&r.title),
                    html_escape_str(&album_str),
                    dur,
                ));
                // Telegram callback_data max 64 bytes, track_id usually short
                let label = truncate_str(&format!("🟡 {} — {}", r.artist, r.title), 60);
                buttons.push(vec![
                    InlineKeyboardButton::callback(label, format!("get:ym:{}", r.track_id)),
                ]);
            }
        }
    }

    // Всегда кнопка авто-поиска (SC → YT)
    buttons.push(vec![
        InlineKeyboardButton::callback(
            auto_search_label(),
            format!("get:auto:{}", truncate_str(&search_q, 50)),
        ),
    ]);

    if idx > 0 {
        let header = format!("🎵 Найдено ({idx}):\n\n");
        let keyboard = InlineKeyboardMarkup::new(buttons);
        bot.send_message(msg.chat.id, format!("{header}{text}"))
            .parse_mode(teloxide::types::ParseMode::Html)
            .reply_markup(keyboard)
            .await?;
    } else {
        // Ничего не нашли через поиск — качаем автоматом
        match downloader::find_and_send_with_retry(bot, msg.chat.id, &track).await {
            Ok(()) => {
                bot.send_message(msg.chat.id, "✅ Отправлен!").await?;
            }
            Err(e) => {
                bot.send_message(msg.chat.id, format!("❌ {e}")).await?;
            }
        }
    }

    Ok(())
}

fn format_duration_ms(ms: Option<u64>) -> String {
    match ms {
        Some(ms) => {
            let s = ms / 1000;
            format!("({}:{:02})", s / 60, s % 60)
        }
        None => String::new(),
    }
}

fn auto_search_label() -> &'static str {
    match (downloader::ym_available(), downloader::vk_available()) {
        (true, true) => "🔀 Авто (YM→SC→VK→YT)",
        (true, false) => "🔀 Авто (YM→SC→YT)",
        (false, true) => "🔀 Авто (SC→VK→YT)",
        (false, false) => "🔀 Авто (SC→YT)",
    }
}

fn truncate_str(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        s.to_string()
    } else {
        let mut end = max_bytes;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

/// Обработка inline-запросов (@bot Артист - Название).
pub async fn process_inline(bot: Bot, q: InlineQuery) -> HandlerResult {
    let query = q.query.trim();

    if query.is_empty() {
        bot.answer_inline_query(q.id.clone(), Vec::<InlineQueryResult>::new())
            .cache_time(0)
            .await?;
        return Ok(());
    }

    // Предлагаем отправить команду /get в чат с ботом
    let track = yandex::parse_track_text(query)
        .unwrap_or_else(|| Track::new("", query));

    let display = track.search_query();

    let result = InlineQueryResultArticle::new(
        "1",
        format!("🎵 Скачать: {display}"),
        InputMessageContent::Text(
            InputMessageContentText::new(format!("/get {display}"))
        ),
    )
    .description("Нажми, чтобы отправить команду боту");

    bot.answer_inline_query(q.id.clone(), vec![InlineQueryResult::Article(result)])
        .cache_time(10)
        .await?;

    Ok(())
}

/// Клавиатура выбора источника. `prefix` — "all" или "range:1-20".
fn source_keyboard(prefix: &str) -> InlineKeyboardMarkup {
    let has_ym = downloader::ym_available();
    let has_vk = downloader::vk_available();

    let auto_label = match (has_ym, has_vk) {
        (true, true) => "🔀 Авто (YM→SC→VK→YT)",
        (true, false) => "🔀 Авто (YM→SC→YT)",
        (false, true) => "🔀 Авто (SC→VK→YT)",
        (false, false) => "🔀 Авто (SC→YT)",
    };

    let mut rows: Vec<Vec<InlineKeyboardButton>> = vec![
        vec![InlineKeyboardButton::callback(auto_label, format!("dl:{prefix}:auto"))],
    ];

    // ЯМ — только если токен есть
    if has_ym {
        rows.push(vec![
            InlineKeyboardButton::callback("🎵 Яндекс.Музыка", format!("dl:{prefix}:ym")),
        ]);
    }

    let row2 = vec![
        InlineKeyboardButton::callback("☁️ SoundCloud", format!("dl:{prefix}:sc")),
        InlineKeyboardButton::callback("▶️ YouTube", format!("dl:{prefix}:yt")),
    ];
    rows.push(row2);

    if has_vk {
        rows.push(vec![
            InlineKeyboardButton::callback("🎶 VK Music", format!("dl:{prefix}:vk")),
        ]);
    }

    InlineKeyboardMarkup::new(rows)
}

fn parse_source(s: &str) -> Source {
    match s {
        "ym" => Source::YandexMusic,
        "sc" => Source::SoundCloud,
        "yt" => Source::YouTube,
        "vk" => Source::Vk,
        _ => Source::Auto,
    }
}

/// Обработка нажатий на inline-кнопки.
pub async fn process_callback(bot: Bot, q: CallbackQuery) -> HandlerResult {
    let Some(data) = &q.data else { return Ok(()) };
    let Some(msg) = &q.message else { return Ok(()) };
    let chat_id = msg.chat().id;
    let msg_id = msg.id();

    let parts: Vec<&str> = data.splitn(3, ':').collect();
    if parts.len() < 2 {
        return Ok(());
    }

    // Обработка выбора трека: "get:<source>:<track_id_or_query>"
    if parts[0] == "get" && parts.len() == 3 {
        bot.answer_callback_query(q.id.clone()).await?;
        let _ = bot.edit_message_reply_markup(chat_id, msg_id).await;

        let source_key = parts[1];
        let value = parts[2];

        if source_key == "ym" {
            // Скачиваем конкретный трек из ЯМ по ID
            bot.send_message(chat_id, "⏳ Скачиваю из Яндекс.Музыки...").await?;
            let output_path = PathBuf::from("downloads").join(format!("{value}.mp3"));
            match crate::ym::download_track(value, &output_path).await {
                Ok(()) => {
                    let bytes = tokio::fs::read(&output_path).await.unwrap_or_default();
                    if !bytes.is_empty() {
                        let input_file = teloxide::types::InputFile::memory(bytes)
                            .file_name(format!("{value}.mp3"));
                        let _ = bot.send_audio(chat_id, input_file).await;
                    }
                    tokio::fs::remove_file(&output_path).await.ok();
                    bot.send_message(chat_id, "✅ Отправлен!").await?;
                }
                Err(e) => {
                    bot.send_message(chat_id, format!("❌ {e}")).await?;
                }
            }
        } else {
            // Авто-поиск
            let track = if let Some(t) = yandex::parse_track_text(value) {
                t
            } else {
                Track::new("", value)
            };
            match downloader::find_and_send_with_retry(&bot, chat_id, &track).await {
                Ok(()) => {
                    bot.send_message(chat_id, "✅ Отправлен!").await?;
                }
                Err(e) => {
                    bot.send_message(chat_id, format!("❌ {e}")).await?;
                }
            }
        }
        return Ok(());
    }

    // Обработка скачивания плейлиста: "dl:<scope>:<source>"
    // scope может быть "all" или "range:25-40", поэтому source — последний сегмент после последнего ':'
    if parts[0] != "dl" {
        return Ok(());
    }

    // Берём source из конца (после последнего :), scope — всё между первым и последним :
    let Some(last_colon) = data.rfind(':') else { return Ok(()) };
    let source = parse_source(&data[last_colon + 1..]);
    let scope = &data[3..last_colon]; // пропускаем "dl:"

    bot.answer_callback_query(q.id.clone()).await?;

    // Удалим кнопки из сообщения
    let _ = bot.edit_message_reply_markup(chat_id, msg_id).await;

    let Some(playlist) = get_playlist(chat_id).await else {
        bot.send_message(chat_id, "❌ Плейлист не найден. Отправь ссылку заново.")
            .await?;
        return Ok(());
    };

    // Определяем какие треки качать
    let tracks: Vec<Track> = if scope == "all" {
        playlist.tracks.clone()
    } else if let Some(range) = scope.strip_prefix("range:") {
        let indices = parse_track_indices(range, playlist.len());
        indices.into_iter().map(|i| playlist.tracks[i].clone()).collect()
    } else {
        return Ok(());
    };

    if tracks.is_empty() {
        bot.send_message(chat_id, "❌ Нет треков для скачивания.")
            .await?;
        return Ok(());
    }

    let count = tracks.len();
    let src_label = source.label();
    log::info!("[DL] chat={chat_id}, треков={count}, источник={src_label}");

    bot.send_message(
        chat_id,
        format!("📥 Скачиваю {count} треков ({src_label})...\nОстановить: /stop"),
    )
    .await?;

    // Запускаем в фоне чтобы не блокировать обработку команд (в т.ч. /stop)
    let bot_clone = bot.clone();
    tokio::spawn(async move {
        if let Err(e) = download_tracks_parallel_source(&bot_clone, chat_id, &tracks, source).await {
            log::error!("Ошибка скачивания: {e:#}");
            let _ = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}")).await;
        }
    });

    Ok(())
}
