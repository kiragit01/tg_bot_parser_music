use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, OnceLock};

use teloxide::prelude::*;
use teloxide::types::{
    InlineKeyboardButton, InlineKeyboardMarkup,
    InlineQueryResult, InlineQueryResultArticle, InputMessageContent, InputMessageContentText,
    ThreadId, UserId,
};
use teloxide::utils::command::BotCommands;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::cache;
use crate::commands::Command;
use crate::downloader::{self, Source};
use crate::models::{html_escape, Platform, Playlist, SearchResult, Track};
use crate::yandex;

type HandlerResult = ResponseResult<()>;

/// Закешированный username бота (заполняется при старте).
static BOT_USERNAME: OnceLock<String> = OnceLock::new();

pub fn set_bot_username(username: String) {
    BOT_USERNAME.set(username).ok();
}

fn bot_username() -> &'static str {
    BOT_USERNAME.get().map(|s| s.as_str()).unwrap_or("")
}

/// Плейлисты per-user per-chat (с ограничением размера).
static PLAYLISTS: LazyLock<Mutex<HashMap<(ChatId, UserId), Arc<Playlist>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Кеш результатов поиска per-user per-chat (для callback-кнопок).
static SEARCH_CACHE: LazyLock<Mutex<HashMap<(ChatId, UserId), Vec<SearchResult>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Токены отмены per-chat.
static CANCEL_TOKENS: LazyLock<Mutex<HashMap<ChatId, CancellationToken>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Кеш топиков: chat_id → thread_id созданного топика (для повторного использования диапазонами).
static TOPIC_CACHE: LazyLock<Mutex<HashMap<ChatId, ThreadId>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Глобальный лимит одновременных скачиваний.
static GLOBAL_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| {
    Semaphore::new(global_max_concurrent())
});

/// Ожидание ввода от пользователя (после пустой команды).
#[derive(Debug, Clone)]
enum PendingCommand {
    Get,
    Lyrics,
    Download,
}

/// Ожидание ввода per-user per-chat.
static PENDING_INPUT: LazyLock<Mutex<HashMap<(ChatId, UserId), PendingCommand>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn set_pending(chat_id: ChatId, user_id: UserId, cmd: PendingCommand) {
    PENDING_INPUT.lock().await.insert((chat_id, user_id), cmd);
}

async fn take_pending(chat_id: ChatId, user_id: UserId) -> Option<PendingCommand> {
    PENDING_INPUT.lock().await.remove(&(chat_id, user_id))
}

async fn clear_pending(chat_id: ChatId, user_id: UserId) {
    PENDING_INPUT.lock().await.remove(&(chat_id, user_id));
}

/// Макс. плейлистов в кеше (per-user).
const MAX_CACHED_PLAYLISTS: usize = 500;

async fn set_cancel_token(chat_id: ChatId) -> CancellationToken {
    let token = CancellationToken::new();
    let mut map = CANCEL_TOKENS.lock().await;
    // Отменяем предыдущую задачу если была
    if let Some(old) = map.insert(chat_id, token.clone()) {
        old.cancel();
    }
    token
}

async fn cancel_download(chat_id: ChatId) -> bool {
    let mut map = CANCEL_TOKENS.lock().await;
    if let Some(token) = map.remove(&chat_id) {
        token.cancel();
        return true;
    }
    false
}

async fn clear_cancel_token(chat_id: ChatId) {
    CANCEL_TOKENS.lock().await.remove(&chat_id);
}

async fn save_playlist(chat_id: ChatId, user_id: UserId, playlist: Playlist) {
    let key = (chat_id, user_id);
    let mut map = PLAYLISTS.lock().await;
    // LRU-подобная очистка: если кеш переполнен, удаляем случайный
    if map.len() >= MAX_CACHED_PLAYLISTS && !map.contains_key(&key) {
        if let Some(&old_key) = map.keys().next() {
            map.remove(&old_key);
        }
    }
    map.insert(key, Arc::new(playlist));
}

async fn get_playlist(chat_id: ChatId, user_id: UserId) -> Option<Arc<Playlist>> {
    PLAYLISTS.lock().await.get(&(chat_id, user_id)).cloned()
}

async fn save_search_results(chat_id: ChatId, user_id: UserId, results: Vec<SearchResult>) {
    let key = (chat_id, user_id);
    let mut map = SEARCH_CACHE.lock().await;
    if map.len() >= MAX_CACHED_PLAYLISTS && !map.contains_key(&key) {
        if let Some(&old_key) = map.keys().next() {
            map.remove(&old_key);
        }
    }
    map.insert(key, results);
}

async fn get_search_result(chat_id: ChatId, user_id: UserId, index: usize) -> Option<SearchResult> {
    SEARCH_CACHE.lock().await.get(&(chat_id, user_id)).and_then(|v| v.get(index).cloned())
}

/// Извлекает UserId из сообщения (fallback на 0 для системных).
fn msg_user_id(msg: &Message) -> UserId {
    msg.from.as_ref().map(|u| u.id).unwrap_or(UserId(0))
}

/// Извлекает thread_id из сообщения (для ответа в ту же тему форума).
fn msg_thread_id(msg: &Message) -> Option<ThreadId> {
    msg.thread_id
}

/// Отправляет сообщение в тот же тред, откуда пришёл запрос.
async fn reply_text(bot: &Bot, msg: &Message, text: &str) -> ResponseResult<Message> {
    let mut req = bot.send_message(msg.chat.id, text);
    if let Some(tid) = msg_thread_id(msg) {
        req = req.message_thread_id(tid);
    }
    req.await
}

/// Отправляет сообщение с HTML в тот же тред.
async fn reply_html(bot: &Bot, msg: &Message, text: &str) -> ResponseResult<Message> {
    let mut req = bot.send_message(msg.chat.id, text)
        .parse_mode(teloxide::types::ParseMode::Html);
    if let Some(tid) = msg_thread_id(msg) {
        req = req.message_thread_id(tid);
    }
    req.await
}

/// Отправляет сообщение с клавиатурой в тот же тред.
async fn reply_markup(bot: &Bot, msg: &Message, text: &str, keyboard: InlineKeyboardMarkup) -> ResponseResult<Message> {
    let mut req = bot.send_message(msg.chat.id, text)
        .reply_markup(keyboard);
    if let Some(tid) = msg_thread_id(msg) {
        req = req.message_thread_id(tid);
    }
    req.await
}

const PROGRESS_EVERY: usize = 10;

/// Общий лимит параллельных скачиваний на всех пользователей.
fn global_max_concurrent() -> usize {
    std::env::var("MAX_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
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
        // Не текст (файл, стикер и т.д.) — сбрасываем ожидание
        clear_pending(msg.chat.id, msg_user_id(&msg)).await;
        return Ok(());
    };

    let user_id = msg_user_id(&msg);

    // Команда или ссылка — сбрасывает ожидание
    if let Ok(cmd) = Command::parse(text, bot_username()) {
        clear_pending(msg.chat.id, user_id).await;
        log::info!("[CMD] {} -> {:?}", user_tag(&msg), cmd);
        return handle_command(&bot, &msg, cmd).await;
    }

    let is_group = msg.chat.is_group() || msg.chat.is_supergroup();

    // Ссылка на ЯМ или iframe-код — сбрасывает ожидание
    if yandex::is_yandex_music_url(text) {
        clear_pending(msg.chat.id, user_id).await;
        log::info!("[PARSE] {} отправил ссылку ЯМ", user_tag(&msg));
        handle_parse_playlist(&bot, &msg, text).await?;
        return Ok(());
    }

    // Проверяем ожидание ввода (после пустой /get, /lyrics, /download)
    if let Some(pending) = take_pending(msg.chat.id, user_id).await {
        let input = text.trim();
        if !input.is_empty() {
            match pending {
                PendingCommand::Get => {
                    log::info!("[GET-PENDING] {} -> {}", user_tag(&msg), input);
                    return handle_get_track(&bot, &msg, input).await;
                }
                PendingCommand::Lyrics => {
                    log::info!("[LYRICS-PENDING] {} -> {}", user_tag(&msg), input);
                    return handle_lyrics(&bot, &msg, input).await;
                }
                PendingCommand::Download => {
                    log::info!("[DL-PENDING] {} -> {}", user_tag(&msg), input);
                    return handle_download_range(&bot, &msg, input).await;
                }
            }
        }
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

    reply_text(&bot, &msg,
        "🤔 Не понял. Попробуй:\n\
         • /get Исполнитель - Название — скачать трек\n\
         • Ссылку на плейлист Яндекс.Музыки\n\
         • Текстовый список треков (Исполнитель - Название)\n\
         • /help — все команды",
    )
    .await?;

    Ok(())
}

async fn handle_command(bot: &Bot, msg: &Message, cmd: Command) -> HandlerResult {
    match cmd {
        Command::Start => {
            reply_html(bot, msg,
                "👋 Привет! Я скачиваю музыку прямо в Telegram.\n\n\
                 🎵 <b>Что умею:</b>\n\
                 • Скачать любой трек — /get Исполнитель - Название\n\
                 • Перенести плейлист из Яндекс.Музыки — кинь ссылку\n\
                 • Скачать по текстовому списку (Исполнитель - Название)\n\
                 • Показать текст песни — /lyrics\n\n\
                 🔍 <b>Где ищу:</b> SoundCloud, VK, YouTube, Яндекс.Музыка\n\
                 ⚡ <b>Фишки:</b> параллельная загрузка, кэш (повторно — мгновенно), без цензуры\n\n\
                 /help — все команды",
            )
            .await?;
        }

        Command::Help => {
            reply_text(bot, msg, &Command::descriptions().to_string())
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
                log::info!("[STOP] {} остановил скачивание", user_tag(msg));
                reply_text(bot, msg, "⏹ Скачивание остановлено.").await?;
            } else {
                reply_text(bot, msg, "ℹ️ Нет активного скачивания.").await?;
            }
        }

        Command::Status => {
            let has_active = CANCEL_TOKENS.lock().await.contains_key(&msg.chat.id);
            let status_msg = if has_active {
                "📊 Скачивание в процессе. Остановить: /stop"
            } else {
                "📊 Нет активных задач."
            };
            reply_text(bot, msg, status_msg).await?;
        }

        Command::Settings => {
            handle_settings(bot, msg).await?;
        }

        Command::Lyrics(query) => {
            handle_lyrics(bot, msg, &query).await?;
        }
    }

    Ok(())
}

async fn handle_parse_playlist(bot: &Bot, msg: &Message, text: &str) -> HandlerResult {
    reply_text(bot, msg, "⏳ Парсю плейлист...").await?;

    match yandex::parse_playlist(text).await {
        Ok(playlist) => {
            if playlist.is_empty() {
                reply_text(bot, msg, "😕 Плейлист пуст или не удалось извлечь треки.").await?;
                return Ok(());
            }

            let count = playlist.len();
            reply_text(bot, msg, &format!("✅ Найдено треков: {count}")).await?;

            let pages = playlist.format_pages(50);
            for page in &pages {
                reply_html(bot, msg, page).await?;
            }

            save_playlist(msg.chat.id, msg_user_id(msg), playlist.clone()).await;

            let forum = is_forum_chat(&msg.chat);
            if let Some(keyboard) = source_keyboard("all", forum, false) {
                reply_markup(bot, msg,
                    &format!(
                        "📥 Скачать все {count} треков — выбери источник:\n\
                         Или один трек: /get Исполнитель - Название"
                    ),
                    keyboard,
                ).await?;
            } else {
                // Нет YM — скачиваем автоматом
                reply_text(bot, msg, &format!("📥 Скачиваю {count} треков...\nОстановить: /stop")).await?;
                let bot_clone = bot.clone();
                let chat_id = msg.chat.id;
                let thread_id = msg_thread_id(msg);
                let tracks = playlist.tracks;
                tokio::spawn(async move {
                    if let Err(e) = download_tracks_parallel_source(&bot_clone, chat_id, &tracks, Source::Auto, thread_id, None).await {
                        log::error!("Ошибка скачивания: {e:#}");
                        let mut err_msg = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}"));
                        if let Some(tid) = thread_id {
                            err_msg = err_msg.message_thread_id(tid);
                        }
                        let _ = err_msg.await;
                    }
                });
            }
        }
        Err(e) => {
            log::error!("Ошибка парсинга: {e:#}");
            reply_text(bot, msg, &format!("❌ {e}")).await?;
        }
    }

    Ok(())
}

async fn handle_download_all(bot: &Bot, msg: &Message) -> HandlerResult {
    let Some(playlist) = get_playlist(msg.chat.id, msg_user_id(msg)).await else {
        reply_text(bot, msg, "❌ Нет сохранённого плейлиста. Сначала отправь ссылку или iframe-код.").await?;
        return Ok(());
    };

    let count = playlist.len();
    let forum = is_forum_chat(&msg.chat);
    let has_topic = has_saved_topic(msg.chat.id).await;

    if let Some(keyboard) = source_keyboard("all", forum, has_topic) {
        reply_markup(bot, msg,
            &format!("📥 Скачать все {count} треков — выбери источник:"),
            keyboard,
        ).await?;
    } else {
        // Нет YM — скачиваем автоматом
        reply_text(bot, msg, &format!("📥 Скачиваю {count} треков...\nОстановить: /stop")).await?;
        let bot_clone = bot.clone();
        let chat_id = msg.chat.id;
        let thread_id = msg_thread_id(msg);
        let tracks = playlist.tracks.clone();
        tokio::spawn(async move {
            if let Err(e) = download_tracks_parallel_source(&bot_clone, chat_id, &tracks, Source::Auto, thread_id, None).await {
                log::error!("Ошибка скачивания: {e:#}");
                let mut err_msg = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}"));
                if let Some(tid) = thread_id {
                    err_msg = err_msg.message_thread_id(tid);
                }
                let _ = err_msg.await;
            }
        });
    }

    Ok(())
}

async fn handle_text_tracklist(bot: &Bot, msg: &Message, lines: &[&str]) -> HandlerResult {
    let tracks: Vec<Track> = lines
        .iter()
        .filter_map(|line| yandex::parse_track_text(line))
        .collect();

    if tracks.is_empty() {
        reply_text(bot, msg, "😕 Не удалось распознать треки.").await?;
        return Ok(());
    }

    let count = tracks.len();
    reply_text(bot, msg, &format!("✅ {count} треков. Скачиваю...\nОстановить: /stop")).await?;

    let bot_clone = bot.clone();
    let chat_id = msg.chat.id;
    let thread_id = msg_thread_id(msg);
    tokio::spawn(async move {
        if let Err(e) = download_tracks_parallel_source(&bot_clone, chat_id, &tracks, Source::Auto, thread_id, None).await {
            log::error!("Ошибка скачивания: {e:#}");
            let mut err_msg = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}"));
            if let Some(tid) = thread_id {
                err_msg = err_msg.message_thread_id(tid);
            }
            let _ = err_msg.await;
        }
    });

    Ok(())
}

async fn download_tracks_parallel(
    bot: &Bot,
    chat_id: ChatId,
    tracks: &[Track],
) -> HandlerResult {
    download_tracks_parallel_source(bot, chat_id, tracks, Source::Auto, None, None).await
}

async fn download_tracks_parallel_source(
    bot: &Bot,
    chat_id: ChatId,
    tracks: &[Track],
    source: Source,
    thread_id: Option<ThreadId>,
    user_id: Option<u64>,
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
        let tid = thread_id;

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

            match downloader::find_and_send_with_retry_source(&bot, chat_id, &track, source, cancel.clone(), tid, user_id).await {
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

            // Не шлём прогресс если уже отменено
            if cancel.is_cancelled() {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }

            let completed = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if completed % PROGRESS_EVERY == 0 || completed == total {
                let mut progress_req = bot
                    .send_message(chat_id, format!("📊 Прогресс: {completed}/{total}"));
                if let Some(t) = tid {
                    progress_req = progress_req.message_thread_id(t);
                }
                let _ = progress_req.await;
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
    let ok_count = completed.saturating_sub(failed_list.len());

    // Хелпер для отправки сообщения в нужный поток
    macro_rules! send_to_thread {
        ($text:expr) => {{
            let mut r = bot.send_message(chat_id, $text);
            if let Some(t) = thread_id {
                r = r.message_thread_id(t);
            }
            r
        }};
    }

    if was_cancelled {
        let summary = format!("⏹ Остановлено. Скачано: {ok_count}/{total}");
        if !failed_list.is_empty() {
            let fail_count = failed_list.len();
            let items: String = failed_list
                .iter()
                .enumerate()
                .map(|(i, name)| format!("{}. {}", i + 1, html_escape(name)))
                .collect::<Vec<_>>()
                .join("\n");
            let msg = format!(
                "{summary}\n\n❌ Не найдены ({fail_count}):\n\
                 <blockquote expandable>{items}</blockquote>"
            );
            send_to_thread!(msg)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        } else {
            send_to_thread!(summary).await?;
        }
    } else if failed_list.is_empty() {
        send_to_thread!(format!("🎉 Готово! Все {total} треков скачаны.")).await?;
    } else {
        let fail_count = failed_list.len();
        let items: String = failed_list
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{}. {}", i + 1, html_escape(name)))
            .collect::<Vec<_>>()
            .join("\n");

        let summary = format!(
            "🎉 Готово! Скачано: {ok_count}/{total}\n\n\
             ❌ Не найдены ({fail_count}):\n\
             <blockquote expandable>{items}</blockquote>"
        );
        send_to_thread!(summary)
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
        match get_playlist(msg.chat.id, msg_user_id(msg)).await {
            Some(pl) => {
                set_pending(msg.chat.id, msg_user_id(msg), PendingCommand::Download).await;
                reply_text(bot, msg, &format!(
                    "📥 Напиши номера треков (доступно: 1-{}):\n\
                     Например: 1-20, 30-60, 1,5,10", pl.len()
                )).await?;
            }
            None => {
                reply_text(bot, msg, "❌ Сначала отправь ссылку на плейлист, потом /download").await?;
            }
        };
        return Ok(());
    }

    let Some(playlist) = get_playlist(msg.chat.id, msg_user_id(msg)).await else {
        reply_text(bot, msg, "❌ Нет сохранённого плейлиста. Сначала отправь ссылку или iframe-код.").await?;
        return Ok(());
    };

    let indices = parse_track_indices(range, playlist.len());

    if indices.is_empty() {
        reply_text(bot, msg, &format!(
            "❌ Неверный диапазон. Доступно: 1-{}\n\
             Примеры: /download 30-60 или /download 1,5,10 или /download 30-60,70-99",
            playlist.len()
        )).await?;
        return Ok(());
    }

    let count = indices.len();
    let range_clean = range.trim().to_string();

    let forum = is_forum_chat(&msg.chat);
    let has_topic = has_saved_topic(msg.chat.id).await;

    if let Some(keyboard) = source_keyboard(&format!("range:{range_clean}"), forum, has_topic) {
        reply_markup(bot, msg,
            &format!("📥 Скачать {count} треков — выбери источник:"),
            keyboard,
        ).await?;
    } else {
        // Нет YM — скачиваем автоматом
        let tracks: Vec<Track> = indices.iter().map(|&i| playlist.tracks[i].clone()).collect();
        reply_text(bot, msg, &format!("📥 Скачиваю {count} треков...\nОстановить: /stop")).await?;
        let bot_clone = bot.clone();
        let chat_id = msg.chat.id;
        let thread_id = msg_thread_id(msg);
        tokio::spawn(async move {
            if let Err(e) = download_tracks_parallel_source(&bot_clone, chat_id, &tracks, Source::Auto, thread_id, None).await {
                log::error!("Ошибка скачивания: {e:#}");
                let mut err_msg = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}"));
                if let Some(tid) = thread_id {
                    err_msg = err_msg.message_thread_id(tid);
                }
                let _ = err_msg.await;
            }
        });
    }

    Ok(())
}

async fn handle_get_track(bot: &Bot, msg: &Message, query: &str) -> HandlerResult {
    let query = query.trim();
    if query.is_empty() {
        set_pending(msg.chat.id, msg_user_id(msg), PendingCommand::Get).await;
        reply_text(bot, msg, "🔍 Напиши название трека или Исполнитель - Название:").await?;
        return Ok(());
    }

    let track = if let Some(t) = yandex::parse_track_text(query) {
        t
    } else {
        Track::new("", query)
    };

    let search_q = track.search_query();

    // === Кеш поиска: проверяем ===
    if let Some(cached) = cache::get_search(&search_q, "all").await {
        reply_text(bot, msg, &format!("🔍 Ищу на всех платформах: {search_q}")).await?;
        let all_results: Vec<SearchResult> = cached
            .into_iter()
            .map(|c| SearchResult {
                platform: match c.platform.as_str() {
                    "YM" => Platform::YandexMusic,
                    "SC" => Platform::SoundCloud,
                    "YT" => Platform::YouTube,
                    "VK" => Platform::Vk,
                    _ => Platform::YouTube,
                },
                title: c.title,
                artist: c.artist,
                duration_sec: c.duration_sec,
                download_key: String::new(),
            })
            .collect();
        log::info!("[SEARCH CACHE] Результаты из кеша для: {search_q}");
        return show_search_results(bot, msg, &search_q, &track, all_results).await;
    }

    reply_text(bot, msg, &format!("🔍 Ищу на всех платформах: {search_q}")).await?;

    // Параллельный поиск на всех доступных платформах
    let search_q_ref = &search_q;
    let (ym_res, vk_res, sc_res, yt_res) = tokio::join!(
        search_ym(search_q_ref),
        search_vk(search_q_ref),
        async {
            downloader::search_ytdlp_metadata(search_q_ref, "sc", 2)
                .await
                .unwrap_or_default()
        },
        async {
            downloader::search_ytdlp_metadata(search_q_ref, "yt", 2)
                .await
                .unwrap_or_default()
        },
    );

    // Собираем все результаты в единый список
    let mut all_results: Vec<SearchResult> = Vec::new();
    all_results.extend(ym_res);
    all_results.extend(vk_res);
    all_results.extend(sc_res);
    all_results.extend(yt_res);

    // === Кеш поиска: сохраняем ===
    if !all_results.is_empty() {
        let cached: Vec<cache::CachedSearchResult> = all_results
            .iter()
            .map(|r| cache::CachedSearchResult {
                artist: r.artist.clone(),
                title: r.title.clone(),
                duration_sec: r.duration_sec,
                platform: r.platform.label().to_string(),
            })
            .collect();
        cache::save_search(&search_q, "all", &cached).await;
    }

    show_search_results(bot, msg, &search_q, &track, all_results).await
}

async fn show_search_results(
    bot: &Bot,
    msg: &Message,
    search_q: &str,
    track: &Track,
    all_results: Vec<SearchResult>,
) -> HandlerResult {
    let thread_id = msg_thread_id(msg);
    let user_id = Some(msg_user_id(msg).0);

    if all_results.is_empty() {
        match downloader::find_and_send_with_retry_source(bot, msg.chat.id, track, Source::Auto, tokio_util::sync::CancellationToken::new(), thread_id, user_id).await {
            Ok(()) => {
                reply_text(bot, msg, "✅ Отправлен!").await?;
            }
            Err(e) => {
                reply_text(bot, msg, &format!("❌ {e}")).await?;
            }
        }
        return Ok(());
    }

    let count = all_results.len();
    let mut text = format!("🎵 Найдено ({count}):\n\n");
    let mut buttons: Vec<Vec<InlineKeyboardButton>> = Vec::new();

    for (i, result) in all_results.iter().enumerate() {
        let idx = i + 1;
        text.push_str(&result.display_line(idx));
        text.push('\n');
        buttons.push(vec![InlineKeyboardButton::callback(
            result.button_label(idx),
            format!("s:{i}"),
        )]);
    }

    buttons.push(vec![InlineKeyboardButton::callback(
        auto_search_label(),
        format!("get:auto:{}", truncate_str(search_q, 50)),
    )]);

    save_search_results(msg.chat.id, msg_user_id(msg), all_results).await;

    let keyboard = InlineKeyboardMarkup::new(buttons);
    let mut req = bot.send_message(msg.chat.id, text)
        .parse_mode(teloxide::types::ParseMode::Html)
        .reply_markup(keyboard);
    if let Some(tid) = thread_id {
        req = req.message_thread_id(tid);
    }
    req.await?;

    Ok(())
}

/// Поиск в Яндекс.Музыке → Vec<SearchResult>.
async fn search_ym(query: &str) -> Vec<SearchResult> {
    if !downloader::ym_available() {
        return Vec::new();
    }
    match crate::ym::search_tracks(query, 2).await {
        Ok(results) => results
            .into_iter()
            .map(|r| SearchResult {
                platform: Platform::YandexMusic,
                title: r.title,
                artist: r.artist,
                duration_sec: r.duration_ms.map(|ms| (ms / 1000) as u32),
                download_key: r.track_id,
            })
            .collect(),
        Err(e) => {
            log::debug!("YM поиск: {e:#}");
            Vec::new()
        }
    }
}

/// Поиск в VK → Vec<SearchResult>.
async fn search_vk(query: &str) -> Vec<SearchResult> {
    let Some(token) = crate::tokens::VK.next() else {
        return Vec::new();
    };
    match crate::vk::search_tracks(token, query, 2).await {
        Ok(results) => results
            .into_iter()
            .map(|a| SearchResult {
                platform: Platform::Vk,
                title: a.title,
                artist: a.artist,
                duration_sec: Some(a.duration),
                download_key: a.url,
            })
            .collect(),
        Err(e) => {
            log::debug!("VK поиск: {e:#}");
            Vec::new()
        }
    }
}


fn auto_search_label() -> &'static str {
    match (downloader::vk_available(), downloader::ym_available()) {
        (true, true) => "🔀 Авто (SC→VK→YT→YM)",
        (true, false) => "🔀 Авто (SC→VK→YT)",
        (false, true) => "🔀 Авто (SC→YT→YM)",
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

async fn handle_settings(bot: &Bot, msg: &Message) -> HandlerResult {
    let user_id = msg_user_id(msg).0;
    let settings = cache::get_user_settings(user_id).await;
    let keyboard = settings_keyboard(&settings);
    reply_markup(bot, msg, "⚙️ Настройки:", keyboard).await?;
    Ok(())
}

/// Отправляет текст песни в blockquote (из callback — chat_id + thread_id).
async fn send_lyrics_to_chat(bot: &Bot, chat_id: ChatId, thread_id: Option<ThreadId>, title: &str, text: &str) -> HandlerResult {
    use teloxide::types::ParseMode;
    let escaped_title = html_escape(title);
    let escaped_text = html_escape(text);
    let full = format!("📝 <b>{escaped_title}</b>\n\n<blockquote expandable>{escaped_text}</blockquote>");

    if full.len() <= 4096 {
        let mut req = bot.send_message(chat_id, &full).parse_mode(ParseMode::Html);
        if let Some(tid) = thread_id { req = req.message_thread_id(tid); }
        req.await?;
    } else {
        let max_text_len = 3800;
        let mut offset = 0;
        let mut part = 1;
        while offset < escaped_text.len() {
            let boundary = (offset + max_text_len).min(escaped_text.len());
            let safe_boundary = escaped_text.floor_char_boundary(boundary);
            let end = if safe_boundary >= escaped_text.len() {
                escaped_text.len()
            } else {
                escaped_text[offset..safe_boundary].rfind('\n').map(|pos| offset + pos).unwrap_or(safe_boundary)
            };
            let chunk = &escaped_text[offset..end];
            let msg_text = if part == 1 {
                format!("📝 <b>{escaped_title}</b>\n\n<blockquote expandable>{chunk}</blockquote>")
            } else {
                format!("<blockquote expandable>{chunk}</blockquote>")
            };
            let mut req = bot.send_message(chat_id, &msg_text).parse_mode(ParseMode::Html);
            if let Some(tid) = thread_id { req = req.message_thread_id(tid); }
            req.await?;
            offset = end;
            if offset < escaped_text.len() && escaped_text.as_bytes()[offset] == b'\n' { offset += 1; }
            part += 1;
        }
    }
    Ok(())
}

/// Отправляет текст песни в blockquote формате. Разбивает на части если длинный.
async fn send_lyrics_message(bot: &Bot, msg: &Message, title: &str, text: &str) -> HandlerResult {
    use teloxide::types::ParseMode;

    let escaped_title = html_escape(title);
    let escaped_text = html_escape(text);
    let thread_id = msg_thread_id(msg);
    let chat_id = msg.chat.id;

    // Формируем: заголовок + свёрнутая цитата
    let full = format!("📝 <b>{escaped_title}</b>\n\n<blockquote expandable>{escaped_text}</blockquote>");

    if full.len() <= 4096 {
        let mut req = bot.send_message(chat_id, &full)
            .parse_mode(ParseMode::Html);
        if let Some(tid) = thread_id {
            req = req.message_thread_id(tid);
        }
        req.await?;
    } else {
        // Разбиваем текст на части (безопасно по границам символов)
        let max_text_len = 3800;
        let mut offset = 0;
        let mut part = 1;
        while offset < escaped_text.len() {
            let boundary = (offset + max_text_len).min(escaped_text.len());
            let safe_boundary = escaped_text.floor_char_boundary(boundary);

            let end = if safe_boundary >= escaped_text.len() {
                escaped_text.len()
            } else {
                escaped_text[offset..safe_boundary]
                    .rfind('\n')
                    .map(|pos| offset + pos)
                    .unwrap_or(safe_boundary)
            };

            let chunk = &escaped_text[offset..end];
            let msg_text = if part == 1 {
                format!("📝 <b>{escaped_title}</b>\n\n<blockquote expandable>{chunk}</blockquote>")
            } else {
                format!("<blockquote expandable>{chunk}</blockquote>")
            };

            let mut req = bot.send_message(chat_id, &msg_text)
                .parse_mode(ParseMode::Html);
            if let Some(tid) = thread_id {
                req = req.message_thread_id(tid);
            }
            req.await?;

            offset = end;
            if offset < escaped_text.len() && escaped_text.as_bytes()[offset] == b'\n' {
                offset += 1;
            }
            part += 1;
        }
    }
    Ok(())
}

async fn handle_lyrics(bot: &Bot, msg: &Message, query: &str) -> HandlerResult {
    let query = query.trim();
    if query.is_empty() {
        set_pending(msg.chat.id, msg_user_id(msg), PendingCommand::Lyrics).await;
        reply_text(bot, msg, "📝 Напиши название трека, для которого найти текст:").await?;
        return Ok(());
    }

    reply_text(bot, msg, "⏳ Ищу текст...").await?;
    match crate::lyrics::fetch_lyrics(query).await {
        Some(text) => {
            send_lyrics_message(bot, msg, query, &text).await?;
        }
        None => {
            reply_text(bot, msg, "😕 Текст не найден. Проверь написание или попробуй на английском.").await?;
        }
    }
    Ok(())
}

/// Создаёт клавиатуру с кнопкой "📝 Текст" если у пользователя включены тексты.
async fn make_lyrics_keyboard(user_id: u64, query: &str) -> Option<InlineKeyboardMarkup> {
    let settings = cache::get_user_settings(user_id).await;
    if !settings.show_lyrics {
        return None;
    }
    let cb_data = format!("lyrics:{}", truncate_str(query, 55));
    Some(InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("📝 Текст", cb_data)],
    ]))
}

fn settings_keyboard(settings: &cache::UserSettings) -> InlineKeyboardMarkup {
    let lyrics_label = if settings.show_lyrics {
        "📝 Тексты песен: ВКЛ ✅"
    } else {
        "📝 Тексты песен: ВЫКЛ ❌"
    };
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(lyrics_label, "set:lyrics")],
    ])
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
/// `is_forum` — показывать кнопку создания топика.
/// `has_topic` — показывать кнопку "в топик плейлиста" (для диапазонов).
/// Клавиатура выбора источника для скачивания плейлиста.
/// Показывает только Auto + YM (если токен есть). SC/VK/YT убраны.
/// Возвращает None если YM недоступен — тогда нужно скачивать автоматом.
fn source_keyboard(prefix: &str, is_forum: bool, has_topic: bool) -> Option<InlineKeyboardMarkup> {
    let has_ym = downloader::ym_available();

    if !has_ym {
        return None; // Без YM — автоскачивание, клавиатуру не показываем
    }

    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();

    // Кнопки топика — только для форум-групп
    if is_forum {
        rows.push(vec![InlineKeyboardButton::callback(
            "📌 В новый топик (авто)",
            format!("dl:{prefix}:auto:t"),
        )]);
        if has_topic {
            rows.push(vec![InlineKeyboardButton::callback(
                "📌 В топик плейлиста (авто)",
                format!("dl:{prefix}:auto:e"),
            )]);
        }
    }

    rows.push(vec![InlineKeyboardButton::callback("🔀 Авто", format!("dl:{prefix}:auto"))]);
    rows.push(vec![InlineKeyboardButton::callback("🎵 Яндекс.Музыка", format!("dl:{prefix}:ym"))]);

    Some(InlineKeyboardMarkup::new(rows))
}

/// Проверяет, является ли чат форумом (супергруппа с топиками).
fn is_forum_chat(chat: &teloxide::types::Chat) -> bool {
    use teloxide::types::{ChatKind, PublicChatKind};
    match &chat.kind {
        ChatKind::Public(public) => match &public.kind {
            PublicChatKind::Supergroup(sg) => sg.is_forum,
            _ => false,
        },
        _ => false,
    }
}

/// Проверяет, есть ли сохранённый топик для чата.
async fn has_saved_topic(chat_id: ChatId) -> bool {
    TOPIC_CACHE.lock().await.contains_key(&chat_id)
}

/// Получает сохранённый thread_id для чата.
async fn get_saved_topic(chat_id: ChatId) -> Option<ThreadId> {
    TOPIC_CACHE.lock().await.get(&chat_id).copied()
}

/// Сохраняет thread_id топика для чата.
async fn save_topic(chat_id: ChatId, thread_id: ThreadId) {
    TOPIC_CACHE.lock().await.insert(chat_id, thread_id);
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

    // Извлекаем thread_id из сообщения callback (для ответа в ту же тему)
    let cb_thread_id: Option<ThreadId> = msg.regular_message().and_then(|m| m.thread_id);

    // Хелпер: отправить сообщение в тот же тред
    macro_rules! cb_send {
        ($text:expr) => {{
            let mut r = bot.send_message(chat_id, $text);
            if let Some(tid) = cb_thread_id {
                r = r.message_thread_id(tid);
            }
            r.await
        }};
    }

    let parts: Vec<&str> = data.splitn(3, ':').collect();
    if parts.len() < 2 {
        return Ok(());
    }

    // Обработка выбора из поиска: "s:<index>"
    if parts[0] == "s" {
        bot.answer_callback_query(q.id.clone()).await?;
        let _ = bot.edit_message_reply_markup(chat_id, msg_id).await;

        let Ok(index) = parts[1].parse::<usize>() else {
            return Ok(());
        };

        let Some(result) = get_search_result(chat_id, q.from.id, index).await else {
            cb_send!("❌ Результат не найден. Попробуй поиск заново.")?;
            return Ok(());
        };

        let platform_name = result.platform.full_name();
        let cache_query = format!("{} - {}", result.artist, result.title);

        // === Кеш: проверяем file_id ===
        if let Some(cached) = cache::get(&cache_query).await {
            match downloader::send_cached(&bot, chat_id, &cached, cb_thread_id, Some(q.from.id.0), &cache_query).await {
                Ok(()) => {
                    log::info!("[CACHE] Отправлен из кеша: {cache_query}");
                    cb_send!("✅ Отправлен!")?;
                    return Ok(());
                }
                Err(e) => {
                    log::warn!("Кеш не сработал: {e:#}, качаю заново");
                }
            }
        }

        cb_send!(format!("⏳ Скачиваю из {}...", platform_name))?;

        let safe_name = downloader::sanitize_filename(&format!(
            "{} - {}",
            result.artist, result.title
        ));
        let output_path = PathBuf::from("downloads").join(format!("{safe_name}.mp3"));

        let download_result = match result.platform {
            Platform::YandexMusic => {
                crate::ym::download_track(&result.download_key, &output_path).await
            }
            Platform::Vk => {
                async {
                    let url = if result.download_key.is_empty() {
                        let search_q = format!("{} - {}", result.artist, result.title);
                        let token = crate::tokens::VK.next()
                            .ok_or_else(|| anyhow::anyhow!("VK_TOKEN не задан"))?;
                        let tracks = crate::vk::search_tracks(token, &search_q, 1).await?;
                        tracks.into_iter().next()
                            .ok_or_else(|| anyhow::anyhow!("VK: трек не найден"))?
                            .url
                    } else {
                        result.download_key.clone()
                    };
                    let bytes = crate::vk::download_audio(&url).await?;
                    if bytes.is_empty() {
                        anyhow::bail!("VK: пустой файл");
                    }
                    tokio::fs::write(&output_path, &bytes).await?;
                    Ok(())
                }.await
            }
            Platform::SoundCloud | Platform::YouTube => {
                if result.download_key.is_empty() {
                    // Из кеша поиска — URL нет, ищем заново
                    let prefix = if result.platform == Platform::SoundCloud { "sc" } else { "yt" };
                    let search_q = format!("{} - {}", result.artist, result.title);
                    match downloader::search_ytdlp_metadata(&search_q, prefix, 1).await {
                        Ok(results) if !results.is_empty() => {
                            downloader::download_by_url(&results[0].download_key, &output_path).await
                        }
                        _ => Err(anyhow::anyhow!("Не удалось найти URL для скачивания"))
                    }
                } else {
                    downloader::download_by_url(&result.download_key, &output_path).await
                }
            }
        };

        match download_result {
            Ok(()) => {
                let audio_bytes = tokio::fs::read(&output_path).await.unwrap_or_default();
                if !audio_bytes.is_empty() {
                    let input_file = teloxide::types::InputFile::memory(audio_bytes)
                        .file_name(format!("{safe_name}.mp3"));
                    let mut req = bot.send_audio(chat_id, input_file)
                        .title(&result.title)
                        .performer(&result.artist);

                    if let Some(tid) = cb_thread_id {
                        req = req.message_thread_id(tid);
                    }

                    // Обложка — пробуем для всех платформ (YM → yt-dlp fallback)
                    let cover_query = format!("{} - {}", result.artist, result.title);
                    if let Some(thumb) = downloader::fetch_thumbnail_by_query(&cover_query).await {
                        req = req.thumbnail(
                            teloxide::types::InputFile::memory(thumb)
                                .file_name("cover.jpg"),
                        );
                    }

                    // Кнопка текста — если у пользователя включена настройка
                    if let Some(kb) = make_lyrics_keyboard(q.from.id.0, &cover_query).await {
                        req = req.reply_markup(kb);
                    }

                    match req.await {
                        Ok(msg) => {
                            // === Кеш: сохраняем file_id ===
                            if let Some(audio) = msg.audio() {
                                cache::save(
                                    &cache_query,
                                    &audio.file.id.0,
                                    &result.artist,
                                    &result.title,
                                    result.duration_sec,
                                    result.platform.label(),
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            log::warn!("Ошибка отправки аудио: {e}");
                        }
                    }
                }
                tokio::fs::remove_file(&output_path).await.ok();
                cb_send!("✅ Отправлен!")?;
            }
            Err(e) => {
                tokio::fs::remove_file(&output_path).await.ok();
                cb_send!(format!("❌ {e}"))?;
            }
        }
        return Ok(());
    }

    // Обработка настроек: "set:lyrics"
    if data == "set:lyrics" {
        let new_val = cache::toggle_lyrics(q.from.id.0).await;
        let settings = cache::UserSettings { show_lyrics: new_val };
        let keyboard = settings_keyboard(&settings);
        let _ = bot.edit_message_text(chat_id, msg_id, "⚙️ Настройки:")
            .reply_markup(keyboard)
            .await;
        let status = if new_val { "включены ✅" } else { "выключены ❌" };
        bot.answer_callback_query(q.id.clone())
            .text(format!("Тексты песен {status}"))
            .await?;
        return Ok(());
    }

    // Обработка запроса текста: "lyrics:<query>"
    if let Some(lyrics_query) = data.strip_prefix("lyrics:") {
        bot.answer_callback_query(q.id.clone()).await?;
        cb_send!("⏳ Ищу текст...")?;
        match crate::lyrics::fetch_lyrics(lyrics_query).await {
            Some(text) => {
                send_lyrics_to_chat(&bot, chat_id, cb_thread_id, lyrics_query, &text).await?;
            }
            None => {
                cb_send!("😕 Текст не найден.")?;
            }
        }
        return Ok(());
    }

    // Обработка авто-поиска: "get:auto:<query>"
    if parts[0] == "get" && parts.len() == 3 {
        bot.answer_callback_query(q.id.clone()).await?;
        let _ = bot.edit_message_reply_markup(chat_id, msg_id).await;

        let value = parts[2];
        let track = if let Some(t) = yandex::parse_track_text(value) {
            t
        } else {
            Track::new("", value)
        };
        cb_send!("⏳ Авто-поиск...")?;
        match downloader::find_and_send_with_retry_source(&bot, chat_id, &track, Source::Auto, CancellationToken::new(), cb_thread_id, Some(q.from.id.0)).await {
            Ok(()) => {
                cb_send!("✅ Отправлен!")?;
            }
            Err(e) => {
                cb_send!(format!("❌ {e}"))?;
            }
        }
        return Ok(());
    }

    // Обработка скачивания плейлиста: "dl:<scope>:<source>" или "dl:<scope>:<source>:t" (новый топик) / ":e" (существующий)
    if parts[0] != "dl" {
        return Ok(());
    }

    // Парсим callback: dl:<scope>:<source>[:<topic_mode>]
    // topic_mode: "t" = новый топик, "e" = существующий топик
    let (topic_mode, data_without_topic) = if data.ends_with(":t") {
        (Some("t"), &data[..data.len() - 2])
    } else if data.ends_with(":e") {
        (Some("e"), &data[..data.len() - 2])
    } else {
        (None, data.as_str())
    };

    // Берём source из конца (после последнего :), scope — всё между первым и последним :
    let Some(last_colon) = data_without_topic.rfind(':') else { return Ok(()) };
    let source = parse_source(&data_without_topic[last_colon + 1..]);
    let scope = &data_without_topic[3..last_colon]; // пропускаем "dl:"

    bot.answer_callback_query(q.id.clone()).await?;

    // Удалим кнопки из сообщения
    let _ = bot.edit_message_reply_markup(chat_id, msg_id).await;

    let Some(playlist) = get_playlist(chat_id, q.from.id).await else {
        cb_send!("❌ Плейлист не найден. Отправь ссылку заново.")?;
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
        cb_send!("❌ Нет треков для скачивания.")?;
        return Ok(());
    }

    let count = tracks.len();
    let src_label = source.label();

    // === Определяем thread_id ===
    let thread_id: Option<ThreadId> = match topic_mode {
        Some("t") => {
            // Создаём новый форум-топик
            let topic_name = playlist
                .title
                .as_deref()
                .unwrap_or("🎵 Музыка");
            let topic_name = format!("🎵 {topic_name}");
            match bot.create_forum_topic(chat_id, &topic_name).await {
                Ok(topic) => {
                    let tid = topic.thread_id;
                    save_topic(chat_id, tid).await;
                    log::info!("[TOPIC] Создан топик «{topic_name}» для chat={chat_id}");
                    Some(tid)
                }
                Err(e) => {
                    log::warn!("Не удалось создать топик: {e}");
                    cb_send!(format!("⚠️ Не удалось создать топик: {e}\nСкачиваю в этот чат."))?;
                    None
                }
            }
        }
        Some("e") => {
            // Используем существующий топик
            get_saved_topic(chat_id).await
        }
        _ => cb_thread_id, // Если не создаём/не используем топик — отвечаем в тот же тред
    };

    log::info!("[DL] chat={chat_id}, треков={count}, источник={src_label}, топик={thread_id:?}");

    // Отправляем статус в нужный поток
    let mut start_msg = bot.send_message(
        chat_id,
        format!("📥 Скачиваю {count} треков ({src_label})...\nОстановить: /stop"),
    );
    if let Some(tid) = thread_id {
        start_msg = start_msg.message_thread_id(tid);
    }
    start_msg.await?;

    // Запускаем в фоне чтобы не блокировать обработку команд (в т.ч. /stop)
    let bot_clone = bot.clone();
    tokio::spawn(async move {
        if let Err(e) = download_tracks_parallel_source(&bot_clone, chat_id, &tracks, source, thread_id, Some(q.from.id.0)).await {
            log::error!("Ошибка скачивания: {e:#}");
            let mut err_msg = bot_clone.send_message(chat_id, format!("❌ Ошибка: {e}"));
            if let Some(tid) = thread_id {
                err_msg = err_msg.message_thread_id(tid);
            }
            let _ = err_msg.await;
        }
    });

    Ok(())
}
