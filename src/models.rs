use serde::{Deserialize, Serialize};

/// Платформа-источник трека.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    YandexMusic,
    Vk,
    SoundCloud,
    YouTube,
}

impl Platform {
    pub fn emoji(self) -> &'static str {
        match self {
            Platform::YandexMusic => "🟡",
            Platform::Vk => "🟣",
            Platform::SoundCloud => "🟠",
            Platform::YouTube => "🔴",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Platform::YandexMusic => "YM",
            Platform::Vk => "VK",
            Platform::SoundCloud => "SC",
            Platform::YouTube => "YT",
        }
    }

    pub fn full_name(self) -> &'static str {
        match self {
            Platform::YandexMusic => "Яндекс.Музыка",
            Platform::Vk => "VK Music",
            Platform::SoundCloud => "SoundCloud",
            Platform::YouTube => "YouTube",
        }
    }
}

/// Результат поиска с любой платформы.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub platform: Platform,
    pub title: String,
    pub artist: String,
    pub duration_sec: Option<u32>,
    /// Ключ для скачивания: YM track_id, VK audio URL, SC/YT video URL.
    pub download_key: String,
}

impl SearchResult {
    /// Форматированная длительность (m:ss).
    pub fn duration_display(&self) -> String {
        match self.duration_sec {
            Some(s) => format!("{}:{:02}", s / 60, s % 60),
            None => "—:——".to_string(),
        }
    }

    /// Строка для списка результатов: "🟡 Artist — Title (3:42) — YM"
    pub fn display_line(&self, idx: usize) -> String {
        format!(
            "{}. {} {} — {} ({}) — {}",
            idx,
            self.platform.emoji(),
            html_escape(&self.artist),
            html_escape(&self.title),
            self.duration_display(),
            self.platform.label(),
        )
    }

    /// Короткий label для inline-кнопки.
    pub fn button_label(&self, idx: usize) -> String {
        let raw = format!(
            "{} {}. {} — {}",
            self.platform.emoji(),
            idx,
            self.artist,
            self.title,
        );
        // Telegram callback button label — ограничиваем для читаемости
        truncate_display(&raw, 55)
    }
}

/// Обрезает строку до max_chars символов (не байтов).
fn truncate_display(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars - 1).collect();
        format!("{truncated}…")
    }
}

/// Один трек.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Track {
    pub artist: String,
    pub title: String,
    pub album: Option<String>,
    pub duration_sec: Option<u32>,
}

impl Track {
    pub fn new(artist: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            artist: artist.into(),
            title: title.into(),
            album: None,
            duration_sec: None,
        }
    }

    pub fn with_album(mut self, album: impl Into<String>) -> Self {
        self.album = Some(album.into());
        self
    }

    /// "Исполнитель - Название" для поиска.
    pub fn search_query(&self) -> String {
        if self.artist.is_empty() {
            self.title.clone()
        } else {
            format!("{} - {}", self.artist, self.title)
        }
    }

    /// Форматирование для отображения пользователю (plain text).
    pub fn display(&self) -> String {
        match &self.album {
            Some(album) => format!("🎵 {} — {} ({})", self.artist, self.title, album),
            None => format!("🎵 {} — {}", self.artist, self.title),
        }
    }

    /// Форматирование с HTML-экранированием.
    pub fn display_html(&self) -> String {
        let artist = html_escape(&self.artist);
        let title = html_escape(&self.title);
        match &self.album {
            Some(album) => format!("🎵 {} — {} ({})", artist, title, html_escape(album)),
            None => format!("🎵 {} — {}", artist, title),
        }
    }
}

/// Результат парсинга плейлиста.
#[derive(Debug, Clone)]
pub struct Playlist {
    pub title: Option<String>,
    pub tracks: Vec<Track>,
}

impl Playlist {
    pub fn new(tracks: Vec<Track>) -> Self {
        Self {
            title: None,
            tracks,
        }
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn len(&self) -> usize {
        self.tracks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tracks.is_empty()
    }

    /// Разбивает список треков на страницы, обёрнутые в expandable blockquote (HTML).
    pub fn format_pages(&self, page_size: usize) -> Vec<String> {
        self.tracks
            .chunks(page_size)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let header = match &self.title {
                    Some(t) => format!("📋 {} (стр. {})\n", html_escape(t), chunk_idx + 1),
                    None => format!("📋 Плейлист (стр. {})\n", chunk_idx + 1),
                };

                let tracks_text: String = chunk
                    .iter()
                    .enumerate()
                    .map(|(i, track)| {
                        let global_idx = chunk_idx * page_size + i + 1;
                        format!("{}. {}", global_idx, track.display_html())
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!("{}<blockquote expandable>{}</blockquote>", header, tracks_text)
            })
            .collect()
    }
}

pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
