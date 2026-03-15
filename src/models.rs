use serde::{Deserialize, Serialize};

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

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
