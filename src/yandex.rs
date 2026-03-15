use std::sync::LazyLock;

use anyhow::{Context, Result, bail};
use regex::Regex;
use scraper::{Html, Selector};

use crate::models::{Playlist, Track};

/// Предкомпилированные регулярные выражения.
static RE_IFRAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"iframe/playlist/([^/\s"]+)/(\d+)"#).unwrap());
static RE_LEGACY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"users/([^/]+)/playlists/(\d+)"#).unwrap());
static RE_UUID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"music\.yandex\.\w+/playlists/([0-9a-f-]+)"#).unwrap());

/// Глобальный HTTP-клиент.
static HTTP: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .user_agent(UA)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("reqwest client")
});

/// Проверяет, является ли текст URL/embed на Яндекс.Музыку.
pub fn is_yandex_music_url(text: &str) -> bool {
    text.contains("music.yandex.ru") || text.contains("music.yandex.com")
}

/// Тип входных данных для парсинга.
enum PlaylistInput {
    /// Старый формат URL: /users/{owner}/playlists/{id}
    Legacy { owner: String, playlist_id: String },
    /// Iframe embed код (содержит /iframe/playlist/{owner}/{id})
    Iframe { owner: String, playlist_id: String },
    /// Новый UUID формат — нужно резолвить через HTML
    Uuid { url: String },
}

/// Определяет тип входных данных.
fn classify_input(text: &str) -> Result<PlaylistInput> {
    // Iframe embed: ищем src="...iframe/playlist/{owner}/{id}"
    if let Some(caps) = RE_IFRAME.captures(text) {
        return Ok(PlaylistInput::Iframe {
            owner: caps[1].to_string(),
            playlist_id: caps[2].to_string(),
        });
    }

    // Старый формат: /users/{owner}/playlists/{id}
    if let Some(caps) = RE_LEGACY.captures(text) {
        return Ok(PlaylistInput::Legacy {
            owner: caps[1].to_string(),
            playlist_id: caps[2].to_string(),
        });
    }

    // UUID формат: /playlists/{uuid}
    if let Some(caps) = RE_UUID.captures(text) {
        let uuid = &caps[1];
        return Ok(PlaylistInput::Uuid {
            url: format!("https://music.yandex.ru/playlists/{uuid}"),
        });
    }

    bail!(
        "Не распознан формат. Поддерживаются:\n\
         • Ссылка: music.yandex.ru/users/USER/playlists/ID\n\
         • Ссылка: music.yandex.ru/playlists/UUID\n\
         • Iframe embed-код (кнопка «<> Код для вставки» на ЯМ)"
    )
}

/// Основная функция парсинга.
pub async fn parse_playlist(text: &str) -> Result<Playlist> {
    let input = classify_input(text)?;

    match input {
        PlaylistInput::Legacy { owner, playlist_id }
        | PlaylistInput::Iframe { owner, playlist_id } => {
            // Пробуем JSON API
            match parse_via_json_api(&owner, &playlist_id).await {
                Ok(pl) if !pl.is_empty() => {
                    log::info!("Плейлист через JSON API: {} треков", pl.len());
                    return Ok(pl);
                }
                Ok(_) => log::warn!("JSON API: пустой плейлист, пробую HTML..."),
                Err(e) => log::warn!("JSON API: {e:#}, пробую HTML..."),
            }

            // Fallback: HTML страницы плейлиста
            let url = format!(
                "https://music.yandex.ru/users/{owner}/playlists/{playlist_id}"
            );
            parse_via_html(&url).await
        }

        PlaylistInput::Uuid { url } => {
            // UUID URL: пробуем загрузить страницу и найти owner/id в HTML
            log::info!("UUID ссылка, пробую резолвить через HTML...");

            match resolve_uuid_playlist(&url).await {
                Ok(pl) if !pl.is_empty() => Ok(pl),
                Ok(_) | Err(_) => {
                    bail!(
                        "Не удалось распарсить UUID-ссылку.\n\n\
                         Яндекс.Музыка использует новый формат ссылок, который сложно парсить.\n\
                         Попробуй один из вариантов:\n\
                         1. Нажми «Поделиться» → «Код для вставки» и отправь iframe-код\n\
                         2. Экспортируй список через ymusicexport.ru и отправь текстом"
                    )
                }
            }
        }
    }
}

/// Пытается извлечь треки из UUID-страницы.
async fn resolve_uuid_playlist(url: &str) -> Result<Playlist> {
    let response = HTTP
        .get(url)
        .send()
        .await
        .context("Не удалось загрузить страницу")?;

    // Проверяем, не редиректнуло ли на старый формат
    let final_url = response.url().to_string();
    if let Some(caps) = RE_LEGACY.captures(&final_url) {
        let owner = caps[1].to_string();
        let playlist_id = caps[2].to_string();
        return parse_via_json_api(&owner, &playlist_id).await;
    }

    let html_text = response.text().await?;

    // Ищем iframe src или ссылки с owner/id внутри HTML (до создания Html, чтобы не держать !Send через .await)
    if let Some(caps) = RE_IFRAME.captures(&html_text) {
        let owner = caps[1].to_string();
        let playlist_id = caps[2].to_string();
        return parse_via_json_api(&owner, &playlist_id).await;
    }

    // Ищем owner/playlists в любом месте HTML
    if let Some(caps) = RE_LEGACY.captures(&html_text) {
        let owner = caps[1].to_string();
        let playlist_id = caps[2].to_string();
        return parse_via_json_api(&owner, &playlist_id).await;
    }

    // Парсим HTML (scraper::Html не Send, поэтому после всех .await)
    let document = Html::parse_document(&html_text);

    if let Some(pl) = extract_from_json_scripts(&document) {
        if !pl.is_empty() {
            return Ok(pl);
        }
    }

    if let Some(pl) = extract_from_html_elements(&document) {
        if !pl.is_empty() {
            return Ok(pl);
        }
    }

    bail!("Не удалось извлечь треки из UUID-страницы")
}

/// Парсинг через JSON API.
async fn parse_via_json_api(owner: &str, playlist_id: &str) -> Result<Playlist> {
    let url = format!(
        "https://music.yandex.ru/handlers/playlist.jsx?owner={owner}&kinds={playlist_id}"
    );

    let response = HTTP
        .get(&url)
        .header("Accept", "application/json")
        .header("X-Retpath-Y", "https://music.yandex.ru")
        .send()
        .await
        .context("Запрос к JSON API")?;

    if !response.status().is_success() {
        bail!("JSON API: статус {}", response.status());
    }

    let json: serde_json::Value = response.json().await.context("Десериализация JSON")?;

    let tracks_json = json
        .get("playlist")
        .and_then(|p| p.get("tracks"))
        .and_then(|t| t.as_array())
        .context("Не найден массив tracks в JSON")?;

    let title = json
        .get("playlist")
        .and_then(|p| p.get("title"))
        .and_then(|t| t.as_str())
        .map(String::from);

    let tracks = parse_tracks_from_json_array(tracks_json);

    let mut playlist = Playlist::new(tracks);
    if let Some(t) = title {
        playlist = playlist.with_title(t);
    }
    Ok(playlist)
}

/// Парсинг через HTML-скрейпинг.
async fn parse_via_html(url: &str) -> Result<Playlist> {
    let html_text = HTTP
        .get(url)
        .send()
        .await
        .context("Загрузка страницы")?
        .text()
        .await?;

    let document = Html::parse_document(&html_text);

    if let Some(pl) = extract_from_json_scripts(&document) {
        if !pl.is_empty() {
            return Ok(pl);
        }
    }

    if let Some(pl) = extract_from_html_elements(&document) {
        if !pl.is_empty() {
            return Ok(pl);
        }
    }

    bail!(
        "Не удалось извлечь треки из HTML.\n\
         Попробуй отправить iframe-код или текстовый список."
    )
}

fn extract_from_json_scripts(document: &Html) -> Option<Playlist> {
    let selector = Selector::parse("script[type='application/json']").ok()?;
    for script in document.select(&selector) {
        let text = script.text().collect::<String>();
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(tracks) = find_tracks_in_json(&json) {
                if !tracks.is_empty() {
                    return Some(Playlist::new(tracks));
                }
            }
        }
    }
    None
}

fn find_tracks_in_json(value: &serde_json::Value) -> Option<Vec<Track>> {
    if let Some(obj) = value.as_object() {
        if let Some(tracks_val) = obj.get("tracks") {
            if let Some(arr) = tracks_val.as_array() {
                let tracks = parse_tracks_from_json_array(arr);
                if !tracks.is_empty() {
                    return Some(tracks);
                }
            }
        }
        for (_key, val) in obj {
            if let Some(tracks) = find_tracks_in_json(val) {
                return Some(tracks);
            }
        }
    }
    if let Some(arr) = value.as_array() {
        for item in arr {
            if let Some(tracks) = find_tracks_in_json(item) {
                return Some(tracks);
            }
        }
    }
    None
}

fn parse_tracks_from_json_array(arr: &[serde_json::Value]) -> Vec<Track> {
    arr.iter()
        .filter_map(|t| {
            let title = t.get("title").and_then(|v| v.as_str())?;
            let artist = t
                .get("artists")
                .and_then(|a| a.as_array())
                .map(|artists| {
                    artists
                        .iter()
                        .filter_map(|a| a.get("name").and_then(|n| n.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .filter(|s| !s.is_empty())?;

            let album = t
                .get("albums")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .and_then(|a| a.get("title"))
                .and_then(|v| v.as_str())
                .map(String::from);

            let duration_sec = t
                .get("durationMs")
                .and_then(|d| d.as_u64())
                .map(|ms| (ms / 1000) as u32);

            let mut track = Track::new(artist, title);
            track.album = album;
            track.duration_sec = duration_sec;
            Some(track)
        })
        .collect()
}

fn extract_from_html_elements(document: &Html) -> Option<Playlist> {
    let selectors = [
        "div.d-track",
        "div[class*='track']",
        "div.lightlist__cont",
    ];
    for sel_str in &selectors {
        if let Ok(selector) = Selector::parse(sel_str) {
            let tracks: Vec<Track> = document
                .select(&selector)
                .filter_map(|el| {
                    let text = el.text().collect::<String>();
                    parse_track_text(&text)
                })
                .collect();
            if !tracks.is_empty() {
                return Some(Playlist::new(tracks));
            }
        }
    }
    None
}

/// Парсит текст "Artist — Title" или "Artist - Title" в Track.
pub fn parse_track_text(text: &str) -> Option<Track> {
    let text = text.trim();
    for separator in [" — ", " – ", " - "] {
        if let Some((artist, title)) = text.split_once(separator) {
            let artist = artist.trim();
            let title = title.trim();
            if !artist.is_empty() && !title.is_empty() {
                return Some(Track::new(artist, title));
            }
        }
    }
    None
}

const UA: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_yandex_music_url() {
        assert!(is_yandex_music_url("https://music.yandex.ru/users/test/playlists/123"));
        assert!(is_yandex_music_url("https://music.yandex.ru/playlists/1ba19a0b-bf6a-6699-9be8-ea53e9ff7fac"));
        assert!(!is_yandex_music_url("https://youtube.com/watch?v=abc"));
    }

    #[test]
    fn test_classify_legacy() {
        let result = classify_input("https://music.yandex.ru/users/johndoe/playlists/42").unwrap();
        match result {
            PlaylistInput::Legacy { owner, playlist_id } => {
                assert_eq!(owner, "johndoe");
                assert_eq!(playlist_id, "42");
            }
            _ => panic!("Expected Legacy"),
        }
    }

    #[test]
    fn test_classify_uuid() {
        let result = classify_input("https://music.yandex.ru/playlists/1ba19a0b-bf6a-6699-9be8-ea53e9ff7fac?utm_source=web").unwrap();
        assert!(matches!(result, PlaylistInput::Uuid { .. }));
    }

    #[test]
    fn test_classify_iframe() {
        let iframe = r#"<iframe src="https://music.yandex.ru/iframe/playlist/kirill.2-07/1026"></iframe>"#;
        let result = classify_input(iframe).unwrap();
        match result {
            PlaylistInput::Iframe { owner, playlist_id } => {
                assert_eq!(owner, "kirill.2-07");
                assert_eq!(playlist_id, "1026");
            }
            _ => panic!("Expected Iframe"),
        }
    }

    #[test]
    fn test_parse_track_text() {
        let track = parse_track_text("Metallica — Enter Sandman").unwrap();
        assert_eq!(track.artist, "Metallica");
        assert_eq!(track.title, "Enter Sandman");

        assert!(parse_track_text("just some text").is_none());
    }
}
