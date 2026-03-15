use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use tokio::fs;

use yandex_music::YandexMusicClient;
use yandex_music::api::search::get_search::SearchOptions;
use yandex_music::api::track::get_file_info::GetFileInfoOptions;
use yandex_music::model::info::file_info::Quality;

use crate::models::Track;

/// Результат поиска трека в ЯМ.
#[derive(Debug, Clone)]
pub struct YmSearchResult {
    pub track_id: String,
    pub title: String,
    pub artist: String,
    pub album: Option<String>,
    pub duration_ms: Option<u64>,
    pub cover_url: Option<String>,
}

fn ym_token() -> Option<String> {
    std::env::var("YM_TOKEN").ok().filter(|t| !t.is_empty())
}

pub fn is_available() -> bool {
    ym_token().is_some()
}

fn build_client() -> Result<YandexMusicClient> {
    let token = ym_token().context("YM_TOKEN не задан")?;
    YandexMusicClient::builder(token)
        .build()
        .map_err(|e| anyhow::anyhow!("Ошибка клиента ЯМ: {e}"))
}

/// Ищет треки в ЯМ, возвращает список результатов.
pub async fn search_tracks(query: &str, limit: usize) -> Result<Vec<YmSearchResult>> {
    let client = build_client()?;
    let opts = SearchOptions::new(query);
    let search = client
        .search(&opts)
        .await
        .map_err(|e| anyhow::anyhow!("ЯМ поиск: {e}"))?;

    let Some(tracks) = search.tracks else {
        return Ok(Vec::new());
    };

    let results: Vec<YmSearchResult> = tracks
        .results
        .into_iter()
        .take(limit)
        .filter_map(|t| {
            let title = t.title.clone()?;
            let artist = t
                .artists
                .first()
                .and_then(|a| a.name.clone())
                .unwrap_or_default();
            let album = t.albums.first().and_then(|a| a.title.clone());
            let duration_ms = t.duration.map(|d| d.as_millis() as u64);
            let cover_url = t.cover_uri.as_ref().map(|uri| {
                let uri = uri.replace("%%", "400x400");
                if uri.starts_with("http") { uri } else { format!("https://{uri}") }
            });
            Some(YmSearchResult {
                track_id: t.id.clone(),
                title,
                artist,
                album,
                duration_ms,
                cover_url,
            })
        })
        .collect();

    Ok(results)
}

/// Скачивает трек из ЯМ по track_id в output_path.
pub async fn download_track(track_id: &str, output_path: &PathBuf) -> Result<()> {
    let client = build_client()?;

    let opts = GetFileInfoOptions::new(track_id).quality(Quality::Normal);

    let file_info = client
        .get_file_info(&opts)
        .await
        .map_err(|e| anyhow::anyhow!("ЯМ download info: {e}"))?;

    let url = if !file_info.url.is_empty() {
        file_info.url
    } else if let Some(u) = file_info.urls.first() {
        u.clone()
    } else {
        bail!("ЯМ: нет URL для скачивания трека {track_id}");
    };

    let resp = reqwest::get(&url)
        .await
        .context("ЯМ: ошибка загрузки аудио")?;

    if !resp.status().is_success() {
        bail!("ЯМ: HTTP {} при скачивании", resp.status());
    }

    let bytes = resp.bytes().await.context("ЯМ: чтение аудио")?;
    if bytes.is_empty() {
        bail!("ЯМ: пустой файл для {track_id}");
    }

    fs::write(output_path, &bytes).await.context("ЯМ: запись файла")?;
    Ok(())
}

/// Ищет трек и скачивает первый подходящий результат.
pub async fn search_and_download(track: &Track, output_path: &PathBuf) -> Result<()> {
    let query = track.search_query();
    let results = search_tracks(&query, 3).await?;

    if results.is_empty() {
        bail!("ЯМ: ничего не найдено для {query}");
    }

    download_track(&results[0].track_id, output_path).await
}
