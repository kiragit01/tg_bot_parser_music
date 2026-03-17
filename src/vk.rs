use std::sync::LazyLock;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::models::Track;

const VK_API_URL: &str = "https://api.vk.com/method";
const VK_API_VERSION: &str = "5.95";

// User-Agent Kate Mobile — без него VK блокирует доступ к аудио
const VK_USER_AGENT: &str =
    "KateMobileAndroid/95 lite-523 (Android 13; SDK 33; arm64-v8a; Xiaomi M2101K6G; ru)";

/// Глобальный HTTP-клиент для VK API.
static VK_HTTP: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .user_agent(VK_USER_AGENT)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("reqwest client")
});

#[derive(Deserialize)]
struct VkResponse {
    response: Option<VkAudioResponse>,
    error: Option<VkError>,
}

#[derive(Deserialize)]
struct VkError {
    error_msg: String,
}

#[derive(Deserialize)]
struct VkAudioResponse {
    items: Vec<VkAudio>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct VkAudio {
    pub url: String,
    pub artist: String,
    pub title: String,
    pub duration: u32,
}

/// Ищет треки через VK API, возвращает до `limit` результатов с непустыми URL.
pub async fn search_tracks(token: &str, query: &str, limit: usize) -> Result<Vec<VkAudio>> {
    let count = (limit + 3).min(10).to_string(); // запрашиваем чуть больше — часть без URL

    let resp = VK_HTTP
        .get(format!("{VK_API_URL}/audio.search"))
        .query(&[
            ("q", query),
            ("count", count.as_str()),
            ("access_token", token),
            ("v", VK_API_VERSION),
        ])
        .send()
        .await
        .context("VK API запрос")?;

    let vk: VkResponse = resp.json().await.context("VK API десериализация")?;

    if let Some(err) = vk.error {
        bail!("VK API: {}", err.error_msg);
    }

    let items = vk.response.context("Пустой ответ VK API")?.items;

    let results: Vec<VkAudio> = items
        .into_iter()
        .filter(|a| !a.url.is_empty())
        .take(limit)
        .collect();

    Ok(results)
}

/// Ищет один трек через VK API (обратная совместимость).
pub async fn search_track(token: &str, track: &Track) -> Result<VkAudio> {
    let query = track.search_query();
    let results = search_tracks(token, &query, 1).await?;
    results
        .into_iter()
        .next()
        .context(format!("Не найден в VK: {query}"))
}

/// Скачивает mp3 по прямой ссылке из VK.
pub async fn download_audio(url: &str) -> Result<bytes::Bytes> {
    let bytes = VK_HTTP
        .get(url)
        .send()
        .await
        .context("Скачивание аудио из VK")?
        .bytes()
        .await
        .context("Чтение аудио из VK")?;

    Ok(bytes)
}
