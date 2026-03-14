use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::models::Track;

const VK_API_URL: &str = "https://api.vk.com/method";
const VK_API_VERSION: &str = "5.95";

// User-Agent Kate Mobile — без него VK блокирует доступ к аудио
const VK_USER_AGENT: &str =
    "KateMobileAndroid/95 lite-523 (Android 13; SDK 33; arm64-v8a; Xiaomi M2101K6G; ru)";

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
pub struct VkAudio {
    pub url: String,
    pub artist: String,
    pub title: String,
    pub duration: u32,
}

fn vk_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(VK_USER_AGENT)
        .build()
        .expect("reqwest client")
}

/// Ищет трек через VK API.
pub async fn search_track(token: &str, track: &Track) -> Result<VkAudio> {
    let query = track.search_query();

    let resp = vk_client()
        .get(format!("{VK_API_URL}/audio.search"))
        .query(&[
            ("q", query.as_str()),
            ("count", "5"),
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

    if items.is_empty() {
        bail!("Не найден в VK: {}", track.search_query());
    }

    let audio = items
        .into_iter()
        .find(|a| !a.url.is_empty())
        .context(format!("VK: все результаты без URL для {}", track.search_query()))?;

    Ok(audio)
}

/// Скачивает mp3 по прямой ссылке из VK.
pub async fn download_audio(url: &str) -> Result<Vec<u8>> {
    let bytes = vk_client()
        .get(url)
        .send()
        .await
        .context("Скачивание аудио из VK")?
        .bytes()
        .await
        .context("Чтение аудио из VK")?;

    Ok(bytes.to_vec())
}
