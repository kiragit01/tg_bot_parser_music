use scraper::{Html, Selector};

const GENIUS_SEARCH_URL: &str = "https://api.genius.com/search";

fn genius_token() -> Option<String> {
    std::env::var("GENIUS_TOKEN").ok().filter(|t| !t.is_empty())
}

/// Ищет и возвращает текст песни с Genius. Возвращает None если не найден.
pub async fn fetch_lyrics(query: &str) -> Option<String> {
    let token = genius_token()?;

    // 1. Поиск трека через Genius API
    let client = reqwest::Client::new();
    let resp = client
        .get(GENIUS_SEARCH_URL)
        .query(&[("q", query)])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .ok()?;

    let json: serde_json::Value = resp.json().await.ok()?;
    let hits = json["response"]["hits"].as_array()?;

    // Ищем лучший хит: пропускаем переводы и нерелевантные результаты
    let song_url = find_best_hit(hits, query)?;

    // 2. Скрапим текст со страницы Genius
    let lyrics = scrape_lyrics_from_page(&song_url).await?;

    // 3. Проверяем что это реально текст песни, а не мусор
    if looks_like_lyrics(&lyrics) {
        Some(lyrics)
    } else {
        log::debug!("Genius: результат не похож на текст песни для «{query}»");
        None
    }
}

/// Находит лучший хит из результатов Genius, пропуская переводы.
fn find_best_hit(hits: &[serde_json::Value], query: &str) -> Option<String> {
    let query_lower = query.to_lowercase();

    // Извлекаем ключевые слова из запроса для проверки релевантности
    let query_words: Vec<&str> = query_lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 2)
        .collect();

    let mut best_url: Option<String> = None;
    let mut best_is_translation = false;

    for hit in hits.iter().take(5) {
        let result = &hit["result"];
        let url = result["url"].as_str().unwrap_or("");
        let title = result["title"].as_str().unwrap_or("").to_lowercase();
        let full_title = result["full_title"].as_str().unwrap_or("").to_lowercase();
        let artist = result["primary_artist"]["name"].as_str().unwrap_or("").to_lowercase();

        // Пропускаем результаты без URL
        if url.is_empty() {
            continue;
        }

        // Проверяем релевантность: хотя бы одно слово из запроса в названии или артисте
        let is_relevant = query_words.iter().any(|w| {
            title.contains(w) || artist.contains(w)
        });
        if !is_relevant && !query_words.is_empty() {
            continue;
        }

        // Определяем, является ли это переводом
        let is_translation = full_title.contains("translation")
            || title.contains("english translation")
            || title.contains("перевод")
            || url.contains("-translation");

        // Предпочитаем оригинал над переводом
        if best_url.is_none() || (best_is_translation && !is_translation) {
            best_url = Some(url.to_string());
            best_is_translation = is_translation;
        }

        // Если нашли релевантный оригинал — сразу берём
        if !is_translation {
            return Some(url.to_string());
        }
    }

    best_url
}

/// Проверяет, похож ли текст на настоящий текст песни.
fn looks_like_lyrics(text: &str) -> bool {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();

    // Слишком мало строк — скорее всего не текст песни
    if lines.len() < 4 {
        return false;
    }

    // Если есть метки куплетов/припевов — точно текст
    let has_sections = lines.iter().any(|l| {
        let t = l.trim();
        (t.starts_with('[') && t.ends_with(']'))
            || t.starts_with("[Куплет")
            || t.starts_with("[Припев")
            || t.starts_with("[Verse")
            || t.starts_with("[Chorus")
            || t.starts_with("[Intro")
            || t.starts_with("[Интро")
    });
    if has_sections {
        return true;
    }

    // Средняя длина строки: тексты песен обычно 20-80 символов
    let avg_len: usize = lines.iter().map(|l| l.len()).sum::<usize>() / lines.len().max(1);
    if avg_len > 150 {
        // Слишком длинные строки — скорее всего описание/статья
        return false;
    }

    // Специфичные слова-маркеры описаний (не текстов песен)
    let description_markers = [
        "разбираем", "релизы", "стрим", "подписывайтесь", "донат",
        "дата проведения", "фрешсет", "канала твича",
    ];
    let text_lower = text.to_lowercase();
    let marker_count = description_markers.iter().filter(|m| text_lower.contains(**m)).count();
    if marker_count >= 2 {
        return false;
    }

    true
}

async fn scrape_lyrics_from_page(url: &str) -> Option<String> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .build()
        .ok()?;

    let html = client.get(url).send().await.ok()?.text().await.ok()?;
    let document = Html::parse_document(&html);

    // Genius использует div[data-lyrics-container="true"] для блоков текста
    let lyrics_selector = Selector::parse("[data-lyrics-container='true']").ok()?;

    let mut lyrics = String::new();
    for element in document.select(&lyrics_selector) {
        extract_text_with_breaks(&element, &mut lyrics);
        lyrics.push('\n');
    }

    let lyrics = clean_lyrics(&lyrics);
    if lyrics.is_empty() { None } else { Some(lyrics) }
}

/// Рекурсивно извлекает текст из HTML, заменяя <br> на \n.
/// Пропускает <img>, <script>, <style> и другие нетекстовые элементы.
fn extract_text_with_breaks(node: &scraper::ElementRef, output: &mut String) {
    for child in node.children() {
        match child.value() {
            scraper::node::Node::Text(text) => {
                output.push_str(text);
            }
            scraper::node::Node::Element(el) => {
                let tag = el.name();
                if tag == "br" {
                    output.push('\n');
                } else if tag == "img" || tag == "script" || tag == "style" || tag == "svg" {
                    // игнорируем
                } else {
                    if let Some(child_ref) = scraper::ElementRef::wrap(child) {
                        extract_text_with_breaks(&child_ref, output);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Очищает текст от мусора Genius (Contributors, Translations, заголовки и т.д.)
fn clean_lyrics(raw: &str) -> String {
    let mut lines: Vec<&str> = Vec::new();
    let mut found_section = false;

    for line in raw.lines() {
        let trimmed = line.trim();

        // Пропускаем мусор
        if is_genius_junk(trimmed) {
            continue;
        }

        // Пропускаем строки до первой секции [Куплет], [Verse] и т.д. или первой пустой строки после текста
        if !found_section {
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                found_section = true;
            } else if trimmed.is_empty() {
                continue;
            } else {
                // Текст до первой секции — проверяем, мусор ли это
                let lower = trimmed.to_lowercase();
                if lower.contains("read more")
                    || lower.contains("lyrics")
                    || lower.contains("текст песни")
                    || lower.ends_with('…')
                    || lower.ends_with("...")
                {
                    continue;
                }
                // Если нет секций вообще, берём текст как есть
                found_section = true;
            }
        }

        lines.push(line);
    }

    // Убираем trailing пустые строки
    while lines.last().map_or(false, |l| l.trim().is_empty()) {
        lines.pop();
    }

    lines.join("\n")
}

/// Определяет, является ли строка мусором со страницы Genius.
fn is_genius_junk(line: &str) -> bool {
    let lower = line.to_lowercase();

    // "24 Contributors" / "2 ContributorsTranslations..."
    if lower.contains("contributor") {
        return true;
    }
    // "Translations"
    if lower.starts_with("translations") {
        return true;
    }
    // "... (English Translation) Lyrics" и подобное
    if lower.contains("translation") && lower.contains("lyrics") {
        return true;
    }
    // img/class теги просочившиеся как текст
    if line.contains("<img ") || line.contains("class=\"") || line.contains("src=\"") {
        return true;
    }
    // "Embed"
    if line.trim() == "Embed" {
        return true;
    }
    // "You might also like"
    if lower.contains("you might also like") {
        return true;
    }
    // "See [Artist] Live"
    if lower.starts_with("see ") && lower.ends_with(" live") {
        return true;
    }
    // "Read More"
    if lower.contains("read more") {
        return true;
    }
    // Genius promo
    if lower.contains("how to format lyrics") || lower.contains("sign up") {
        return true;
    }
    false
}
