mod commands;
mod downloader;
mod handlers;
mod models;
mod vk;
mod yandex;

use teloxide::prelude::*;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    pretty_env_logger::init();
    log::info!("Запускаю ym-telegram-bot...");

    if let Err(e) = downloader::init().await {
        log::error!("Не удалось инициализировать yt-dlp: {e:#}");
        std::process::exit(1);
    }
    log::info!("yt-dlp инициализирован");

    let bot = Bot::from_env();

    teloxide::repl(bot, |bot: Bot, msg: Message| async move {
        handlers::process_message(bot, msg).await
    })
    .await;
}
