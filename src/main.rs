mod cache;
mod commands;
mod downloader;
mod handlers;
mod models;
mod vk;
mod yandex;
mod ym;

use teloxide::prelude::*;
use teloxide::dispatching::UpdateFilterExt;
use teloxide::types::Update;

fn setup_logger() {
    let colors = fern::colors::ColoredLevelConfig::new()
        .info(fern::colors::Color::Green)
        .warn(fern::colors::Color::Yellow)
        .error(fern::colors::Color::Red)
        .debug(fern::colors::Color::Cyan);

    let base = fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{} [{}] [{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                colors.color(record.level()),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        // Заглушаем спам от внутренних крейтов
        .level_for("hyper", log::LevelFilter::Warn)
        .level_for("reqwest", log::LevelFilter::Warn)
        .level_for("teloxide", log::LevelFilter::Warn)
        .level_for("html5ever", log::LevelFilter::Warn);

    // Консоль — всё
    let console = fern::Dispatch::new()
        .chain(std::io::stdout());

    // Файл — только warn/error (для анализа проблем)
    let file = fern::Dispatch::new()
        .level(log::LevelFilter::Warn)
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] [{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                message
            ))
        })
        .chain(fern::log_file("bot.log").expect("Не удалось открыть bot.log"));

    base.chain(console)
        .chain(file)
        .apply()
        .expect("Не удалось настроить логирование");
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    setup_logger();
    log::info!("Запускаю ym-telegram-bot...");

    if let Err(e) = downloader::init().await {
        log::error!("Не удалось инициализировать yt-dlp: {e:#}");
        std::process::exit(1);
    }
    log::info!("yt-dlp инициализирован");

    if let Err(e) = cache::init().await {
        log::error!("Не удалось инициализировать кеш: {e:#}");
        std::process::exit(1);
    }

    let bot = Bot::from_env();

    // Кешируем username бота один раз при старте
    let me = bot.get_me().await.expect("Не удалось получить информацию о боте");
    let bot_username = me.username.clone().unwrap_or_default();
    handlers::set_bot_username(bot_username);
    log::info!("Бот: @{}", me.username.as_deref().unwrap_or("?"));

    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .endpoint(|bot: Bot, msg: Message| async move {
                    handlers::process_message(bot, msg).await
                }),
        )
        .branch(
            Update::filter_callback_query()
                .endpoint(|bot: Bot, q: CallbackQuery| async move {
                    handlers::process_callback(bot, q).await
                }),
        )
        .branch(
            Update::filter_inline_query()
                .endpoint(|bot: Bot, q: InlineQuery| async move {
                    handlers::process_inline(bot, q).await
                }),
        );

    Dispatcher::builder(bot, handler)
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}
