use teloxide::utils::command::BotCommands;

/// Команды бота.
#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "lowercase", description = "Команды:")]
pub enum Command {
    #[command(description = "Приветствие")]
    Start,

    #[command(description = "Список команд")]
    Help,

    #[command(description = "Плейлист ЯМ → список треков")]
    Parse(String),

    #[command(description = "Скачать трек: /get Артист - Трек")]
    Get(String),

    #[command(description = "Скачать весь плейлист")]
    DownloadAll,

    #[command(description = "Скачать выборочно: /download 1-20")]
    Download(String),

    #[command(description = "Остановить загрузку")]
    Stop,

    #[command(description = "Статус загрузки")]
    Status,

    #[command(description = "Настройки")]
    Settings,

    #[command(description = "Текст песни: /lyrics Артист - Трек")]
    Lyrics(String),
}
