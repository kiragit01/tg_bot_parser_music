use teloxide::utils::command::BotCommands;

/// Команды бота.
#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "lowercase", description = "Доступные команды:")]
pub enum Command {
    #[command(description = "Начать работу с ботом")]
    Start,

    #[command(description = "Показать это сообщение")]
    Help,

    #[command(description = "Парсить плейлист: /parse <ссылка на ЯМ>")]
    Parse(String),

    #[command(description = "Найти и скачать трек: /get <исполнитель - название>")]
    Get(String),

    #[command(description = "Скачать все треки из последнего плейлиста")]
    DownloadAll,

    #[command(description = "Скачать треки: /download 30-60 или 1,5,10")]
    Download(String),

    #[command(description = "Остановить скачивание")]
    Stop,

    #[command(description = "Статус текущей задачи")]
    Status,

    #[command(description = "Настройки (тексты песен и др.)")]
    Settings,

    #[command(description = "Текст песни: /lyrics Исполнитель - Название")]
    Lyrics(String),
}
