use teloxide::utils::command::BotCommands;

/// Команды бота.
#[derive(BotCommands, Clone)]
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

    #[command(description = "Статус текущей задачи")]
    Status,
}
