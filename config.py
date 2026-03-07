from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    anthropic_api_key: str
    telegram_bot_token: str
    telegram_chat_id: str

    gmail_client_id: str
    gmail_client_secret: str
    gmail_refresh_token: str

    database_path: str = "birthdays.db"


settings = Settings()
