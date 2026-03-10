from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    anthropic_api_key: str
    telegram_bot_token: str
    telegram_chat_id: str

    gmail_client_id: str
    gmail_client_secret: str
    gmail_refresh_token: str

    database_url: str

    # Extraction settings
    birthday_confidence_threshold: float = 0.6
    action_confidence_threshold: float = 0.65
    extraction_batch_size: int = 20

    # Crawler settings
    birthday_lookback_days: int = 730
    action_lookback_days: int = 60
    gmail_max_retries: int = 3
    gmail_retry_base_delay: float = 1.0

    # Feedback settings
    feedback_example_count: int = 10
    adaptive_threshold_enabled: bool = True

    # Bot settings
    bot_poll_interval: float = 2.0


settings = Settings()
