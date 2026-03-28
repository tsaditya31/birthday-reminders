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

    # Google Calendar (reuses gmail_client_id/secret)
    google_calendar_refresh_token: str = ""

    # Extraction settings
    birthday_confidence_threshold: float = 0.6
    action_confidence_threshold: float = 0.65
    extraction_batch_size: int = 20

    # Crawler settings
    birthday_lookback_days: int = 730
    action_lookback_days: int = 180
    gmail_max_retries: int = 3
    gmail_retry_base_delay: float = 1.0

    # Extraction versioning — bump when extraction logic changes meaningfully
    extraction_version: int = 1

    # Feedback settings
    feedback_example_count: int = 10
    adaptive_threshold_enabled: bool = True

    # Bot settings
    bot_poll_interval: float = 2.0

    # Timezone
    user_timezone: str = "America/New_York"

    # Heartbeat settings
    heartbeat_interval_hours: float = 4.0
    proactive_alerts_enabled: bool = True
    max_clarifications_per_tick: int = 2
    digest_hour_start: int = 6
    digest_hour_end: int = 10


settings = Settings()
