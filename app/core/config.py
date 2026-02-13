from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    BOT_TOKEN: str
    DB_URL: str
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    LOG_LEVEL: str = "INFO"

    model_config = {
        "env_file": ".env",
        "case_sensitive": True,
    }

    @property
    def TELEGRAM_BOT_TOKEN(self) -> str:
        return self.BOT_TOKEN

    @property
    def DATABASE_URL(self) -> str:
        return self.DB_URL


settings = Settings()