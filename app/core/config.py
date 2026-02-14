from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    DB_URL: str

    model_config = {
        "env_file": ".env",
        "case_sensitive": True,
    }


settings = Settings()