from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database - Render provides this automatically
    database_url: str = "postgresql+asyncpg://fluid_user:password@localhost:5432/fluid_db"

    # Security
    secret_key: str = "dev-secret-key-change-in-production"
    access_token_expire_minutes: int = 480

    # App
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "info"

    # Polling
    sensor_poll_interval: int = 30

    # Alerts
    alert_cooldown_minutes: int = 15

    # Plant identity
    plant_name: str = "Genome Valley Unit 7"
    plant_system: str = "Purified Water Loop A"
    plant_standard: str = "WHO / IP 2022"

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
