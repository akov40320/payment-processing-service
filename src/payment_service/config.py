from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "payment-processing-service"
    environment: str = "development"
    api_key: SecretStr = Field(min_length=16)
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    payment_processing_min_delay: float = Field(default=2, ge=0)
    payment_processing_max_delay: float = Field(default=5, gt=0)
    payment_success_rate: float = Field(default=0.9, ge=0, le=1)
    webhook_timeout_seconds: float = Field(default=5, gt=0)
    outbox_poll_interval_seconds: float = Field(default=1, gt=0)
    webhook_poll_interval_seconds: float = Field(default=1, gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
