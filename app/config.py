from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: str = Field(default="", min_length=1)
    webhook_signing_secret: str = Field(default="", min_length=1)
    webhook_allow_private: bool = False
    enable_docs: bool = False
    outbox_poll_seconds: float = Field(default=0.5, gt=0)
    outbox_lease_seconds: int = Field(default=30, gt=0)
    gateway_lease_seconds: int = Field(default=30, ge=10)
    webhook_lease_seconds: int = Field(default=30, ge=10)
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    retry_initial_seconds: float = Field(default=1.0, gt=0)
    webhook_timeout_seconds: float = Field(default=5.0, gt=0)
    metrics_port: int = Field(default=9100, ge=1, le=65535)
    api_metrics_port: int = Field(default=9200, ge=1, le=65535)

    @model_validator(mode="after")
    def check_leases(self):
        if self.gateway_lease_seconds <= self.gateway_max_delay:
            raise ValueError("gateway lease must exceed maximum gateway delay")
        if self.webhook_lease_seconds <= self.webhook_timeout_seconds:
            raise ValueError("webhook lease must exceed webhook timeout")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
