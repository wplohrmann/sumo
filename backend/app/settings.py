from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="SUMO_", extra="ignore")

    database_url: str = Field(
        default="postgresql+asyncpg://sumo:sumo@localhost:5433/sumo",
        description="Async SQLAlchemy URL for the app database.",
    )
    admin_password: str = Field(
        default="change-me",
        description="Plain-text admin password set via env in prod.",
    )
    session_secret: str = Field(
        default="dev-secret-change-me",
        description="HMAC secret for signing the session cookie.",
    )
    cookie_name: str = Field(default="sumo_session")
    cookie_secure: bool = Field(default=False)
    sumo_api_base_url: str = Field(default="https://www.sumo-api.com/api")
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:5173"])


@lru_cache
def get_settings() -> Settings:
    return Settings()
