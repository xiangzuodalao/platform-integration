from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PLATFORM_INTEGRATION_", extra="forbid")

    pdm_credential_ref: str | None = None
