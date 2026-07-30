from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from platform_integration.credentials import CredentialReference


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PLATFORM_INTEGRATION_", extra="forbid")

    database_url: SecretStr | None = None
    pdm_credential_ref: CredentialReference | None = None
