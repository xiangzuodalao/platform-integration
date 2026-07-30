from uuid import UUID

from pydantic import HttpUrl
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from platform_integration.credentials import CredentialReference


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PLATFORM_INTEGRATION_", extra="forbid")

    database_url: SecretStr | None = None
    tenant_alias: str | None = None
    tenant_id: UUID | None = None
    isolated_pilot_mode: bool = False
    pdm_base_url: HttpUrl | None = None
    tb_base_url: HttpUrl | None = None
    cmms_base_url: HttpUrl | None = None
    tb_tenant_id: UUID | None = None
    cmms_company_id: int | None = None
    pdm_credential_ref: CredentialReference | None = None
    tb_credential_ref: CredentialReference | None = None
    cmms_credential_ref: CredentialReference | None = None
    cmms_webhook_secret_ref: CredentialReference | None = None
