from uuid import UUID

from pydantic import Field, HttpUrl, SecretStr, field_validator
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
    closed_loop_enabled: bool = False
    approver_tb_user_id: UUID | None = None
    pilot_work_order_equipment_id: UUID | None = None
    feedback_poll_seconds: int = Field(default=300, ge=30, le=3600)

    @field_validator(
        "tb_tenant_id",
        "cmms_company_id",
        "approver_tb_user_id",
        "pilot_work_order_equipment_id",
        mode="before",
    )
    @classmethod
    def empty_optional_identity_is_unset(cls, value: object) -> object:
        return None if value == "" else value
