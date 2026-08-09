import importlib
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError


sys.path.insert(0, str(Path(__file__).parents[1] / "src"))


def require_module(name: str, behaviour: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        target_or_parent = {
            ".".join(name.split(".")[:index]) for index in range(1, len(name.split(".")) + 1)
        }
        if exc.name not in target_or_parent:
            raise
        assert False, f"{behaviour} is unavailable: {name} has not been implemented"


def test_settings_reads_the_prefixed_external_credential_reference(monkeypatch):
    """Breaking the PLATFORM_INTEGRATION_ prefix must fail this configuration contract."""
    config = require_module("platform_integration.config", "prefixed credential reference settings")
    monkeypatch.setenv("PLATFORM_INTEGRATION_PDM_CREDENTIAL_REF", "PDM_PILOT_CREDENTIAL")

    assert config.Settings().pdm_credential_ref == "PDM_PILOT_CREDENTIAL"


def test_settings_rejects_unknown_constructor_fields():
    """Allowing unrecognised settings must fail this configuration contract."""
    config = require_module("platform_integration.config", "strict settings validation")

    with pytest.raises(ValidationError) as exc_info:
        config.Settings(unexpected_setting="not-allowed")

    error = exc_info.value.errors()[0]
    assert error["loc"] == ("unexpected_setting",)
    assert error["type"] == "extra_forbidden"


def test_settings_has_no_builtin_credential_when_external_reference_is_absent(monkeypatch):
    """Synthesising a credential without an environment reference must fail this safety contract."""
    config = require_module(
        "platform_integration.config", "empty external credential reference handling"
    )
    monkeypatch.delenv("PLATFORM_INTEGRATION_PDM_CREDENTIAL_REF", raising=False)

    assert config.Settings().pdm_credential_ref is None


def test_isolated_provisioning_settings_are_external_and_exact():
    """Implicit tenant identities or endpoints could direct writes outside the isolated pilot."""
    config = require_module("platform_integration.config", "isolated provisioning settings")
    settings = config.Settings(
        tenant_alias="ifactory-pilot",
        tenant_id="00000000-0000-4000-8000-000000000001",
        isolated_pilot_mode=True,
        tb_base_url="https://tb.invalid",
        cmms_base_url="https://cmms.invalid",
        tb_tenant_id="00000000-0000-4000-8000-000000000001",
        cmms_company_id=201,
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
    )

    assert str(settings.tenant_id) == "00000000-0000-4000-8000-000000000001"
    assert settings.isolated_pilot_mode is True
    assert str(settings.tb_base_url) == "https://tb.invalid/"
    assert str(settings.cmms_base_url) == "https://cmms.invalid/"


def test_closed_loop_is_off_by_default_and_polling_is_bounded():
    """An accidental default-on write path or tight polling loop would break Phase 2 safety."""
    config = require_module("platform_integration.config", "closed-loop settings")

    assert config.Settings().closed_loop_enabled is False
    assert config.Settings().feedback_poll_seconds == 300
    with pytest.raises(ValidationError):
        config.Settings(feedback_poll_seconds=29)


def test_optional_bootstrap_identities_accept_empty_environment_values(monkeypatch):
    """Migrate and provision must parse before runtime writes back discovered typed identities."""
    config = require_module("platform_integration.config", "bootstrap optional identities")
    for name in (
        "TB_TENANT_ID",
        "CMMS_COMPANY_ID",
        "APPROVER_TB_USER_ID",
        "PILOT_WORK_ORDER_EQUIPMENT_ID",
    ):
        monkeypatch.setenv(f"PLATFORM_INTEGRATION_{name}", "")

    settings = config.Settings()

    assert settings.tb_tenant_id is None
    assert settings.cmms_company_id is None
    assert settings.approver_tb_user_id is None
    assert settings.pilot_work_order_equipment_id is None


@pytest.mark.parametrize(
    "invalid",
    [
        "AB",
        "pdm_pilot_credential",
        "1PDM_PILOT_CREDENTIAL",
        "PDM-PILOT-CREDENTIAL",
        "${PDM_PILOT_CREDENTIAL}",
        "A" * 129,
    ],
)
def test_settings_rejects_non_environment_variable_references(monkeypatch, invalid: str):
    """Accepting indirect or ambiguous references would weaken direct environment lookup."""
    config = require_module("platform_integration.config", "credential reference validation")
    monkeypatch.setenv("PLATFORM_INTEGRATION_PDM_CREDENTIAL_REF", invalid)

    with pytest.raises(ValidationError):
        config.Settings()


@pytest.mark.parametrize("length", [3, 128])
def test_settings_accepts_credential_reference_length_boundaries(monkeypatch, length: int):
    """Rejecting a frozen regex boundary would make valid deployment references unusable."""
    config = require_module("platform_integration.config", "credential reference boundaries")
    reference = "A" * length
    monkeypatch.setenv("PLATFORM_INTEGRATION_PDM_CREDENTIAL_REF", reference)

    assert config.Settings().pdm_credential_ref == reference
