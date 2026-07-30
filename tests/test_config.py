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
