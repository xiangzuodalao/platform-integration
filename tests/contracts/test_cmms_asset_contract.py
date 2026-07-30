from __future__ import annotations

import importlib
from uuid import UUID

import pytest
from pydantic import ValidationError


EQUIPMENT_ID = "00000000-0000-4000-8000-000000000101"


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


def test_asset_create_normalizes_standard_uppercase_uuid_and_emits_exact_body() -> None:
    """Failing to normalize a standard uppercase UUID would drift from the CMMS provider."""
    contracts = require_module(
        "platform_integration.contracts.cmms", "CMMS asset creation contract"
    )

    request = contracts.CmmsAssetCreate(
        name="Pilot CNC",
        equipment_id=EQUIPMENT_ID.upper(),
    )

    assert request.model_dump(mode="json") == {
        "name": "Pilot CNC",
        "equipment_id": EQUIPMENT_ID,
    }
    assert (
        contracts.CmmsAssetCreate(name="Pilot CNC", equipment_id=UUID(EQUIPMENT_ID)).model_dump(
            mode="json"
        )["equipment_id"]
        == EQUIPMENT_ID
    )


@pytest.mark.parametrize(
    "invalid",
    [
        "00000000000040008000000000000101",
        "{00000000-0000-4000-8000-000000000101}",
        "not-a-uuid",
        101,
    ],
)
def test_asset_contract_rejects_nonhyphenated_uuid_wire_values(invalid: object) -> None:
    """Accepting compact, braced, malformed, or numeric UUIDs would violate the CMMS wire form."""
    contracts = require_module("platform_integration.contracts.cmms", "strict CMMS UUID contract")

    with pytest.raises(ValidationError):
        contracts.CmmsAssetCreate(name="Pilot CNC", equipment_id=invalid)


def test_asset_create_forbids_provider_and_tenant_fields() -> None:
    """Forwarding fields beyond name and equipment_id could cross a service ownership boundary."""
    contracts = require_module(
        "platform_integration.contracts.cmms", "minimal CMMS asset create projection"
    )

    for payload in (
        {"name": "Pilot CNC", "equipment_id": EQUIPMENT_ID, "company_id": 7},
        {"name": "", "equipment_id": EQUIPMENT_ID},
    ):
        with pytest.raises(ValidationError):
            contracts.CmmsAssetCreate.model_validate(payload)


def test_asset_response_projects_provider_extras_without_copying_them() -> None:
    """Rejecting additive AssetShowDTO fields or retaining them would couple to the CMMS ORM shape."""
    contracts = require_module(
        "platform_integration.contracts.cmms", "additive CMMS asset response projection"
    )
    provider_response = {
        "id": 42,
        "name": "Pilot CNC",
        "equipment_id": EQUIPMENT_ID.upper(),
        "company_id": 7,
        "created_at": "2026-07-29T12:00:00Z",
        "custom_fields": {"line": "A"},
    }

    asset = contracts.CmmsAsset.from_provider(provider_response)

    assert asset.model_dump(mode="json") == {
        "id": 42,
        "name": "Pilot CNC",
        "equipment_id": EQUIPMENT_ID,
    }


@pytest.mark.parametrize(
    "mutation",
    [
        {"id": "42"},
        {"id": 0},
        {"name": ""},
        {"equipment_id": "00000000000040008000000000000101"},
    ],
)
def test_asset_response_rejects_invalid_projected_fields(mutation: dict) -> None:
    """Coercing the three projected identity fields would admit an invalid CMMS asset."""
    contracts = require_module(
        "platform_integration.contracts.cmms", "strict CMMS asset response projection"
    )
    payload = {
        "id": 42,
        "name": "Pilot CNC",
        "equipment_id": EQUIPMENT_ID,
        "provider_extra": "allowed",
        **mutation,
    }

    with pytest.raises(ValidationError):
        contracts.CmmsAsset.from_provider(payload)
