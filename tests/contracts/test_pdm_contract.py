from __future__ import annotations

import importlib
from copy import deepcopy
from uuid import UUID

import pytest
import rfc8785
from pydantic import ValidationError


REQUEST_DIGEST = "067096fc115546164d8608cddadc56b185fefea9dffab4c7462c8d28011cdfbd"
INPUT_DIGEST = "c67fe58c14f2e8a1ef252e6ff47b4877c0da8fa52664772dd3c08ae3d6552609"
TENANT_ID = "00000000-0000-4000-8000-000000000001"
CORRELATION_ID = "00000000-0000-4000-8000-000000000102"
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


def request_payload() -> dict:
    return {
        "tenant_id": TENANT_ID,
        "correlation_id": CORRELATION_ID,
        "equipment_id": EQUIPMENT_ID,
        "model_profile_id": "pilot-cnc-vibration",
        "model_info_id": "pilot-fixture-v1-cnc-vibration",
        "meas_code": "vibration_rms",
        "unit": "mm/s",
        "sampling_frequency": "1min",
        "window_start": 1785283740000,
        "window_end": 1785287700000,
        "request_digest": REQUEST_DIGEST,
        "history": [
            {
                "data_id": f"fixture-{index:03d}",
                "timestamp": 1785283740000 + index * 60_000,
                "value": "4.00",
                "unit": "mm/s",
            }
            for index in range(66)
        ],
    }


def response_payload() -> dict:
    return {
        "correlation_id": CORRELATION_ID,
        "equipment_id": EQUIPMENT_ID,
        "model_profile_id": "pilot-cnc-vibration",
        "model_info_id": "pilot-fixture-v1-cnc-vibration",
        "model_artifact_sha256": "a" * 64,
        "meas_code": "vibration_rms",
        "request_digest": REQUEST_DIGEST,
        "input_digest": INPUT_DIGEST,
        "generated_at": "2026-07-29T12:00:00Z",
        "forecast": [
            {
                "timestamp": 1785287700000 + index * 60_000,
                "value": "4.00",
                "unit": "mm/s",
            }
            for index in range(15)
        ],
    }


def test_request_digest_matches_the_frozen_rfc8785_example() -> None:
    """Changing the projection or canonical encoding must break the shared digest literal."""
    models = require_module("platform_integration.contracts.pdm", "PDM request contract")
    canonical = require_module(
        "platform_integration.contracts.canonical", "RFC 8785 request digest"
    )
    payload = request_payload()
    payload["history"] = list(reversed(payload["history"]))

    request = models.PredictionRequestV2.model_validate(payload)
    projection = canonical.prediction_request_projection(request)

    assert tuple(projection) == (
        "tenant_id",
        "equipment_id",
        "model_profile_id",
        "model_info_id",
        "meas_code",
        "unit",
        "sampling_frequency",
        "window_start",
        "window_end",
        "history",
    )
    assert projection["history"][0]["data_id"] == "fixture-000"
    assert "correlation_id" not in projection
    assert "request_digest" not in projection
    assert canonical.calculate_request_digest(request) == REQUEST_DIGEST
    assert rfc8785.dumps(projection).startswith(b'{"equipment_id":')


def test_request_digest_orders_equal_timestamps_by_data_id() -> None:
    """Dropping the data-id tie breaker would make duplicate-timestamp digests unstable."""
    models = require_module("platform_integration.contracts.pdm", "PDM request contract")
    canonical = require_module(
        "platform_integration.contracts.canonical", "stable PDM history projection"
    )
    payload = request_payload()
    first = payload["history"][0]
    payload["history"] = [
        {**first, "data_id": "z"},
        {**first, "data_id": "a"},
        *payload["history"][1:],
    ]

    request = models.PredictionRequestV2.model_validate(payload)

    assert [
        point["data_id"]
        for point in canonical.prediction_request_projection(request)["history"][:2]
    ] == ["a", "z"]


@pytest.mark.parametrize("field", ["tenant_id", "correlation_id", "equipment_id"])
@pytest.mark.parametrize(
    "invalid",
    [
        "00000000-0000-4000-8000-00000000010A",
        "0000000000004000800000000000010a",
        "{00000000-0000-4000-8000-00000000010a}",
        101,
        "not-a-uuid",
    ],
)
def test_request_rejects_noncanonical_uuid_wire_values(field: str, invalid: object) -> None:
    """Accepting alternate UUID lexemes would make cross-language replay identity ambiguous."""
    models = require_module("platform_integration.contracts.pdm", "strict PDM UUID contract")
    payload = request_payload()
    payload[field] = invalid

    with pytest.raises(ValidationError):
        models.PredictionRequestV2.model_validate(payload)


@pytest.mark.parametrize(
    ("path", "invalid"),
    [
        (("window_start",), "1785283740000"),
        (("window_end",), 1785287700000.0),
        (("history", 0, "timestamp"), True),
        (("history", 0, "value"), 4),
        (("history", 0, "value"), "4e0"),
        (("history", 0, "value"), "+4.00"),
    ],
)
def test_request_rejects_noncontract_integer_and_decimal_tokens(
    path: tuple[str | int, ...], invalid: object
) -> None:
    """Coercing numeric tokens or decimal lexemes would change the PDM wire contract."""
    models = require_module("platform_integration.contracts.pdm", "strict PDM numeric contract")
    payload = request_payload()
    target = payload
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = invalid

    with pytest.raises(ValidationError):
        models.PredictionRequestV2.model_validate(payload)


def test_request_accepts_real_json_uuid_strings_and_forbids_extras() -> None:
    """Rejecting canonical JSON UUIDs or accepting uncontracted fields breaks the provider boundary."""
    models = require_module("platform_integration.contracts.pdm", "PDM request contract")

    request = models.PredictionRequestV2.model_validate(request_payload())
    assert request.model_dump(mode="json")["equipment_id"] == EQUIPMENT_ID

    with pytest.raises(ValidationError):
        models.PredictionRequestV2.model_validate({**request_payload(), "tenant_name": "leak"})


def test_request_accepts_uuid_instances_for_internal_construction() -> None:
    """Rejecting an already-validated UUID would prevent safe internal model composition."""
    models = require_module("platform_integration.contracts.pdm", "internal PDM UUID construction")
    payload = request_payload()
    for field in ("tenant_id", "correlation_id", "equipment_id"):
        payload[field] = UUID(payload[field])

    request = models.PredictionRequestV2.model_validate(payload)

    assert request.model_dump(mode="json")["equipment_id"] == EQUIPMENT_ID


def test_response_accepts_wire_json_and_enforces_forecast_shape() -> None:
    """Rejecting provider JSON or accepting a non-15-point forecast breaks the v2 response contract."""
    models = require_module("platform_integration.contracts.pdm", "PDM response contract")

    response = models.PredictionResponseV2.model_validate(response_payload())
    assert response.model_dump(mode="json")["equipment_id"] == EQUIPMENT_ID
    assert response.generated_at.utcoffset() is not None

    for invalid in (
        {**response_payload(), "forecast": response_payload()["forecast"][:-1]},
        {**response_payload(), "generated_at": "2026-07-29 12:00:00"},
        {**response_payload(), "request_digest": "A" * 64},
        {**response_payload(), "tenant_id": TENANT_ID},
    ):
        with pytest.raises(ValidationError):
            models.PredictionResponseV2.model_validate(invalid)


@pytest.mark.parametrize("field", ["correlation_id", "equipment_id"])
@pytest.mark.parametrize(
    "invalid",
    [
        "00000000-0000-4000-8000-00000000010A",
        "0000000000004000800000000000010a",
        "{00000000-0000-4000-8000-00000000010a}",
        101,
        "not-a-uuid",
    ],
)
def test_response_rejects_noncanonical_uuid_wire_values(field: str, invalid: object) -> None:
    """Accepting alternate response UUID lexemes would weaken request-response identity checks."""
    models = require_module("platform_integration.contracts.pdm", "strict PDM response UUIDs")
    payload = response_payload()
    payload[field] = invalid

    with pytest.raises(ValidationError):
        models.PredictionResponseV2.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("timestamp", "1785287700000"),
        ("timestamp", 1785287700000.0),
        ("value", 4),
        ("value", "4e0"),
        ("value", "+4.00"),
    ],
)
def test_response_rejects_noncontract_forecast_tokens(field: str, invalid: object) -> None:
    """Coercing forecast values would admit a provider response outside the frozen schema."""
    models = require_module("platform_integration.contracts.pdm", "strict PDM forecast tokens")
    payload = response_payload()
    payload["forecast"][0][field] = invalid

    with pytest.raises(ValidationError):
        models.PredictionResponseV2.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    [
        "correlation_id",
        "equipment_id",
        "model_profile_id",
        "model_info_id",
        "meas_code",
        "request_digest",
    ],
)
def test_request_rejects_response_identity_drift(field: str) -> None:
    """Returning a forecast for a different request identity must never reach a caller."""
    models = require_module("platform_integration.contracts.pdm", "PDM response identity checks")
    request = models.PredictionRequestV2.model_validate(request_payload())
    payload = deepcopy(response_payload())
    payload[field] = (
        "00000000-0000-4000-8000-000000000199"
        if field in {"correlation_id", "equipment_id"}
        else ("f" * 64 if field == "request_digest" else f"different-{field}")
    )
    response = models.PredictionResponseV2.model_validate(payload)

    with pytest.raises(models.PdmContractError, match="PDM_RESPONSE_IDENTITY_MISMATCH"):
        request.assert_matching_response(response)
