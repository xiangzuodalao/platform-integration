from __future__ import annotations

import importlib
import json
import logging
import traceback
from uuid import UUID

import httpx
import pytest


REQUEST_DIGEST = "067096fc115546164d8608cddadc56b185fefea9dffab4c7462c8d28011cdfbd"
INPUT_DIGEST = "c67fe58c14f2e8a1ef252e6ff47b4877c0da8fa52664772dd3c08ae3d6552609"
TENANT_ID = "00000000-0000-4000-8000-000000000001"
CORRELATION_ID = "00000000-0000-4000-8000-000000000102"
EQUIPMENT_ID = "00000000-0000-4000-8000-000000000101"
CREDENTIAL_REF = "PDM_PILOT_CREDENTIAL"


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


def prediction_request():
    contracts = require_module("platform_integration.contracts.pdm", "PDM request contract")
    return contracts.PredictionRequestV2.model_validate(request_payload())


@pytest.mark.asyncio
async def test_predict_sends_exact_json_bearer_and_ten_second_timeout(monkeypatch) -> None:
    """Changing the wire body, credential lookup, or timeout would violate the PDM operation."""
    clients = require_module("platform_integration.clients.pdm", "authenticated PDM prediction")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    seen: list[tuple[dict, str, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                json.loads(request.content),
                request.headers["Authorization"],
                request.extensions["timeout"],
            )
        )
        return httpx.Response(200, json=response_payload())

    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"first-sensitive-test-token"}',
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://pdm.invalid",
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        result = await client.predict(prediction_request())
        monkeypatch.setenv(
            CREDENTIAL_REF,
            '{"kind":"opaque_bearer","value":"second-sensitive-test-token"}',
        )
        await client.predict(prediction_request())

    assert result.request_digest == REQUEST_DIGEST
    assert seen[0][0] == request_payload()
    assert seen[0][1] == "Bearer first-sensitive-test-token"
    assert seen[1][1] == "Bearer second-sensitive-test-token"
    assert seen[0][2] == {
        "connect": 10.0,
        "read": 10.0,
        "write": 10.0,
        "pool": 10.0,
    }


@pytest.mark.asyncio
async def test_predict_timeout_is_safe_and_never_retried(monkeypatch, caplog, capfd) -> None:
    """Retrying a timeout or exposing its authorized request could duplicate work or leak a token."""
    clients = require_module("platform_integration.clients.pdm", "safe PDM timeout mapping")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    calls = 0
    secret = "never-print-this-sensitive-test-token"
    caplog.set_level(logging.DEBUG)

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("provider timed out", request=request)

    monkeypatch.setenv(
        CREDENTIAL_REF,
        json.dumps({"kind": "opaque_bearer", "value": secret}),
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://pdm.invalid",
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        with pytest.raises(clients.PdmClientError) as exc_info:
            await client.predict(prediction_request())

    assert calls == 1
    assert exc_info.value.code == "PDM_UNAVAILABLE"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert exc_info.value.__suppress_context__ is True
    captured = capfd.readouterr()
    exposed = "\n".join(
        (
            str(exc_info.value),
            repr(exc_info.value),
            "".join(traceback.format_exception(exc_info.value)),
            caplog.text,
            "\n".join(record.getMessage() for record in caplog.records),
            captured.out,
            captured.err,
        )
    )
    assert secret not in exposed


@pytest.mark.asyncio
async def test_predict_timeout_scrubs_authorization_from_transport_request(monkeypatch) -> None:
    """Retaining authorization on a transport-held timeout request would retain the bearer token."""
    clients = require_module("platform_integration.clients.pdm", "PDM timeout request scrubbing")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    saved_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        saved_requests.append(request)
        raise httpx.ReadTimeout("provider timed out", request=request)

    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"timeout-request-sensitive-canary"}',
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://pdm.invalid",
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        with pytest.raises(clients.PdmClientError, match="PDM_UNAVAILABLE"):
            await client.predict(prediction_request())

    assert len(saved_requests) == 1
    assert "Authorization" not in saved_requests[0].headers


@pytest.mark.asyncio
async def test_predict_scrubs_authorization_from_redirect_response_history(monkeypatch) -> None:
    """Retaining authorization on same-origin response history would retain the bearer token."""
    clients = require_module("platform_integration.clients.pdm", "PDM redirect request scrubbing")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    returned_responses: list[httpx.Response] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/predictions":
            response = httpx.Response(307, headers={"Location": "/internal/predictions"})
        else:
            response = httpx.Response(200, json=response_payload())
        returned_responses.append(response)
        return response

    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"redirect-sensitive-canary"}',
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://pdm.invalid",
        follow_redirects=True,
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        await client.predict(prediction_request())

    final_response = returned_responses[-1]
    assert len(final_response.history) == 1
    assert "Authorization" not in final_response.request.headers
    assert "Authorization" not in final_response.history[0].request.headers


@pytest.mark.asyncio
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
async def test_predict_rejects_response_identity_drift(monkeypatch, field: str) -> None:
    """A provider response for another request identity must not be returned to orchestration."""
    clients = require_module("platform_integration.clients.pdm", "PDM response identity guard")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    payload = response_payload()
    payload[field] = (
        "00000000-0000-4000-8000-000000000199"
        if field in {"correlation_id", "equipment_id"}
        else ("f" * 64 if field == "request_digest" else f"different-{field}")
    )

    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"identity-test-token"}',
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload)),
        base_url="https://pdm.invalid",
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        with pytest.raises(clients.PdmClientError, match="PDM_RESPONSE_IDENTITY_MISMATCH"):
            await client.predict(prediction_request())


@pytest.mark.asyncio
async def test_predict_checks_response_against_the_sent_request_snapshot(monkeypatch) -> None:
    """Reading the caller's mutable request after await would admit a TOCTOU identity change."""
    clients = require_module("platform_integration.clients.pdm", "PDM immutable wire snapshot")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    request = prediction_request()
    changed_equipment_id = "00000000-0000-4000-8000-000000000199"
    sent_equipment_ids: list[str] = []

    async def handler(http_request: httpx.Request) -> httpx.Response:
        sent_equipment_ids.append(json.loads(http_request.content)["equipment_id"])
        request.equipment_id = UUID(changed_equipment_id)
        payload = response_payload()
        payload["equipment_id"] = changed_equipment_id
        return httpx.Response(200, json=payload)

    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"snapshot-test-token"}',
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://pdm.invalid",
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        with pytest.raises(clients.PdmClientError, match="PDM_RESPONSE_IDENTITY_MISMATCH"):
            await client.predict(request)

    assert sent_equipment_ids == [EQUIPMENT_ID]


@pytest.mark.asyncio
async def test_predict_revalidates_a_mutated_request_before_network(monkeypatch) -> None:
    """Sending a model mutated after construction would bypass strict wire validation."""
    clients = require_module("platform_integration.clients.pdm", "PDM wire snapshot validation")
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    request = prediction_request()
    request.equipment_id = "not-a-uuid"
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=response_payload())

    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"snapshot-validation-test-token"}',
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://pdm.invalid",
    ) as http:
        client = clients.PdmClient(
            http=http,
            credentials=credentials.EnvironmentCredentialProvider(),
            pdm_credential_ref=CREDENTIAL_REF,
        )
        with pytest.raises(clients.PdmClientError, match="PDM_INVALID_REQUEST"):
            await client.predict(request)

    assert calls == 0


@pytest.mark.parametrize(
    "envelope",
    [
        '{"kind":"opaque_bearer","value":""}',
        '{"kind":"opaque_bearer","value":"secret","extra":true}',
        '{"kind":"jwt","value":"secret"}',
        '{"kind":"opaque_bearer"}',
        "not-json",
    ],
)
def test_environment_credential_provider_rejects_invalid_envelopes(
    monkeypatch, envelope: str
) -> None:
    """Accepting malformed or expanded credential envelopes would weaken the external boundary."""
    credentials = require_module(
        "platform_integration.credentials", "strict opaque bearer envelope"
    )
    monkeypatch.setenv(CREDENTIAL_REF, envelope)

    with pytest.raises(credentials.CredentialResolutionError):
        credentials.EnvironmentCredentialProvider().get(CREDENTIAL_REF)


def test_environment_credential_error_does_not_retain_the_envelope(monkeypatch) -> None:
    """Keeping a parse failure as exception context could expose the rejected secret envelope."""
    credentials = require_module(
        "platform_integration.credentials", "safe credential envelope rejection"
    )
    secret = "invalid-envelope-sensitive-canary"
    monkeypatch.setenv(
        CREDENTIAL_REF,
        json.dumps({"kind": "opaque_bearer", "value": secret, "extra": True}),
    )

    with pytest.raises(credentials.CredentialResolutionError) as exc_info:
        credentials.EnvironmentCredentialProvider().get(CREDENTIAL_REF)

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert exc_info.value.__suppress_context__ is True
    assert secret not in "".join(traceback.format_exception(exc_info.value))


@pytest.mark.parametrize("length", [3, 128])
def test_environment_credential_reference_accepts_contract_boundaries(
    monkeypatch, length: int
) -> None:
    """Rejecting the minimum or maximum valid environment name would narrow the frozen contract."""
    credentials = require_module(
        "platform_integration.credentials", "credential reference length boundaries"
    )
    reference = "A" * length
    monkeypatch.setenv(
        reference,
        '{"kind":"opaque_bearer","value":"boundary-test-token"}',
    )

    assert (
        credentials.EnvironmentCredentialProvider().get(reference).value.get_secret_value()
        == "boundary-test-token"
    )


def test_pdm_client_requires_a_configured_reference() -> None:
    """Constructing a PDM client without an environment reference must fail before use."""
    clients = require_module(
        "platform_integration.clients.pdm", "required PDM credential reference"
    )
    credentials = require_module(
        "platform_integration.credentials", "environment credential resolution"
    )
    http = httpx.AsyncClient(base_url="https://pdm.invalid", trust_env=False)
    try:
        with pytest.raises(clients.PdmClientError, match="PDM_CREDENTIAL_REF_REQUIRED"):
            clients.PdmClient(
                http=http,
                credentials=credentials.EnvironmentCredentialProvider(),
                pdm_credential_ref=None,
            )
    finally:
        import asyncio

        asyncio.run(http.aclose())


def test_environment_credential_lookup_is_direct_and_noninterpolating(monkeypatch) -> None:
    """Resolving a value embedded in a reference would turn the reference into a secret channel."""
    credentials = require_module(
        "platform_integration.credentials", "direct environment credential lookup"
    )
    monkeypatch.setenv(
        CREDENTIAL_REF,
        '{"kind":"opaque_bearer","value":"direct-test-token"}',
    )
    monkeypatch.setenv("INDIRECT_REF", CREDENTIAL_REF)

    direct = credentials.EnvironmentCredentialProvider().get(CREDENTIAL_REF)
    assert direct.value.get_secret_value() == "direct-test-token"
    with pytest.raises(credentials.CredentialResolutionError):
        credentials.EnvironmentCredentialProvider().get("INDIRECT_REF")
    with pytest.raises(credentials.CredentialResolutionError):
        credentials.EnvironmentCredentialProvider().get("${PDM_PILOT_CREDENTIAL}")
