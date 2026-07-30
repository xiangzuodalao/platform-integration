from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr


EQUIPMENT_ID = "00000000-0000-4000-8000-000000000101"
IDEMPOTENCY_KEY = "pilot-asset:00000000-0000-4000-8000-000000000201"
CMMS_CREDENTIAL_REF = "CMMS_TEST_CREDENTIAL"


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


def asset_create():
    contracts = require_module("platform_integration.contracts.cmms", "CMMS asset contract")
    return contracts.CmmsAssetCreate(
        name="Pilot CNC",
        equipment_id=EQUIPMENT_ID.upper(),
    )


def cmms_client(clients, http: httpx.AsyncClient, provider=None):
    provider = provider or SimpleNamespace(
        get=lambda _: SimpleNamespace(kind="cmms_api_key", value=SecretStr("cmms-secret"))
    )
    return clients.CmmsClient(
        http=http,
        credentials=provider,
        cmms_credential_ref=CMMS_CREDENTIAL_REF,
    )


@pytest.mark.asyncio
async def test_caller_can_get_before_post_with_exact_idempotent_request() -> None:
    """Changing reconciliation order, key, or body would break replay-safe asset provisioning."""
    clients = require_module("platform_integration.clients.cmms", "CMMS GET-before-POST client")
    requests: list[tuple[str, str, dict | None, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.path, body, request.extensions["timeout"]))
        if request.method == "GET":
            return httpx.Response(404, json={"code": "ASSET_NOT_FOUND", "message": "missing"})
        assert request.headers["Idempotency-Key"] == IDEMPOTENCY_KEY
        return httpx.Response(
            201,
            json={
                "id": 42,
                "name": "Pilot CNC",
                "equipment_id": EQUIPMENT_ID,
                "provider_extra": "ignored",
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        existing = await client.find_asset_by_equipment_id(EQUIPMENT_ID.upper())
        result = existing or await client.create_asset(
            asset_create(),
            idempotency_key=IDEMPOTENCY_KEY,
        )

    assert result.model_dump(mode="json") == {
        "id": 42,
        "name": "Pilot CNC",
        "equipment_id": EQUIPMENT_ID,
    }
    assert requests == [
        (
            "GET",
            f"/api/assets/by-equipment-id/{EQUIPMENT_ID}",
            None,
            {"connect": 10.0, "read": 10.0, "write": 10.0, "pool": 10.0},
        ),
        (
            "POST",
            "/api/assets",
            {"name": "Pilot CNC", "equipment_id": EQUIPMENT_ID},
            {"connect": 10.0, "read": 10.0, "write": 10.0, "pool": 10.0},
        ),
    ]


@pytest.mark.asyncio
async def test_find_returns_projected_asset_or_none() -> None:
    """Treating 404 as an error or rejecting provider extras would break reconciliation."""
    clients = require_module("platform_integration.clients.cmms", "CMMS asset lookup")
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(404, json={"code": "ASSET_NOT_FOUND", "message": "missing"})
        return httpx.Response(
            200,
            json={
                "id": 42,
                "name": "Pilot CNC",
                "equipment_id": EQUIPMENT_ID.upper(),
                "company": {"id": 7},
                "custom_fields": {"line": "A"},
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        assert await client.find_asset_by_equipment_id(EQUIPMENT_ID) is None
        asset = await client.find_asset_by_equipment_id(EQUIPMENT_ID)

    assert asset is not None
    assert asset.model_dump(mode="json") == {
        "id": 42,
        "name": "Pilot CNC",
        "equipment_id": EQUIPMENT_ID,
    }


@pytest.mark.asyncio
async def test_create_same_body_replay_preserves_exact_header_and_payload() -> None:
    """Mutating a replay body or idempotency key would turn a safe replay into a conflict."""
    clients = require_module("platform_integration.clients.cmms", "CMMS idempotent asset replay")
    seen: list[tuple[str, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.headers["Idempotency-Key"], json.loads(request.content)))
        return httpx.Response(
            201,
            json={"id": 42, "name": "Pilot CNC", "equipment_id": EQUIPMENT_ID},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        first = await client.create_asset(asset_create(), idempotency_key=IDEMPOTENCY_KEY)
        replay = await client.create_asset(asset_create(), idempotency_key=IDEMPOTENCY_KEY)

    expected = (
        IDEMPOTENCY_KEY,
        {"name": "Pilot CNC", "equipment_id": EQUIPMENT_ID},
    )
    assert seen == [expected, expected]
    assert first == replay


@pytest.mark.asyncio
async def test_create_maps_idempotency_conflict() -> None:
    """Returning a generic HTTP error for body drift would hide the stable CMMS conflict code."""
    clients = require_module("platform_integration.clients.cmms", "CMMS conflict mapping")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                409,
                json={
                    "code": "IDEMPOTENCY_CONFLICT",
                    "message": "Idempotency key conflicts with this request.",
                },
            )
        ),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        with pytest.raises(clients.CmmsClientError) as exc_info:
            await client.create_asset(asset_create(), idempotency_key=IDEMPOTENCY_KEY)

    assert exc_info.value.code == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_post_timeout_is_unknown_without_retry_or_hidden_get() -> None:
    """Retrying or reconciling inside create_asset could duplicate a write after an unknown result."""
    clients = require_module(
        "platform_integration.clients.cmms", "CMMS unknown-write-result handling"
    )
    methods: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "POST":
            raise httpx.ReadTimeout("unknown write result", request=request)
        return httpx.Response(
            200,
            json={"id": 42, "name": "Pilot CNC", "equipment_id": EQUIPMENT_ID},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        with pytest.raises(clients.CmmsClientError) as exc_info:
            await client.create_asset(asset_create(), idempotency_key=IDEMPOTENCY_KEY)
        reconciled = await client.find_asset_by_equipment_id(EQUIPMENT_ID)

    assert exc_info.value.code == "CMMS_WRITE_RESULT_UNKNOWN"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert exc_info.value.__suppress_context__ is True
    assert reconciled is not None and reconciled.id == 42
    assert methods == ["POST", "GET"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "pilot-asset:00000000-0000-4000-8000-00000000020A",
        "pilot-asset:00000000000040008000000000000201",
        "pilot-asset:{00000000-0000-4000-8000-000000000201}",
        "asset:00000000-0000-4000-8000-000000000201",
    ],
)
async def test_create_rejects_noncanonical_pilot_idempotency_keys(invalid: str) -> None:
    """Forwarding a noncanonical key would violate the CMMS replay namespace."""
    clients = require_module("platform_integration.clients.cmms", "CMMS idempotency key validation")
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(201, json={})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        with pytest.raises(clients.CmmsClientError, match="IDEMPOTENCY_KEY_INVALID"):
            await client.create_asset(asset_create(), idempotency_key=invalid)

    assert calls == 0


@pytest.mark.asyncio
async def test_cmms_uses_rotated_api_key_per_call_and_strict_identity_endpoints() -> None:
    """Caching credentials or using the wrong auth/search contract would break safe rotation."""
    clients = require_module("platform_integration.clients.cmms", "credential-bound CMMS client")
    secrets = iter(("first-secret", "second-secret"))
    provider = SimpleNamespace(
        get=lambda _: SimpleNamespace(kind="cmms_api_key", value=SecretStr(next(secrets)))
    )
    seen: list[tuple[str, str, str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, request.headers["x-api-key"], body))
        if request.url.path == "/api/auth/me":
            return httpx.Response(
                200,
                json={"companyId": 201, "username": "pilot-operator"},
            )
        return httpx.Response(200, json={"totalElements": 0})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http, provider)
        assert await client.authenticated_company_id() == 201
        assert await client.total_work_orders() == 0

    assert seen == [
        ("GET", "/api/auth/me", "first-secret", None),
        (
            "POST",
            "/api/work-orders/search",
            "second-secret",
            {
                "filterFields": [],
                "direction": "ASC",
                "pageNum": 0,
                "pageSize": 1,
                "sortField": "id",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_cmms_wrong_credential_kind_fails_before_io_without_secret_disclosure() -> None:
    """Accepting a bearer token as a CMMS key would cross credential trust boundaries."""
    clients = require_module("platform_integration.clients.cmms", "strict CMMS credentials")
    calls = 0
    secret = "must-never-leak"

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    provider = SimpleNamespace(
        get=lambda _: SimpleNamespace(kind="opaque_bearer", value=SecretStr(secret))
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http, provider)
        with pytest.raises(clients.CmmsClientError) as exc_info:
            await client.authenticated_company_id()

    assert exc_info.value.code == "CMMS_CREDENTIAL_KIND_INVALID"
    assert calls == 0
    assert secret not in str(exc_info.value)


@pytest.mark.asyncio
async def test_cmms_missing_company_identity_maps_to_stable_safe_error() -> None:
    """Leaking a provider-shape KeyError would break the strict identity boundary."""
    clients = require_module("platform_integration.clients.cmms", "CMMS identity validation")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"username": "pilot-operator"})
        ),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        with pytest.raises(clients.CmmsClientError) as exc_info:
            await client.authenticated_company_id()

    assert exc_info.value.code == "CMMS_INVALID_IDENTITY_RESPONSE"
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
async def test_cmms_rejects_company_identity_above_signed_bigint() -> None:
    """Passing an oversized identity onward would fail only after local persistence began."""
    clients = require_module("platform_integration.clients.cmms", "CMMS BIGINT identity guard")
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"companyId": 9_223_372_036_854_775_808})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://cmms.invalid",
    ) as http:
        client = cmms_client(clients, http)
        with pytest.raises(clients.CmmsClientError) as exc_info:
            await client.authenticated_company_id()

    assert exc_info.value.code == "CMMS_INVALID_IDENTITY_RESPONSE"
    assert calls == 1
