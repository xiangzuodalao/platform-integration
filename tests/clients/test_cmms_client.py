from __future__ import annotations

import importlib
import json

import httpx
import pytest


EQUIPMENT_ID = "00000000-0000-4000-8000-000000000101"
IDEMPOTENCY_KEY = "pilot-asset:00000000-0000-4000-8000-000000000201"


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
        client = clients.CmmsClient(http=http)
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
        client = clients.CmmsClient(http=http)
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
        client = clients.CmmsClient(http=http)
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
        client = clients.CmmsClient(http=http)
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
        client = clients.CmmsClient(http=http)
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
        client = clients.CmmsClient(http=http)
        with pytest.raises(clients.CmmsClientError, match="IDEMPOTENCY_KEY_INVALID"):
            await client.create_asset(asset_create(), idempotency_key=invalid)

    assert calls == 0
