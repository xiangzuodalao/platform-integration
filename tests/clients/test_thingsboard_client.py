from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from platform_integration.clients.thingsboard import (
    ThingsBoardClient,
    ThingsBoardClientError,
)


TENANT_ID = "00000000-0000-4000-8000-000000000001"
DEVICE_ID = "00000000-0000-4000-8000-000000000101"


def provider(kind: str = "thingsboard_bearer", values=("tb-secret",)):
    tokens = iter(values)
    return SimpleNamespace(get=lambda _: SimpleNamespace(kind=kind, value=SecretStr(next(tokens))))


@pytest.mark.asyncio
async def test_thingsboard_uses_exact_endpoints_and_rotated_header_per_call() -> None:
    """Caching a token or drifting an endpoint would break rotation and readback recovery."""
    seen: list[tuple[str, str, str, str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append(
            (
                request.method,
                request.url.path,
                request.url.query.decode(),
                request.headers["X-Authorization"],
                body,
            )
        )
        if request.url.path == "/api/auth/user":
            return httpx.Response(
                200,
                json={
                    "tenantId": {"id": TENANT_ID, "entityType": "TENANT"},
                    "email": "pilot@example.invalid",
                },
            )
        if request.url.path == "/api/tenant/devices":
            return httpx.Response(
                200,
                json={
                    "data": [{"id": {"id": DEVICE_ID}, "name": "LINE-A-CNC-01", "type": "CNC"}],
                    "totalElements": 1,
                    "hasNext": False,
                },
            )
        if request.method == "POST":
            return httpx.Response(204)
        return httpx.Response(
            200,
            json=[
                {"key": "equipment_id", "value": "00000000-0000-4000-8000-000000000201"},
                {"key": "cmms_asset_id", "value": 42},
            ],
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://tb.invalid",
    ) as http:
        client = ThingsBoardClient(
            http=http,
            credentials=provider(values=("one", "two", "three", "four")),
            tb_credential_ref="TB_TEST_CREDENTIAL",
        )
        assert str(await client.authenticated_tenant_id()) == TENANT_ID
        assert (await client.list_devices())[0].name == "LINE-A-CNC-01"
        await client.write_asset_attributes(
            DEVICE_ID,
            equipment_id="00000000-0000-4000-8000-000000000201",
            cmms_asset_id=42,
        )
        attributes = await client.read_asset_attributes(DEVICE_ID)

    assert attributes.cmms_asset_id == 42
    assert seen == [
        ("GET", "/api/auth/user", "", "Bearer one", None),
        (
            "GET",
            "/api/tenant/devices",
            "pageSize=100&page=0",
            "Bearer two",
            None,
        ),
        (
            "POST",
            f"/api/plugins/telemetry/DEVICE/{DEVICE_ID}/attributes/SERVER_SCOPE",
            "",
            "Bearer three",
            {
                "equipment_id": "00000000-0000-4000-8000-000000000201",
                "cmms_asset_id": 42,
            },
        ),
        (
            "GET",
            f"/api/plugins/telemetry/DEVICE/{DEVICE_ID}/values/attributes/SERVER_SCOPE",
            "keys=equipment_id%2Ccmms_asset_id",
            "Bearer four",
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_alarm_baseline_uses_exact_filter_and_strict_count() -> None:
    """A broader or unscoped alarm query could falsely certify isolation."""
    captured: httpx.Request | None = None

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured
        captured = request
        return httpx.Response(200, json={"totalElements": 0, "data": []})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://tb.invalid",
    ) as http:
        client = ThingsBoardClient(
            http=http,
            credentials=provider(),
            tb_credential_ref="TB_TEST_CREDENTIAL",
        )
        assert await client.active_pdm_alarm_count(DEVICE_ID) == 0

    assert captured is not None
    assert captured.url.path == f"/api/v2/alarm/DEVICE/{DEVICE_ID}"
    assert dict(captured.url.params) == {
        "pageSize": "1",
        "page": "0",
        "statusList": "ACTIVE",
        "typeList": "PDM_FORECAST_RISK",
    }


@pytest.mark.asyncio
async def test_attribute_timeout_is_unknown_and_wrong_kind_fails_before_io() -> None:
    """Blindly hiding an unknown write or accepting another credential kind risks duplication."""
    calls = 0

    async def timeout_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("secret detail", request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(timeout_handler),
        base_url="https://tb.invalid",
    ) as http:
        client = ThingsBoardClient(
            http=http,
            credentials=provider(),
            tb_credential_ref="TB_TEST_CREDENTIAL",
        )
        with pytest.raises(ThingsBoardClientError) as exc_info:
            await client.write_asset_attributes(
                DEVICE_ID,
                equipment_id="00000000-0000-4000-8000-000000000201",
                cmms_asset_id=42,
            )
    assert exc_info.value.code == "THINGSBOARD_WRITE_RESULT_UNKNOWN"
    assert "secret" not in str(exc_info.value)
    assert calls == 1

    calls = 0
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(timeout_handler),
        base_url="https://tb.invalid",
    ) as http:
        client = ThingsBoardClient(
            http=http,
            credentials=provider("opaque_bearer"),
            tb_credential_ref="TB_TEST_CREDENTIAL",
        )
        with pytest.raises(ThingsBoardClientError) as wrong_kind:
            await client.authenticated_tenant_id()
    assert wrong_kind.value.code == "THINGSBOARD_CREDENTIAL_KIND_INVALID"
    assert calls == 0
