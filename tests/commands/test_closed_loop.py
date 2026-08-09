from uuid import UUID

import pytest

from platform_integration.commands import closed_loop
from platform_integration.commands.shadow import ShadowCommandError


class FakeHttp:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None


@pytest.mark.asyncio
async def test_alarm_readiness_is_read_only_and_checks_external_tenant(monkeypatch, capsys):
    """A syntactically valid but cross-tenant token must never make a worker healthy."""
    expected = UUID("30000000-0000-4000-8000-000000000001")
    monkeypatch.setenv("PLATFORM_INTEGRATION_CLOSED_LOOP_ENABLED", "1")
    monkeypatch.setenv("PLATFORM_INTEGRATION_TB_BASE_URL", "https://tb.invalid")
    monkeypatch.setenv("PLATFORM_INTEGRATION_TB_CREDENTIAL_REF", "TB_PILOT_CREDENTIAL")
    monkeypatch.setenv("PLATFORM_INTEGRATION_TB_TENANT_ID", str(expected))
    monkeypatch.setattr(closed_loop.httpx, "AsyncClient", lambda **_: FakeHttp())

    class FakeClient:
        def __init__(self, **_):
            pass

        async def authenticated_tenant_id(self):
            return expected

    monkeypatch.setattr(closed_loop, "ThingsBoardClient", FakeClient)

    await closed_loop._provider_readiness("alarm")

    assert capsys.readouterr().out == '{"role":"alarm","status":"ready"}\n'


@pytest.mark.asyncio
async def test_cmms_readiness_rejects_company_drift_without_secret_output(monkeypatch, capsys):
    """An administrator Bearer for another company must fail before any work-order read/write."""
    secret = "cmms-readiness-secret-canary"
    monkeypatch.setenv("PLATFORM_INTEGRATION_CLOSED_LOOP_ENABLED", "1")
    monkeypatch.setenv("PLATFORM_INTEGRATION_CMMS_BASE_URL", "https://cmms.invalid")
    monkeypatch.setenv("PLATFORM_INTEGRATION_CMMS_CREDENTIAL_REF", "CMMS_PILOT_CREDENTIAL")
    monkeypatch.setenv("PLATFORM_INTEGRATION_CMMS_COMPANY_ID", "7001")
    monkeypatch.setenv("CMMS_PILOT_CREDENTIAL", f'{{"kind":"cmms_bearer","value":"{secret}"}}')
    monkeypatch.setattr(closed_loop.httpx, "AsyncClient", lambda **_: FakeHttp())

    class FakeClient:
        def __init__(self, **_):
            pass

        async def authenticated_company_id(self):
            return 7002

    monkeypatch.setattr(closed_loop, "CmmsClient", FakeClient)

    with pytest.raises(ShadowCommandError) as exc_info:
        await closed_loop._provider_readiness("work-order")

    assert exc_info.value.code == "CMMS_COMPANY_ID_MISMATCH"
    assert secret not in capsys.readouterr().out
