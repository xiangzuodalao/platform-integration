from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from uuid import UUID, uuid5

import httpx
import rfc8785

from platform_integration.clients.cmms import CmmsClient
from platform_integration.clients.thingsboard import ThingsBoardClient
from platform_integration.config import Settings
from platform_integration.credentials import EnvironmentCredentialProvider
from platform_integration.db import create_async_sessionmaker
from platform_integration.repositories.provisioning import SqlProvisioningStore
from platform_integration.services.provisioning import ProvisioningPlan, ProvisioningService
from platform_integration.services.tenant_bindings import (
    PILOT_ALIAS,
    TenantBindingConfig,
    TenantBindingsService,
)


class ProvisionCommandError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(slots=True)
class _Runtime:
    service: ProvisioningService
    tb_http: httpx.AsyncClient
    cmms_http: httpx.AsyncClient

    async def close(self) -> None:
        await self.tb_http.aclose()
        await self.cmms_http.aclose()


def _required_settings(settings: Settings, tenant_alias: str) -> None:
    if tenant_alias != PILOT_ALIAS or settings.tenant_alias != PILOT_ALIAS:
        raise ProvisionCommandError("PILOT_ALIAS_TENANT_MISMATCH")
    if settings.isolated_pilot_mode is not True:
        raise ProvisionCommandError("ISOLATED_PILOT_MODE_REQUIRED")
    required = (
        settings.database_url,
        settings.tenant_id,
        settings.tb_base_url,
        settings.cmms_base_url,
        settings.tb_tenant_id,
        settings.cmms_company_id,
        settings.pdm_credential_ref,
        settings.tb_credential_ref,
        settings.cmms_credential_ref,
    )
    if any(value is None for value in required):
        raise ProvisionCommandError("PROVISIONING_CONFIGURATION_INCOMPLETE")


def _runtime(settings: Settings, tenant_alias: str) -> _Runtime:
    _required_settings(settings, tenant_alias)
    assert settings.database_url is not None
    assert settings.tenant_id is not None
    assert settings.tb_base_url is not None
    assert settings.cmms_base_url is not None
    assert settings.tb_tenant_id is not None
    assert settings.cmms_company_id is not None
    assert settings.pdm_credential_ref is not None
    assert settings.tb_credential_ref is not None
    assert settings.cmms_credential_ref is not None
    credentials = EnvironmentCredentialProvider()
    tb_http = httpx.AsyncClient(base_url=str(settings.tb_base_url))
    cmms_http = httpx.AsyncClient(base_url=str(settings.cmms_base_url))
    tb = ThingsBoardClient(
        http=tb_http,
        credentials=credentials,
        tb_credential_ref=settings.tb_credential_ref,
    )
    cmms = CmmsClient(
        http=cmms_http,
        credentials=credentials,
        cmms_credential_ref=settings.cmms_credential_ref,
    )
    binding_config = TenantBindingConfig(
        tenant_id=settings.tenant_id,
        alias=settings.tenant_alias,
        tb_tenant_id=settings.tb_tenant_id,
        cmms_company_id=settings.cmms_company_id,
        pdm_credential_ref=settings.pdm_credential_ref,
        tb_credential_ref=settings.tb_credential_ref,
        cmms_credential_ref=settings.cmms_credential_ref,
        cmms_webhook_secret_ref=settings.cmms_webhook_secret_ref,
    )
    bindings = TenantBindingsService(binding_config, tb=tb, cmms=cmms)
    store = SqlProvisioningStore(
        create_async_sessionmaker(settings.database_url.get_secret_value())
    )
    return _Runtime(
        service=ProvisioningService(
            tenant_bindings=bindings,
            store=store,
            thingsboard=tb,
            cmms=cmms,
        ),
        tb_http=tb_http,
        cmms_http=cmms_http,
    )


def _actor_id(tenant_id: UUID, actor: str) -> UUID:
    if not actor or len(actor) > 160:
        raise ProvisionCommandError("ACTOR_INVALID")
    return uuid5(tenant_id, f"pilot-operator:{actor}")


def _receipt(plan: ProvisioningPlan) -> dict[str, object]:
    return {
        "tenant_id": str(plan.tenant_id),
        "plan_hash": plan.plan_hash,
        "applied_at": None if plan.applied_at is None else plan.applied_at.isoformat(),
        "terminal_result": plan.terminal_result,
        "target_results": plan.target_results,
    }


async def _plan(tenant_alias: str, actor: str) -> None:
    settings = Settings()
    runtime = _runtime(settings, tenant_alias)
    try:
        assert settings.tenant_id is not None
        plan = await runtime.service.build_plan(
            settings.tenant_id, _actor_id(settings.tenant_id, actor)
        )
        print(rfc8785.dumps(plan.canonical).decode())
        print(json.dumps({"plan_hash": plan.plan_hash}, sort_keys=True, separators=(",", ":")))
    finally:
        await runtime.close()


async def _apply(tenant_alias: str, plan_hash: str, confirmed_hash: str, actor: str) -> None:
    settings = Settings()
    runtime = _runtime(settings, tenant_alias)
    try:
        assert settings.tenant_id is not None
        result = await runtime.service.apply(
            plan_hash,
            confirmed_hash,
            _actor_id(settings.tenant_id, actor),
        )
        print(json.dumps(_receipt(result), sort_keys=True, separators=(",", ":")))
    finally:
        await runtime.close()


async def _verify(tenant_alias: str, plan_hash: str) -> None:
    settings = Settings()
    runtime = _runtime(settings, tenant_alias)
    try:
        assert settings.tenant_id is not None
        result = await runtime.service.verify(settings.tenant_id, plan_hash)
        print(json.dumps(_receipt(result), sort_keys=True, separators=(",", ":")))
    finally:
        await runtime.close()


def run_provision_plan(tenant_alias: str, actor: str) -> None:
    asyncio.run(_plan(tenant_alias, actor))


def run_provision_apply(tenant_alias: str, plan_hash: str, confirmed_hash: str, actor: str) -> None:
    asyncio.run(_apply(tenant_alias, plan_hash, confirmed_hash, actor))


def run_provision_verify(tenant_alias: str, plan_hash: str) -> None:
    asyncio.run(_verify(tenant_alias, plan_hash))
