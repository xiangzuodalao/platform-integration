from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import rfc8785

from platform_integration.clients.cmms import CmmsClient
from platform_integration.clients.thingsboard import ThingsBoardClient
from platform_integration.commands.shadow import ShadowCommandError, _install_stop_handlers
from platform_integration.config import Settings
from platform_integration.credentials import EnvironmentCredentialProvider
from platform_integration.db import create_async_sessionmaker
from platform_integration.services.closed_loop_delivery import (
    AlarmDeliveryWorker,
    ClosedLoopDeliveryStore,
    StatusPollWorker,
    WorkOrderDeliveryWorker,
)
from platform_integration.services.closed_loop_acceptance import (
    ClosedLoopAcceptanceStore,
    ClosedLoopAcceptanceVerifier,
)


def _base_runtime(role: str | None = None) -> tuple[Settings, object, ClosedLoopDeliveryStore]:
    settings = Settings()
    if (
        not settings.closed_loop_enabled
        or settings.database_url is None
        or settings.tenant_id is None
        or settings.pilot_work_order_equipment_id is None
    ):
        raise ShadowCommandError("CLOSED_LOOP_CONFIGURATION_INCOMPLETE")
    if role == "alarm" and (settings.tb_base_url is None or settings.tb_credential_ref is None):
        raise ShadowCommandError("ALARM_WORKER_CONFIGURATION_INCOMPLETE")
    if role in {"work-order", "status-poll"} and (
        settings.cmms_base_url is None or settings.cmms_credential_ref is None
    ):
        raise ShadowCommandError("CMMS_WORKER_CONFIGURATION_INCOMPLETE")
    if role not in {None, "alarm", "work-order", "status-poll"}:
        raise ShadowCommandError("CLOSED_LOOP_ROLE_INVALID")
    sessions = create_async_sessionmaker(settings.database_url.get_secret_value())
    return (
        settings,
        sessions,
        ClosedLoopDeliveryStore(
            sessions=sessions,
            tenant_id=settings.tenant_id,
            feedback_poll_seconds=settings.feedback_poll_seconds,
        ),
    )


async def _closed_loop_worker(
    *,
    role: str,
    owner: str,
    once: bool,
    now: datetime | None,
) -> None:
    settings, sessions, store = _base_runtime(role)
    credentials = EnvironmentCredentialProvider()
    http: httpx.AsyncClient | None = None
    if role == "alarm":
        assert settings.tb_base_url is not None and settings.tb_credential_ref is not None
        http = httpx.AsyncClient(base_url=str(settings.tb_base_url))
        worker = AlarmDeliveryWorker(
            store=store,
            thingsboard=ThingsBoardClient(
                http=http,
                credentials=credentials,
                tb_credential_ref=settings.tb_credential_ref,
            ),
            owner=owner,
        )
    elif role in {"work-order", "status-poll"}:
        assert settings.cmms_base_url is not None and settings.cmms_credential_ref is not None
        http = httpx.AsyncClient(base_url=str(settings.cmms_base_url))

        def cmms_factory(reference: str) -> CmmsClient:
            if reference != settings.cmms_credential_ref:
                raise ShadowCommandError("CMMS_CREDENTIAL_REF_DRIFT")
            assert http is not None
            return CmmsClient(
                http=http,
                credentials=credentials,
                cmms_credential_ref=reference,
            )

        if role == "work-order":
            worker = WorkOrderDeliveryWorker(
                store=store,
                cmms_factory=cmms_factory,
                owner=owner,
            )
        else:
            worker = StatusPollWorker(
                store=store,
                cmms_factory=cmms_factory,
                owner=owner,
            )
    stopped = asyncio.Event()
    _install_stop_handlers(stopped)
    try:
        if once:
            await worker.run_once(now or datetime.now(UTC))
            return
        while not stopped.is_set():
            await worker.run_once(datetime.now(UTC))
            try:
                await asyncio.wait_for(stopped.wait(), timeout=5)
            except TimeoutError:
                pass
    finally:
        if http is not None:
            await http.aclose()
        await sessions.kw["bind"].dispose()


def run_closed_loop_worker(
    *,
    role: str,
    owner: str,
    once: bool,
    now: datetime | None,
) -> None:
    asyncio.run(_closed_loop_worker(role=role, owner=owner, once=once, now=now))


async def _closed_loop_summary(tenant_alias: str) -> None:
    _, sessions, store = _base_runtime()
    try:
        summary = await store.summary(tenant_alias)
        print(rfc8785.dumps(summary).decode())
    finally:
        await sessions.kw["bind"].dispose()


def run_closed_loop_summary(tenant_alias: str) -> None:
    asyncio.run(_closed_loop_summary(tenant_alias))


async def _provider_readiness(role: str) -> None:
    settings = Settings()
    credentials = EnvironmentCredentialProvider()
    if not settings.closed_loop_enabled:
        raise ShadowCommandError("CLOSED_LOOP_DISABLED")
    if role == "alarm":
        if (
            settings.tb_base_url is None
            or settings.tb_credential_ref is None
            or settings.tb_tenant_id is None
        ):
            raise ShadowCommandError("ALARM_READINESS_CONFIGURATION_INCOMPLETE")
        async with httpx.AsyncClient(base_url=str(settings.tb_base_url)) as http:
            tenant_id = await ThingsBoardClient(
                http=http,
                credentials=credentials,
                tb_credential_ref=settings.tb_credential_ref,
            ).authenticated_tenant_id()
        if tenant_id != settings.tb_tenant_id:
            raise ShadowCommandError("THINGSBOARD_TENANT_ID_MISMATCH")
    elif role in {"work-order", "status-poll"}:
        if (
            settings.cmms_base_url is None
            or settings.cmms_credential_ref is None
            or settings.cmms_company_id is None
        ):
            raise ShadowCommandError("CMMS_READINESS_CONFIGURATION_INCOMPLETE")
        async with httpx.AsyncClient(base_url=str(settings.cmms_base_url)) as http:
            company_id = await CmmsClient(
                http=http,
                credentials=credentials,
                cmms_credential_ref=settings.cmms_credential_ref,
            ).authenticated_company_id()
        if company_id != settings.cmms_company_id:
            raise ShadowCommandError("CMMS_COMPANY_ID_MISMATCH")
    else:
        raise ShadowCommandError("CLOSED_LOOP_ROLE_INVALID")
    print(rfc8785.dumps({"role": role, "status": "ready"}).decode())


def run_provider_readiness(role: str) -> None:
    asyncio.run(_provider_readiness(role))


async def _closed_loop_acceptance_verify(
    tenant_alias: str,
    expected_stage: str,
) -> None:
    settings = Settings()
    if (
        not settings.closed_loop_enabled
        or not settings.isolated_pilot_mode
        or settings.database_url is None
        or settings.tenant_alias is None
        or settings.tenant_id is None
        or settings.pilot_work_order_equipment_id is None
        or settings.tb_base_url is None
        or settings.tb_tenant_id is None
        or settings.tb_credential_ref is None
        or settings.cmms_base_url is None
        or settings.cmms_company_id is None
        or settings.cmms_credential_ref is None
    ):
        raise ShadowCommandError("CLOSED_LOOP_ACCEPTANCE_CONFIGURATION_INCOMPLETE")
    if tenant_alias != settings.tenant_alias:
        raise ShadowCommandError("CLOSED_LOOP_ACCEPTANCE_TENANT_MISMATCH")

    sessions = create_async_sessionmaker(settings.database_url.get_secret_value())
    credentials = EnvironmentCredentialProvider()
    try:
        async with (
            httpx.AsyncClient(base_url=str(settings.tb_base_url)) as tb_http,
            httpx.AsyncClient(base_url=str(settings.cmms_base_url)) as cmms_http,
        ):
            evidence = await ClosedLoopAcceptanceVerifier(
                store=ClosedLoopAcceptanceStore(
                    sessions=sessions,
                    tenant_id=settings.tenant_id,
                ),
                thingsboard=ThingsBoardClient(
                    http=tb_http,
                    credentials=credentials,
                    tb_credential_ref=settings.tb_credential_ref,
                ),
                cmms=CmmsClient(
                    http=cmms_http,
                    credentials=credentials,
                    cmms_credential_ref=settings.cmms_credential_ref,
                ),
                tenant_id=settings.tenant_id,
                tb_tenant_id=settings.tb_tenant_id,
                cmms_company_id=settings.cmms_company_id,
                tb_credential_ref=settings.tb_credential_ref,
                cmms_credential_ref=settings.cmms_credential_ref,
                equipment_id=settings.pilot_work_order_equipment_id,
            ).verify(
                tenant_alias=tenant_alias,
                expected_stage=expected_stage,
            )
        print(rfc8785.dumps(evidence).decode())
    finally:
        await sessions.kw["bind"].dispose()


def run_closed_loop_acceptance_verify(tenant_alias: str, expected_stage: str) -> None:
    asyncio.run(_closed_loop_acceptance_verify(tenant_alias, expected_stage))
