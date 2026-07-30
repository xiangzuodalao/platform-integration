from __future__ import annotations

import asyncio
import re
import signal
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import rfc8785
from alembic import command
from alembic.config import Config

from platform_integration.clients.cmms import CmmsClient
from platform_integration.clients.pdm import PdmClient
from platform_integration.clients.thingsboard import ThingsBoardClient
from platform_integration.config import Settings
from platform_integration.credentials import EnvironmentCredentialProvider
from platform_integration.db import create_async_sessionmaker
from platform_integration.services.prediction_requests import PredictionRequestBuilder
from platform_integration.services.prediction_runs import ShadowExecutionStore
from platform_integration.services.risk import RiskEvaluator
from platform_integration.services.shadow_summary import ShadowSummaryService
from platform_integration.workers.prediction import PredictionProcessor, PredictionWorker
from platform_integration.workers.scheduler import Scheduler, floor_slot


COMPONENT_ROOT = Path(__file__).parents[3]
RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")


class ShadowCommandError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def parse_rfc3339(value: str) -> datetime:
    if type(value) is not str or RFC3339_RE.fullmatch(value) is None:
        raise ShadowCommandError("RFC3339_TIMESTAMP_REQUIRED")
    try:
        normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        if parsed.utcoffset() is None:
            raise ShadowCommandError("RFC3339_TIMESTAMP_REQUIRED")
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError):
        raise ShadowCommandError("RFC3339_TIMESTAMP_REQUIRED") from None


def _database_settings() -> tuple[Settings, object]:
    settings = Settings()
    if settings.database_url is None or settings.tenant_id is None:
        raise ShadowCommandError("SHADOW_CONFIGURATION_INCOMPLETE")
    sessions = create_async_sessionmaker(settings.database_url.get_secret_value())
    return settings, sessions


def run_migrate() -> None:
    settings = Settings()
    if settings.database_url is None:
        raise ShadowCommandError("DATABASE_URL_REQUIRED")
    config = Config(str(COMPONENT_ROOT / "alembic.ini"))
    config.set_main_option(
        "sqlalchemy.url",
        settings.database_url.get_secret_value().replace("%", "%%"),
    )
    command.upgrade(config, "head")


async def _discover_identities() -> None:
    settings = Settings()
    if (
        settings.tb_base_url is None
        or settings.cmms_base_url is None
        or settings.tb_credential_ref is None
        or settings.cmms_credential_ref is None
    ):
        raise ShadowCommandError("IDENTITY_DISCOVERY_CONFIGURATION_INCOMPLETE")
    credentials = EnvironmentCredentialProvider()
    async with (
        httpx.AsyncClient(base_url=str(settings.tb_base_url)) as tb_http,
        httpx.AsyncClient(base_url=str(settings.cmms_base_url)) as cmms_http,
    ):
        tb_id = await ThingsBoardClient(
            http=tb_http,
            credentials=credentials,
            tb_credential_ref=settings.tb_credential_ref,
        ).authenticated_tenant_id()
        cmms_id = await CmmsClient(
            http=cmms_http,
            credentials=credentials,
            cmms_credential_ref=settings.cmms_credential_ref,
        ).authenticated_company_id()
    if settings.tb_tenant_id is not None and settings.tb_tenant_id != tb_id:
        raise ShadowCommandError("THINGSBOARD_TENANT_ID_MISMATCH")
    if settings.cmms_company_id is not None and settings.cmms_company_id != cmms_id:
        raise ShadowCommandError("CMMS_COMPANY_ID_MISMATCH")
    print(f"PLATFORM_INTEGRATION_TB_TENANT_ID={tb_id}")
    print(f"PLATFORM_INTEGRATION_CMMS_COMPANY_ID={cmms_id}")


def run_discover_identities() -> None:
    asyncio.run(_discover_identities())


def _install_stop_handlers(stopped: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for name in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(name, stopped.set)
        except NotImplementedError:
            signal.signal(name, lambda *_: loop.call_soon_threadsafe(stopped.set))


async def _scheduler(*, once: bool, now: datetime | None) -> None:
    settings, sessions = _database_settings()
    assert settings.tenant_id is not None
    store = ShadowExecutionStore(sessions=sessions, tenant_id=settings.tenant_id)
    scheduler = Scheduler(store=store, active_bindings=store.active_bindings)
    stopped = asyncio.Event()
    _install_stop_handlers(stopped)
    try:
        if once:
            await scheduler.schedule(floor_slot(now or datetime.now(UTC)))
            return
        while not stopped.is_set():
            await scheduler.schedule(floor_slot(datetime.now(UTC)))
            try:
                await asyncio.wait_for(stopped.wait(), timeout=30)
            except TimeoutError:
                pass
    finally:
        await sessions.kw["bind"].dispose()


def run_scheduler(*, once: bool, now: datetime | None) -> None:
    asyncio.run(_scheduler(once=once, now=now))


@dataclass
class _PredictionRuntime:
    worker: PredictionWorker
    tb_http: httpx.AsyncClient
    pdm_http: httpx.AsyncClient
    sessions: object

    async def close(self) -> None:
        await self.tb_http.aclose()
        await self.pdm_http.aclose()
        await self.sessions.kw["bind"].dispose()


def _prediction_runtime(
    settings: Settings,
    sessions,
    *,
    now: datetime | None = None,
) -> _PredictionRuntime:
    if (
        settings.tenant_id is None
        or settings.tb_base_url is None
        or settings.pdm_base_url is None
        or settings.tb_credential_ref is None
        or settings.pdm_credential_ref is None
    ):
        raise ShadowCommandError("PREDICTION_CONFIGURATION_INCOMPLETE")
    credentials = EnvironmentCredentialProvider()
    tb_http = httpx.AsyncClient(base_url=str(settings.tb_base_url))
    pdm_http = httpx.AsyncClient(base_url=str(settings.pdm_base_url))
    store = ShadowExecutionStore(sessions=sessions, tenant_id=settings.tenant_id)
    clock = (lambda: datetime.now(UTC)) if now is None else (lambda: now)
    processor = PredictionProcessor(
        store=store,
        thingsboard=ThingsBoardClient(
            http=tb_http,
            credentials=credentials,
            tb_credential_ref=settings.tb_credential_ref,
        ),
        pdm=PdmClient(
            http=pdm_http,
            credentials=credentials,
            pdm_credential_ref=settings.pdm_credential_ref,
        ),
        request_builder=PredictionRequestBuilder(),
        risk_evaluator=RiskEvaluator(),
        runtime_tb_credential_ref=str(settings.tb_credential_ref),
        runtime_pdm_credential_ref=str(settings.pdm_credential_ref),
        clock=clock,
    )
    return _PredictionRuntime(
        worker=PredictionWorker(
            store=store,
            processor=processor,
            owner=f"worker-{id(processor):x}",
            clock=clock,
        ),
        tb_http=tb_http,
        pdm_http=pdm_http,
        sessions=sessions,
    )


async def _prediction_worker(*, once: bool, now: datetime | None = None) -> None:
    settings, sessions = _database_settings()
    runtime = _prediction_runtime(settings, sessions, now=now)
    stopped = asyncio.Event()
    _install_stop_handlers(stopped)
    try:
        if once:
            await runtime.worker.run_once()
            return
        while not stopped.is_set():
            await runtime.worker.run_once(stop_requested=stopped)
            try:
                await asyncio.wait_for(stopped.wait(), timeout=5)
            except TimeoutError:
                pass
    finally:
        await runtime.close()


def run_prediction_worker(*, once: bool, now: datetime | None = None) -> None:
    asyncio.run(_prediction_worker(once=once, now=now))


async def _shadow_summary(tenant_alias: str, scheduled_at: datetime) -> None:
    settings, sessions = _database_settings()
    try:
        summary = await ShadowSummaryService(sessions=sessions).load(
            tenant_alias=tenant_alias,
            scheduled_at=scheduled_at,
            expected_tenant_id=settings.tenant_id,
        )
        print(rfc8785.dumps(summary).decode())
    finally:
        await sessions.kw["bind"].dispose()


def run_shadow_summary(tenant_alias: str, scheduled_at: datetime) -> None:
    asyncio.run(_shadow_summary(tenant_alias, scheduled_at))
