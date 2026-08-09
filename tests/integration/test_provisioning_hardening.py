from __future__ import annotations

import asyncio
import hashlib
from contextlib import suppress
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest
import rfc8785
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from testcontainers.community.postgres import PostgresContainer

from platform_integration.db import create_async_sessionmaker
from platform_integration.repositories.provisioning import SqlProvisioningStore
from platform_integration.services.provisioning import ProvisioningError, canonical_plan_hash
from platform_integration.services.tenant_bindings import (
    ISOLATED_TENANT_ID,
    ValidatedTenantBinding,
)
from tests.services.test_provisioning import (
    ACTOR,
    APPLY_ACTOR,
    NOW,
    FakeCmms,
    FakeThingsBoard,
    service,
)


COMPONENT_ROOT = Path(__file__).parents[2]


def alembic_config(database_url: str) -> Config:
    config = Config(str(COMPONENT_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
        database_url = postgres.get_connection_url()
        command.upgrade(alembic_config(database_url), "head")
        yield database_url


@pytest.fixture
def clean_postgres(postgres_url):
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "TRUNCATE audit_event, measurement_binding, equipment_mapping, "
                    "provisioning_plan, tenant_binding CASCADE"
                )
            )
        yield postgres_url
    finally:
        engine.dispose()


def binding_snapshot() -> ValidatedTenantBinding:
    return ValidatedTenantBinding(
        tenant_id=ISOLATED_TENANT_ID,
        alias="ifactory-pilot",
        tb_tenant_id=ISOLATED_TENANT_ID,
        cmms_company_id=201,
        pdm_credential_ref="PDM_PILOT_CREDENTIAL",
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
        cmms_webhook_secret_ref=None,
    )


async def sql_service(database_url, *, now=lambda: NOW):
    sessions = create_async_sessionmaker(database_url)
    store = SqlProvisioningStore(sessions)
    tb = FakeThingsBoard()
    cmms = FakeCmms()
    provisioning, _, _, _ = service(tb=tb, cmms=cmms, store=store, now=now)
    return provisioning, store, tb, cmms, sessions


class ReadinessBarrierThingsBoard(FakeThingsBoard):
    def __init__(self) -> None:
        super().__init__()
        self.pause_next_device_list = False
        self.paused = asyncio.Event()
        self.release = asyncio.Event()

    async def list_devices(self):
        if self.pause_next_device_list:
            self.pause_next_device_list = False
            self.paused.set()
            await self.release.wait()
        return await super().list_devices()


@pytest.mark.asyncio
async def test_full_service_loser_repreflights_after_tenant_lock_before_external_writes(
    clean_postgres,
) -> None:
    """Moving persisted preflight before the tenant lock lets a stale loser write TB."""
    sessions = create_async_sessionmaker(clean_postgres)
    tb = ReadinessBarrierThingsBoard()
    cmms = FakeCmms()
    first_store = SqlProvisioningStore(sessions)
    second_store = SqlProvisioningStore(sessions)
    first_service, _, _, _ = service(tb=tb, cmms=cmms, store=first_store)
    second_service, _, _, _ = service(tb=tb, cmms=cmms, store=second_store)
    first = await first_service.build_plan(ISOLATED_TENANT_ID, ACTOR)
    second = deepcopy(first)
    second.targets[0].measurement_binding["risk_threshold"] = "6.00"
    second.canonical["targets"][0]["measurement_binding"]["risk_threshold"] = "6.00"
    second.plan_hash = canonical_plan_hash(second.canonical)
    await second_store.save_plan(binding_snapshot(), second)

    tb.pause_next_device_list = True
    loser = asyncio.create_task(
        second_service.apply(second.plan_hash, second.plan_hash, APPLY_ACTOR)
    )
    await tb.paused.wait()
    try:
        await first_service.apply(first.plan_hash, first.plan_hash, APPLY_ACTOR)
        cmms_writes_after_winner = len(cmms.create_calls)
        tb_writes_after_winner = len(tb.write_calls)
    finally:
        tb.release.set()

    with pytest.raises(ProvisioningError) as caught:
        await loser

    assert len(cmms.create_calls) == cmms_writes_after_winner
    assert len(tb.write_calls) == tb_writes_after_winner
    assert caught.value.code == "PROVISIONING_PERSISTED_STATE_CONFLICT"
    await sessions.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_tenant_lock_excludes_different_plan_hashes_on_a_pinned_connection(
    clean_postgres,
) -> None:
    """A plan-hash or pooled-session lock permits concurrent writes to the same tenant."""
    provisioning, first_store, _, _, sessions = await sql_service(clean_postgres)
    first = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    second = deepcopy(first)
    second.canonical["targets"][0]["asset_name"] = "competing-plan"
    second.plan_hash = canonical_plan_hash(second.canonical)
    second_store = SqlProvisioningStore(sessions)
    await second_store.save_plan(binding_snapshot(), second)

    await first_store.start_apply(first, APPLY_ACTOR, NOW)
    try:
        with pytest.raises(ProvisioningError, match="PROVISIONING_APPLY_CONCURRENT"):
            await second_store.start_apply(second, APPLY_ACTOR, NOW)

        engine = sessions.kw["bind"]
        async with engine.connect() as probe:
            acquired = await probe.scalar(
                text("SELECT pg_try_advisory_lock(hashtext(:tenant_id))"),
                {"tenant_id": str(ISOLATED_TENANT_ID)},
            )
            if acquired:
                await probe.execute(
                    text("SELECT pg_advisory_unlock(hashtext(:tenant_id))"),
                    {"tenant_id": str(ISOLATED_TENANT_ID)},
                )
            assert acquired is False
    finally:
        await first_store.abort_apply(first)

    await second_store.start_apply(second, APPLY_ACTOR, NOW)
    await second_store.abort_apply(second)
    await sessions.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_database_connection_loss_releases_tenant_apply_lock(clean_postgres) -> None:
    """A crashed worker must not leave a durable tenant lock that blocks recovery."""
    provisioning, crashed_store, _, _, sessions = await sql_service(clean_postgres)
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    await crashed_store.start_apply(plan, APPLY_ACTOR, NOW)

    connection = crashed_store._apply_connections.pop(ISOLATED_TENANT_ID)
    backend_pid = await connection.scalar(text("SELECT pg_backend_pid()"))
    killer = create_engine(clean_postgres, isolation_level="AUTOCOMMIT")
    try:
        with killer.connect() as external:
            assert external.execute(
                text("SELECT pg_terminate_backend(:pid)"), {"pid": backend_pid}
            ).scalar_one()
    finally:
        killer.dispose()
    with suppress(Exception):
        await connection.close()

    replacement = SqlProvisioningStore(sessions)
    await replacement.start_apply(plan, APPLY_ACTOR, NOW)
    await replacement.abort_apply(plan)
    await sessions.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_start_apply_query_failure_releases_tenant_lock(clean_postgres) -> None:
    """A failure after lock acquisition must close the pinned connection."""
    provisioning, store, _, _, sessions = await sql_service(clean_postgres)
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    engine = create_engine(clean_postgres)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE provisioning_plan RENAME TO provisioning_plan_hidden")
            )
        with pytest.raises(Exception, match="provisioning_plan"):
            await store.start_apply(plan, APPLY_ACTOR, NOW)
        with engine.connect() as probe:
            acquired = probe.execute(
                text("SELECT pg_try_advisory_lock(hashtext(:tenant_id))"),
                {"tenant_id": str(ISOLATED_TENANT_ID)},
            ).scalar_one()
            if acquired:
                probe.execute(
                    text("SELECT pg_advisory_unlock(hashtext(:tenant_id))"),
                    {"tenant_id": str(ISOLATED_TENANT_ID)},
                )
            assert acquired is True
    finally:
        await sessions.kw["bind"].dispose()
        with engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE provisioning_plan_hidden RENAME TO provisioning_plan")
            )
        engine.dispose()


@pytest.mark.asyncio
async def test_expired_unapplied_identical_plan_is_safely_renewed(clean_postgres) -> None:
    """A permanent tenant/hash conflict makes the required 30-minute plan impossible to reissue."""
    clock = [NOW]
    provisioning, _, _, _, sessions = await sql_service(clean_postgres, now=lambda: clock[0])
    first = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    clock[0] = NOW + timedelta(minutes=31)

    renewed = await provisioning.build_plan(ISOLATED_TENANT_ID, APPLY_ACTOR)

    assert renewed.plan_hash == first.plan_hash
    assert renewed.created_at == clock[0]
    assert renewed.expires_at == clock[0] + timedelta(minutes=30)
    engine = create_engine(clean_postgres)
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT count(*) AS count, min(actor_id::text) AS actor_id, "
                        "min(created_at) AS created_at FROM provisioning_plan"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
        await sessions.kw["bind"].dispose()
    assert row["count"] == 1
    assert row["actor_id"] == str(APPLY_ACTOR)
    assert row["created_at"] == clock[0]


@pytest.mark.asyncio
async def test_applied_plan_is_never_renewed(clean_postgres) -> None:
    clock = [NOW]
    provisioning, _, _, _, sessions = await sql_service(clean_postgres, now=lambda: clock[0])
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    await provisioning.apply(plan.plan_hash, plan.plan_hash, APPLY_ACTOR)
    clock[0] = NOW + timedelta(minutes=31)

    with pytest.raises(ProvisioningError, match="PROVISIONING_PLAN_ALREADY_EXISTS"):
        await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

    await sessions.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_in_progress_plan_is_never_renewed(clean_postgres) -> None:
    clock = [NOW]
    provisioning, store, _, _, sessions = await sql_service(clean_postgres, now=lambda: clock[0])
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    await store.start_apply(plan, APPLY_ACTOR, NOW)
    clock[0] = NOW + timedelta(minutes=31)
    try:
        with pytest.raises(ProvisioningError, match="PROVISIONING_APPLY_CONCURRENT"):
            await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    finally:
        await store.abort_apply(plan)
        await sessions.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_drifted_expired_plan_is_never_renewed(clean_postgres) -> None:
    clock = [NOW]
    provisioning, _, _, _, sessions = await sql_service(clean_postgres, now=lambda: clock[0])
    await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    engine = create_engine(clean_postgres)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE provisioning_plan "
                    "SET canonical_plan = jsonb_set("
                    "canonical_plan, '{tenant_alias}', '\"drifted\"'::jsonb)"
                )
            )
    finally:
        engine.dispose()
    clock[0] = NOW + timedelta(minutes=31)

    with pytest.raises(ProvisioningError, match="PROVISIONING_PLAN_ALREADY_EXISTS"):
        await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

    await sessions.kw["bind"].dispose()


def insert_mapping(connection, target, digest, summary, case):
    status = "ACTIVE" if case == "status" else "RESERVED"
    enabled = case != "enabled"
    cmms_asset_id = 777 if case == "cmms_asset_id" else None
    connection.execute(
        text(
            """
            INSERT INTO equipment_mapping (
                tenant_id, equipment_id, tb_device_id, cmms_asset_id,
                cmms_request_digest, cmms_request_summary, status, enabled
            ) VALUES (
                :tenant_id, :equipment_id, :tb_device_id, :cmms_asset_id,
                :digest, CAST(:summary AS jsonb), :status, :enabled
            )
            """
        ),
        {
            "tenant_id": str(ISOLATED_TENANT_ID),
            "equipment_id": str(target.equipment_id),
            "tb_device_id": str(target.tb_device_id),
            "cmms_asset_id": cmms_asset_id,
            "digest": digest,
            "summary": rfc8785.dumps(summary).decode(),
            "status": status,
            "enabled": enabled,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["status", "enabled", "cmms_asset_id", "measurement"])
async def test_complete_persisted_preflight_rejects_conflict_before_external_writes(
    clean_postgres, conflict
) -> None:
    """Any omitted persisted field can defer a conflict until after CMMS/TB writes."""
    provisioning, _, tb, cmms, sessions = await sql_service(clean_postgres)
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    target = plan.targets[0]
    summary = {"name": target.asset_name, "equipment_id": str(target.equipment_id)}
    digest = hashlib.sha256(rfc8785.dumps(summary)).hexdigest()
    engine = create_engine(clean_postgres)
    try:
        with engine.begin() as connection:
            insert_mapping(connection, target, digest, summary, conflict)
            if conflict == "measurement":
                connection.execute(
                    text(
                        """
                        INSERT INTO measurement_binding (
                            tenant_id, equipment_id, meas_code, telemetry_key, unit,
                            sampling_frequency, request_window_points, context_points,
                            horizon_points, value_scale, model_profile_id, model_info_id,
                            preprocessing_version, policy_version, risk_direction,
                            risk_threshold, enabled
                        ) VALUES (
                            :tenant_id, :equipment_id, 'unexpected', 'unexpected', 'x',
                            '1min', 66, 60, 15, 2, 'unexpected', 'unexpected',
                            'pdm-v2-pilot-1', 'pilot-threshold-v1', 'ABOVE', 1.00, true
                        )
                        """
                    ),
                    {
                        "tenant_id": str(ISOLATED_TENANT_ID),
                        "equipment_id": str(target.equipment_id),
                    },
                )
    finally:
        engine.dispose()

    with pytest.raises(ProvisioningError, match="PROVISIONING_PERSISTED_STATE_CONFLICT"):
        await provisioning.apply(plan.plan_hash, plan.plan_hash, APPLY_ACTOR)

    assert cmms.create_calls == []
    assert tb.write_calls == []
    await sessions.kw["bind"].dispose()
