from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select
from testcontainers.community.postgres import PostgresContainer


COMPONENT_ROOT = Path(__file__).parents[2]
TENANT_ID = UUID("40000000-0000-4000-8000-000000000001")
EQUIPMENT_ID = UUID("50000000-0000-4000-8000-000000000001")
SCHEDULED_AT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)


def _alembic_config(database_url: str) -> Config:
    config = Config(str(COMPONENT_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


@pytest.fixture(scope="module")
def database_url():
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
        url = postgres.get_connection_url()
        command.upgrade(_alembic_config(url), "head")
        yield url


async def _create_slot(session_factory, *, equipment_id=EQUIPMENT_ID, scheduled_at=SCHEDULED_AT):
    from platform_integration.repositories.prediction_runs import PredictionRunRepository

    async with session_factory() as session, session.begin():
        return await PredictionRunRepository(session).create_slot(
            tenant_id=TENANT_ID,
            equipment_id=equipment_id,
            meas_code="vibration_rms",
            scheduled_at=scheduled_at,
        )


async def test_prediction_slot_is_unique_and_creation_returns_the_existing_identity(database_url):
    """Creating a replacement row for the same slot breaks replay and stale-run recovery."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun

    session_factory = create_async_sessionmaker(database_url)
    equipment_id = UUID("50000000-0000-4000-8000-000000000010")
    try:
        first = await _create_slot(session_factory, equipment_id=equipment_id)
        second = await _create_slot(session_factory, equipment_id=equipment_id)

        async with session_factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(PredictionRun)
                .where(
                    PredictionRun.tenant_id == TENANT_ID,
                    PredictionRun.equipment_id == equipment_id,
                    PredictionRun.meas_code == "vibration_rms",
                    PredictionRun.scheduled_at == SCHEDULED_AT,
                )
            )
        assert second.run_id == first.run_id
        assert count == 1
    finally:
        await session_factory.kw["bind"].dispose()


async def test_only_one_worker_can_claim_a_locked_pending_run(database_url):
    """Removing SKIP LOCKED or the status guard permits two workers to own one run."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.repositories.prediction_runs import PredictionRunRepository

    session_factory = create_async_sessionmaker(database_url)
    run = await _create_slot(
        session_factory,
        equipment_id=UUID("50000000-0000-4000-8000-000000000020"),
    )
    try:
        async with session_factory() as first_session, first_session.begin():
            first = await PredictionRunRepository(first_session).claim(
                run.run_id,
                "worker-a",
                SCHEDULED_AT,
            )
            async with session_factory() as second_session, second_session.begin():
                second = await PredictionRunRepository(second_session).claim(
                    run.run_id,
                    "worker-b",
                    SCHEDULED_AT,
                )

            assert first is not None
            assert first.lease_owner == "worker-a"
            assert first.attempt == 1
            assert second is None

        async with session_factory() as session, session.begin():
            claimed = await session.get(PredictionRun, run.run_id)
            assert claimed is not None
            claimed.status = "SUCCEEDED"
    finally:
        await session_factory.kw["bind"].dispose()


async def test_heartbeat_extends_only_the_current_owners_live_lease(database_url):
    """Allowing another or expired owner to heartbeat can steal or resurrect a lease."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.repositories.prediction_runs import PredictionRunRepository

    session_factory = create_async_sessionmaker(database_url)
    run = await _create_slot(
        session_factory,
        equipment_id=UUID("50000000-0000-4000-8000-000000000030"),
    )
    try:
        async with session_factory() as session, session.begin():
            claimed = await PredictionRunRepository(session).claim(
                run.run_id,
                "worker-a",
                SCHEDULED_AT,
            )
            assert claimed is not None

        heartbeat_at = SCHEDULED_AT + timedelta(seconds=30)
        async with session_factory() as session, session.begin():
            repository = PredictionRunRepository(session)
            assert not await repository.heartbeat(run.run_id, "worker-b", heartbeat_at)
            assert await repository.heartbeat(run.run_id, "worker-a", heartbeat_at)

        async with session_factory() as session, session.begin():
            refreshed = await session.get(PredictionRun, run.run_id)
            assert refreshed is not None
            assert refreshed.heartbeat_at == heartbeat_at
            assert refreshed.lease_expires_at == heartbeat_at + timedelta(minutes=10)
            refreshed.status = "SUCCEEDED"
    finally:
        await session_factory.kw["bind"].dispose()


async def test_stale_run_reuses_the_same_row_through_attempt_three(database_url):
    """Inserting a retry slot or allowing attempt four breaks bounded crash recovery."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.repositories.prediction_runs import PredictionRunRepository

    session_factory = create_async_sessionmaker(database_url)
    equipment_id = UUID("50000000-0000-4000-8000-000000000040")
    run = await _create_slot(session_factory, equipment_id=equipment_id)
    now = SCHEDULED_AT
    try:
        for expected_attempt in (1, 2, 3):
            async with session_factory() as session, session.begin():
                claimed = await PredictionRunRepository(session).claim(
                    run.run_id,
                    f"worker-{expected_attempt}",
                    now,
                )
                assert claimed is not None
                assert claimed.run_id == run.run_id
                assert claimed.attempt == expected_attempt
                assert claimed.completed_at is None

            now += timedelta(minutes=10)
            async with session_factory() as session, session.begin():
                stale_ids = await PredictionRunRepository(session).reconcile_stale(now)
                assert stale_ids == [run.run_id]

            async with session_factory() as session:
                stale = await session.get(PredictionRun, run.run_id)
                assert stale is not None
                assert stale.status == "FAILED_STALE"
                assert stale.error_code == "LEASE_EXPIRED"

        async with session_factory() as session, session.begin():
            fourth = await PredictionRunRepository(session).claim(
                run.run_id,
                "worker-4",
                now,
            )
            assert fourth is None

        recreated = await _create_slot(session_factory, equipment_id=equipment_id)
        async with session_factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(PredictionRun)
                .where(
                    PredictionRun.tenant_id == TENANT_ID,
                    PredictionRun.equipment_id == equipment_id,
                    PredictionRun.meas_code == "vibration_rms",
                    PredictionRun.scheduled_at == SCHEDULED_AT,
                )
            )
        assert recreated.run_id == run.run_id
        assert count == 1
    finally:
        await session_factory.kw["bind"].dispose()
