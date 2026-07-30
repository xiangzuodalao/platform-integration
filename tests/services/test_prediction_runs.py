from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select
from testcontainers.community.postgres import PostgresContainer


COMPONENT_ROOT = Path(__file__).parents[2]
TENANT_ID = UUID("60000000-0000-4000-8000-000000000001")
EQUIPMENT_ID = UUID("61000000-0000-4000-8000-000000000001")
TB_DEVICE_ID = UUID("62000000-0000-4000-8000-000000000001")
SLOT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)


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


async def seed_binding(session_factory):
    from platform_integration.models.bindings import (
        EquipmentMapping,
        MeasurementBinding,
        TenantBinding,
    )

    async with session_factory() as session, session.begin():
        if await session.get(TenantBinding, TENANT_ID) is not None:
            return
        session.add(
            TenantBinding(
                tenant_id=TENANT_ID,
                alias="shadow-store-test",
                tb_tenant_id=UUID("63000000-0000-4000-8000-000000000001"),
                cmms_company_id=7001,
                pdm_credential_ref="PDM_TEST_CREDENTIAL",
                tb_credential_ref="TB_TEST_CREDENTIAL",
                cmms_credential_ref="CMMS_TEST_CREDENTIAL",
                enabled=True,
            )
        )
        session.add(
            EquipmentMapping(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                tb_device_id=TB_DEVICE_ID,
                cmms_asset_id=8001,
                cmms_request_digest="a" * 64,
                status="ACTIVE",
                enabled=True,
            )
        )
        session.add(
            MeasurementBinding(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                telemetry_key="vibration",
                unit="mm/s",
                sampling_frequency="1min",
                request_window_points=66,
                context_points=60,
                horizon_points=15,
                value_scale=2,
                model_profile_id="pilot-cnc-vibration",
                model_info_id="pilot-fixture-v1-cnc-vibration",
                preprocessing_version="pdm-v2-pilot-1",
                policy_version="pilot-policy-v1",
                risk_direction="ABOVE",
                risk_threshold=Decimal("7.50"),
                enabled=True,
            )
        )


async def test_store_claim_joins_only_active_tenant_scoped_mapping(database_url):
    """Returning an unbound run could route telemetry or credentials across tenants."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore

    sessions = create_async_sessionmaker(database_url)
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=SLOT,
            )

        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        claim = await store.claim_next("worker-a", SLOT)

        assert claim is not None
        assert claim.binding.tb_device_id == TB_DEVICE_ID
        assert claim.binding.model_profile_id == "pilot-cnc-vibration"
        assert claim.binding.scheduled_at == SLOT
    finally:
        await sessions.kw["bind"].dispose()


async def test_success_persists_only_summary_and_updates_risk_in_the_same_transaction(database_url):
    """Separating run completion from risk counters can expose contradictory shadow evidence."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun, RiskEvaluationState
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore
    from platform_integration.services.risk import RoundRisk

    sessions = create_async_sessionmaker(database_url)
    next_slot = SLOT + timedelta(minutes=15)
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=next_slot,
            )
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        claim = await store.claim_next("worker-b", next_slot)
        assert claim is not None
        prepared = SimpleNamespace(
            request=SimpleNamespace(request_digest="b" * 64),
            quality_summary={
                "distinct_bucket_count": 66,
                "missing_bucket_count": 0,
                "raw_record_count": 66,
            },
        )
        response = SimpleNamespace(
            input_digest="c" * 64,
            model_artifact_sha256="d" * 64,
        )
        risk = RoundRisk(
            risky=True,
            threshold_crossing_count=2,
            forecast_min=Decimal("7.00"),
            forecast_max=Decimal("8.50"),
            forecast_mean=Decimal("7.60"),
        )
        await store.succeed(claim, prepared, response, risk)

        async with sessions() as session:
            run = await session.get(PredictionRun, claim.run_id)
            state = await session.scalar(
                select(RiskEvaluationState).where(
                    RiskEvaluationState.tenant_id == TENANT_ID,
                    RiskEvaluationState.equipment_id == EQUIPMENT_ID,
                    RiskEvaluationState.meas_code == "vibration_rms",
                )
            )
        assert run is not None and state is not None
        assert run.status == "SUCCEEDED"
        assert run.request_digest == "b" * 64
        assert run.input_digest == "c" * 64
        assert run.model_artifact_sha256 == "d" * 64
        assert run.quality_summary == prepared.quality_summary
        assert "history" not in repr(run.quality_summary)
        assert state.last_prediction_run_id == run.run_id
        assert state.consecutive_risk_count == 1
        assert state.alert_id is None
        assert state.alarm_aggregate_version == 0
    finally:
        await sessions.kw["bind"].dispose()


async def test_inactive_binding_is_terminalized_without_hiding_the_next_eligible_run(database_url):
    """Returning None after one bad binding makes --once leave valid work stranded."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore

    sessions = create_async_sessionmaker(database_url)
    invalid_slot = SLOT + timedelta(minutes=29)
    valid_slot = SLOT + timedelta(minutes=30)
    invalid_equipment = UUID("61000000-0000-4000-8000-000000000099")
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=invalid_equipment,
                meas_code="vibration_rms",
                scheduled_at=invalid_slot,
            )
            valid = await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=valid_slot,
            )
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        claimed = await store.claim_next("worker-c", valid_slot)

        assert claimed is not None
        assert claimed.run_id == valid.run_id
        async with sessions() as session:
            invalid = await session.scalar(
                select(PredictionRun).where(
                    PredictionRun.tenant_id == TENANT_ID,
                    PredictionRun.equipment_id == invalid_equipment,
                    PredictionRun.scheduled_at == invalid_slot,
                )
            )
        assert invalid is not None
        assert invalid.status == "FAILED"
        assert invalid.attempt == 3
        assert invalid.error_code == "PREDICTION_BINDING_INACTIVE"
    finally:
        await sessions.kw["bind"].dispose()


async def test_two_worker_first_risk_state_upsert_is_race_safe_and_newest_round_wins(
    database_url,
):
    """Select-then-insert can race on the risk-state primary key and lose one completion."""
    import asyncio

    from sqlalchemy import delete

    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun, RiskEvaluationState
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore
    from platform_integration.services.risk import RoundRisk

    sessions = create_async_sessionmaker(database_url)
    first_slot = SLOT + timedelta(minutes=45)
    second_slot = SLOT + timedelta(minutes=46)
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await session.execute(
                delete(RiskEvaluationState).where(
                    RiskEvaluationState.tenant_id == TENANT_ID,
                    RiskEvaluationState.equipment_id == EQUIPMENT_ID,
                    RiskEvaluationState.meas_code == "vibration_rms",
                )
            )
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=first_slot,
            )
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=second_slot,
            )
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        first = await store.claim_next("worker-d", first_slot)
        second = await store.claim_next("worker-e", second_slot)
        assert first is not None and second is not None
        prepared = SimpleNamespace(
            request=SimpleNamespace(request_digest="e" * 64),
            quality_summary={
                "distinct_bucket_count": 66,
                "missing_bucket_count": 0,
                "raw_record_count": 66,
            },
        )
        response = SimpleNamespace(
            input_digest="f" * 64,
            model_artifact_sha256="1" * 64,
        )
        risk = RoundRisk(
            risky=True,
            threshold_crossing_count=2,
            forecast_min=Decimal("7.00"),
            forecast_max=Decimal("8.50"),
            forecast_mean=Decimal("7.60"),
        )

        await asyncio.gather(
            store.succeed(first, prepared, response, risk),
            store.succeed(second, prepared, response, risk),
        )

        async with sessions() as session:
            state = await session.get(
                RiskEvaluationState,
                (TENANT_ID, EQUIPMENT_ID, "vibration_rms"),
            )
            completed = (
                await session.scalars(
                    select(PredictionRun).where(
                        PredictionRun.run_id.in_([first.run_id, second.run_id])
                    )
                )
            ).all()
        assert state is not None
        assert state.last_prediction_run_id == second.run_id
        assert state.alert_id is None
        assert state.consecutive_risk_count >= 1
        assert {run.status for run in completed} == {"SUCCEEDED"}
    finally:
        await sessions.kw["bind"].dispose()


async def test_persisted_internal_state_requires_two_risky_and_two_healthy_rounds(
    database_url,
):
    """Losing active state after the first healthy round violates the two-round clear policy."""
    from sqlalchemy import delete

    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import RiskEvaluationState
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore
    from platform_integration.services.risk import RoundRisk

    sessions = create_async_sessionmaker(database_url)
    slots = [SLOT + timedelta(hours=2, minutes=15 * index) for index in range(8)]
    prepared = SimpleNamespace(
        request=SimpleNamespace(request_digest="2" * 64),
        quality_summary={
            "distinct_bucket_count": 66,
            "missing_bucket_count": 0,
            "raw_record_count": 66,
        },
    )
    response = SimpleNamespace(input_digest="3" * 64, model_artifact_sha256="4" * 64)
    risky = RoundRisk(
        risky=True,
        threshold_crossing_count=2,
        forecast_min=Decimal("7.00"),
        forecast_max=Decimal("8.50"),
        forecast_mean=Decimal("7.60"),
    )
    healthy = RoundRisk(
        risky=False,
        threshold_crossing_count=0,
        forecast_min=Decimal("4.00"),
        forecast_max=Decimal("5.00"),
        forecast_mean=Decimal("4.50"),
    )
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await session.execute(
                delete(RiskEvaluationState).where(
                    RiskEvaluationState.tenant_id == TENANT_ID,
                    RiskEvaluationState.equipment_id == EQUIPMENT_ID,
                    RiskEvaluationState.meas_code == "vibration_rms",
                )
            )
            repository = PredictionRunRepository(session)
            for slot in slots:
                await repository.create_slot(
                    tenant_id=TENANT_ID,
                    equipment_id=EQUIPMENT_ID,
                    meas_code="vibration_rms",
                    scheduled_at=slot,
                )
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        observed = []
        rounds = [risky, risky, "failed", healthy, healthy, risky, risky, "skipped"]
        for index, (slot, round_risk) in enumerate(zip(slots, rounds, strict=True)):
            claim = await store.claim_next(f"worker-state-{index}", slot)
            assert claim is not None
            if round_risk == "failed":
                await store.fail(
                    claim,
                    "PDM_REQUEST_FAILED",
                    transient=False,
                    now=slot,
                )
            elif round_risk == "skipped":
                await store.skip(
                    claim,
                    "MISSING_RATIO_EXCEEDED",
                    {"distinct_bucket_count": 59, "missing_bucket_count": 7},
                    slot,
                )
            else:
                await store.succeed(claim, prepared, response, round_risk)
            async with sessions() as session:
                state = await session.get(
                    RiskEvaluationState,
                    (TENANT_ID, EQUIPMENT_ID, "vibration_rms"),
                )
                assert state is not None
                observed.append(
                    (
                        state.consecutive_risk_count,
                        state.consecutive_healthy_count,
                        state.internal_active,
                        state.alert_id,
                    )
                )

        assert observed == [
            (1, 0, False, None),
            (2, 0, True, None),
            (0, 0, True, None),
            (0, 1, True, None),
            (0, 2, False, None),
            (1, 0, False, None),
            (2, 0, True, None),
            (0, 0, True, None),
        ]
    finally:
        await sessions.kw["bind"].dispose()


async def test_transient_retry_of_the_same_run_can_apply_a_later_success(database_url):
    """Treating the same run as stale after a transient attempt loses recovered risk evidence."""
    from sqlalchemy import delete

    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import RiskEvaluationState
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore
    from platform_integration.services.risk import RoundRisk

    sessions = create_async_sessionmaker(database_url)
    slot = SLOT + timedelta(hours=5)
    prepared = SimpleNamespace(
        request=SimpleNamespace(request_digest="5" * 64),
        quality_summary={
            "distinct_bucket_count": 66,
            "missing_bucket_count": 0,
            "raw_record_count": 66,
        },
    )
    response = SimpleNamespace(input_digest="6" * 64, model_artifact_sha256="7" * 64)
    risk = RoundRisk(
        risky=True,
        threshold_crossing_count=2,
        forecast_min=Decimal("7.00"),
        forecast_max=Decimal("8.50"),
        forecast_mean=Decimal("7.60"),
    )
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await session.execute(
                delete(RiskEvaluationState).where(
                    RiskEvaluationState.tenant_id == TENANT_ID,
                    RiskEvaluationState.equipment_id == EQUIPMENT_ID,
                    RiskEvaluationState.meas_code == "vibration_rms",
                )
            )
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=slot,
            )
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        first = await store.claim_next("worker-retry-a", slot)
        assert first is not None
        await store.fail(
            first,
            "PDM_UNAVAILABLE",
            transient=True,
            now=slot + timedelta(minutes=1),
        )
        retry = await store.claim_next("worker-retry-b", slot + timedelta(minutes=1))
        assert retry is not None
        assert retry.run_id == first.run_id
        await store.succeed(retry, prepared, response, risk)

        async with sessions() as session:
            state = await session.get(
                RiskEvaluationState,
                (TENANT_ID, EQUIPMENT_ID, "vibration_rms"),
            )
        assert state is not None
        assert state.last_prediction_run_id == first.run_id
        assert state.consecutive_risk_count == 1
        assert not state.internal_active
    finally:
        await sessions.kw["bind"].dispose()
