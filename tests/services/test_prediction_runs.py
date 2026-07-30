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
        assert await store.pin_request(claim, prepared, now=next_slot)
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
    from platform_integration.models.prediction import PredictionRun, RiskEvaluationState
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
            session.add(
                RiskEvaluationState(
                    tenant_id=TENANT_ID,
                    equipment_id=invalid_equipment,
                    meas_code="vibration_rms",
                    policy_version="pilot-policy-v1",
                    consecutive_risk_count=2,
                    consecutive_healthy_count=1,
                    internal_active=True,
                    alarm_aggregate_version=0,
                    version=1,
                )
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
            state = await session.get(
                RiskEvaluationState,
                (TENANT_ID, invalid_equipment, "vibration_rms"),
            )
        assert invalid is not None
        assert state is not None
        assert invalid.status == "FAILED"
        assert invalid.attempt == 3
        assert invalid.error_code == "PREDICTION_BINDING_INACTIVE"
        assert state.last_prediction_run_id == invalid.run_id
        assert state.consecutive_risk_count == 0
        assert state.consecutive_healthy_count == 0
        assert state.internal_active
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
        assert await store.pin_request(first, prepared, now=first_slot)
        assert await store.pin_request(second, prepared, now=second_slot)

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
                assert await store.pin_request(claim, prepared, now=slot)
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
            session.add(
                RiskEvaluationState(
                    tenant_id=TENANT_ID,
                    equipment_id=EQUIPMENT_ID,
                    meas_code="vibration_rms",
                    policy_version="pilot-policy-v1",
                    consecutive_risk_count=2,
                    consecutive_healthy_count=1,
                    internal_active=True,
                    alarm_aggregate_version=0,
                    version=1,
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
        assert await store.pin_request(first, prepared, now=slot)
        await store.fail(
            first,
            "PDM_UNAVAILABLE",
            transient=True,
            now=slot + timedelta(minutes=1),
        )
        async with sessions() as session:
            failed_state = await session.get(
                RiskEvaluationState,
                (TENANT_ID, EQUIPMENT_ID, "vibration_rms"),
            )
        assert failed_state is not None
        assert failed_state.last_prediction_run_id == first.run_id
        assert failed_state.consecutive_risk_count == 0
        assert failed_state.consecutive_healthy_count == 0
        assert failed_state.internal_active
        retry = await store.claim_next("worker-retry-b", slot + timedelta(minutes=1))
        assert retry is not None
        assert retry.run_id == first.run_id
        assert await store.pin_request(
            retry,
            prepared,
            now=slot + timedelta(minutes=1),
        )
        await store.succeed(retry, prepared, response, risk)

        async with sessions() as session:
            state = await session.get(
                RiskEvaluationState,
                (TENANT_ID, EQUIPMENT_ID, "vibration_rms"),
            )
        assert state is not None
        assert state.last_prediction_run_id == first.run_id
        assert state.consecutive_risk_count == 1
        assert state.internal_active
    finally:
        await sessions.kw["bind"].dispose()


async def test_retry_request_drift_terminalizes_without_overwriting_the_pinned_snapshot(
    database_url,
):
    """Rebuilding a different request for one run breaks retry idempotency and audit identity."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.repositories.prediction_runs import PredictionRunRepository
    from platform_integration.services.prediction_runs import ShadowExecutionStore

    sessions = create_async_sessionmaker(database_url)
    slot = SLOT + timedelta(hours=6)
    first_prepared = SimpleNamespace(
        request=SimpleNamespace(request_digest="8" * 64),
        quality_summary={
            "distinct_bucket_count": 66,
            "missing_bucket_count": 0,
            "raw_record_count": 66,
        },
    )
    changed_prepared = SimpleNamespace(
        request=SimpleNamespace(request_digest="9" * 64),
        quality_summary=first_prepared.quality_summary,
    )
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            await PredictionRunRepository(session).create_slot(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                scheduled_at=slot,
            )
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)
        first = await store.claim_next("worker-drift-a", slot)
        assert first is not None
        assert await store.pin_request(first, first_prepared, now=slot)
        await store.fail(
            first,
            "PDM_UNAVAILABLE",
            transient=True,
            now=slot + timedelta(minutes=1),
        )
        retry = await store.claim_next("worker-drift-b", slot + timedelta(minutes=1))
        assert retry is not None
        assert retry.run_id == first.run_id

        assert not await store.pin_request(
            retry,
            changed_prepared,
            now=slot + timedelta(minutes=1),
        )

        async with sessions() as session:
            run = await session.get(PredictionRun, first.run_id)
        assert run is not None
        assert run.status == "FAILED"
        assert run.error_code == "PREDICTION_REQUEST_DRIFT"
        assert run.attempt == 3
        assert run.request_digest == "8" * 64
        assert run.model_profile_id == "pilot-cnc-vibration"
        assert run.model_info_id == "pilot-fixture-v1-cnc-vibration"
        assert run.preprocessing_version == "pdm-v2-pilot-1"
        assert run.policy_version == "pilot-policy-v1"
        assert run.quality_summary == first_prepared.quality_summary
    finally:
        await sessions.kw["bind"].dispose()


async def test_slot_batch_rolls_back_all_rows_when_one_insert_fails(database_url):
    """The scheduler batch must not commit a prefix if a later binding is invalid."""
    from sqlalchemy.exc import DataError

    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.services.prediction_runs import ShadowExecutionStore

    sessions = create_async_sessionmaker(database_url)
    slot = SLOT + timedelta(days=2)
    bindings = [
        SimpleNamespace(
            tenant_id=TENANT_ID,
            equipment_id=UUID(f"71000000-0000-4000-8000-{index:012d}"),
            meas_code=f"batch-{index:02d}",
        )
        for index in range(20)
    ]
    bindings[-1].meas_code = "x" * 101
    try:
        await seed_binding(sessions)
        store = ShadowExecutionStore(sessions=sessions, tenant_id=TENANT_ID)

        with pytest.raises(DataError):
            await store.create_slot_batch(bindings, scheduled_at=slot)

        async with sessions() as session:
            rows = (
                await session.scalars(
                    select(PredictionRun).where(PredictionRun.scheduled_at == slot)
                )
            ).all()
        assert rows == []
    finally:
        await sessions.kw["bind"].dispose()


async def test_shadow_summary_service_queries_one_complete_twenty_mapping_slot(
    database_url,
):
    """The production query path must preserve the exact pilot-slot evidence boundary."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.bindings import EquipmentMapping
    from platform_integration.models.prediction import PredictionRun
    from platform_integration.services.shadow_summary import ShadowSummaryService

    sessions = create_async_sessionmaker(database_url)
    slot = SLOT + timedelta(days=3)
    try:
        await seed_binding(sessions)
        async with sessions() as session, session.begin():
            for index in range(20):
                equipment_id = UUID(f"72000000-0000-4000-8000-{index:012d}")
                session.add(
                    EquipmentMapping(
                        tenant_id=TENANT_ID,
                        equipment_id=equipment_id,
                        tb_device_id=UUID(f"73000000-0000-4000-8000-{index:012d}"),
                        cmms_asset_id=9000 + index,
                        cmms_request_digest=f"{index:064x}",
                        status="ACTIVE",
                        enabled=True,
                    )
                )
                session.add(
                    PredictionRun(
                        tenant_id=TENANT_ID,
                        equipment_id=equipment_id,
                        meas_code=f"summary-{index:02d}",
                        scheduled_at=slot,
                        status="SUCCEEDED",
                        attempt=1,
                        model_profile_id=f"profile-{index % 4}",
                        model_artifact_sha256=f"{index % 4:x}" * 64,
                    )
                )

        summary = await ShadowSummaryService(sessions=sessions).load(
            tenant_alias="shadow-store-test",
            scheduled_at=slot,
            expected_tenant_id=TENANT_ID,
        )

        assert len(summary["mappings"]) == 20
        assert sum(summary["status_counts"].values()) == 20
        assert summary["status_counts"] == {"SUCCEEDED": 20}
    finally:
        await sessions.kw["bind"].dispose()
