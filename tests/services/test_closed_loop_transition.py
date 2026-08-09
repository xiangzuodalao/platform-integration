from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select
from testcontainers.community.postgres import PostgresContainer


COMPONENT_ROOT = Path(__file__).parents[2]
TENANT_ID = UUID("90000000-0000-4000-8000-000000000001")
EQUIPMENT_ID = UUID("91000000-0000-4000-8000-000000000001")
TB_DEVICE_ID = UUID("92000000-0000-4000-8000-000000000001")
SLOT = datetime(2026, 8, 4, 6, 0, tzinfo=UTC)
OTHER_ALERT_ID = UUID("94000000-0000-4000-8000-000000000001")


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


async def _seed(sessions):
    from platform_integration.models.bindings import (
        EquipmentMapping,
        MeasurementBinding,
        TenantBinding,
    )

    async with sessions() as session, session.begin():
        session.add(
            TenantBinding(
                tenant_id=TENANT_ID,
                alias="closed-loop-test",
                tb_tenant_id=UUID("93000000-0000-4000-8000-000000000001"),
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


async def _complete(store, sessions, slot, risk, owner):
    from platform_integration.repositories.prediction_runs import PredictionRunRepository

    async with sessions() as session, session.begin():
        await PredictionRunRepository(session).create_slot(
            tenant_id=TENANT_ID,
            equipment_id=EQUIPMENT_ID,
            meas_code="vibration_rms",
            scheduled_at=slot,
        )
    claim = await store.claim_next(owner, slot)
    assert claim is not None
    prepared = SimpleNamespace(
        request=SimpleNamespace(request_digest="b" * 64),
        quality_summary={
            "distinct_bucket_count": 66,
            "missing_bucket_count": 0,
            "raw_record_count": 66,
        },
    )
    response = SimpleNamespace(input_digest="c" * 64, model_artifact_sha256="d" * 64)
    assert await store.pin_request(claim, prepared, now=slot)
    await store.succeed(claim, prepared, response, risk)


async def test_two_risky_rounds_create_one_atomic_alert_and_two_healthy_rounds_clear_it(
    database_url,
):
    """The durable Alarm intent must use exactly the same transaction as the risk transition."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.closed_loop import (
        AlarmProjection,
        MaintenanceAlert,
        OutboxEvent,
    )
    from platform_integration.services.prediction_runs import ShadowExecutionStore
    from platform_integration.services.risk import RoundRisk

    sessions = create_async_sessionmaker(database_url)
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
        await _seed(sessions)
        store = ShadowExecutionStore(
            sessions=sessions,
            tenant_id=TENANT_ID,
            closed_loop_enabled=True,
            pilot_work_order_equipment_id=EQUIPMENT_ID,
        )
        await _complete(store, sessions, SLOT, risky, "risk-1")
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(MaintenanceAlert)) == 0

        await _complete(store, sessions, SLOT + timedelta(minutes=15), risky, "risk-2")
        async with sessions() as session:
            alert = await session.scalar(select(MaintenanceAlert))
            projection = await session.scalar(select(AlarmProjection))
            pending = await session.scalar(
                select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "PENDING")
            )
        assert alert is not None and projection is not None
        assert alert.risk_state == "ACTIVE"
        assert alert.maintenance_state == "PENDING_APPROVAL"
        assert alert.work_order_action_allowed
        assert projection.desired_active
        assert projection.details["forecast_summary"] == {
            "minimum": "7.00",
            "maximum": "8.50",
            "mean": "7.60",
            "crossing_count": 2,
        }
        assert len(alert.risk_key) == 64
        assert alert.version == 1
        assert pending == 1

        async with sessions() as session, session.begin():
            session.add(
                OutboxEvent(
                    event_id=UUID("95000000-0000-4000-8000-000000000001"),
                    tenant_id=TENANT_ID,
                    aggregate_type="ALARM_PROJECTION",
                    aggregate_id=OTHER_ALERT_ID,
                    event_type="TB_ALARM_SYNC",
                    delivery_kind="SUPERSEDABLE_STATE",
                    aggregate_version=1,
                    status="PENDING",
                    available_at=SLOT,
                )
            )

        await _complete(store, sessions, SLOT + timedelta(minutes=30), healthy, "healthy-1")
        async with sessions() as session:
            projection = await session.get(AlarmProjection, alert.alert_id)
            unrelated = await session.get(OutboxEvent, UUID("95000000-0000-4000-8000-000000000001"))
        assert projection is not None and projection.desired_active
        assert unrelated is not None and unrelated.status == "PENDING"

        await _complete(store, sessions, SLOT + timedelta(minutes=45), healthy, "healthy-2")
        async with sessions() as session:
            alert = await session.get(MaintenanceAlert, alert.alert_id)
            projection = await session.get(AlarmProjection, alert.alert_id)
            pending = await session.scalar(
                select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "PENDING")
            )
        assert alert is not None and projection is not None
        assert alert.risk_state == "CLEARED"
        assert not projection.desired_active
        assert pending == 2
    finally:
        await sessions.kw["bind"].dispose()
