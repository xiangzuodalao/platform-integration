from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select
from testcontainers.community.postgres import PostgresContainer

from platform_integration.clients.thingsboard import (
    ThingsBoardAlarm,
    ThingsBoardUserIdentity,
)
from platform_integration.services.closed_loop import canonical_sha256, risk_key
from platform_integration.services.maintenance_actions import (
    MaintenanceActionError,
    MaintenanceActionService,
)


COMPONENT_ROOT = Path(__file__).parents[2]
TENANT_ID = UUID("a0000000-0000-4000-8000-000000000001")
TB_TENANT_ID = UUID("a1000000-0000-4000-8000-000000000001")
EQUIPMENT_ID = UUID("a2000000-0000-4000-8000-000000000001")
TB_DEVICE_ID = UUID("a3000000-0000-4000-8000-000000000001")
APPROVER_ID = UUID("a4000000-0000-4000-8000-000000000001")
ALERT_ID = UUID("a5000000-0000-4000-8000-000000000001")
ALARM_ID = UUID("a6000000-0000-4000-8000-000000000001")
RUN_ID = UUID("a7000000-0000-4000-8000-000000000001")
CORRELATION_ID = UUID("a8000000-0000-4000-8000-000000000001")
NOW = datetime(2026, 8, 4, 6, 0, tzinfo=UTC)
RISK_KEY = risk_key(EQUIPMENT_ID, "vibration_rms")


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
    from platform_integration.models.bindings import EquipmentMapping, TenantBinding
    from platform_integration.models.closed_loop import AlarmProjection, MaintenanceAlert
    from platform_integration.models.prediction import RiskEvaluationState

    async with sessions() as session, session.begin():
        session.add(
            TenantBinding(
                tenant_id=TENANT_ID,
                alias="action-test",
                tb_tenant_id=TB_TENANT_ID,
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
        alert = MaintenanceAlert(
            alert_id=ALERT_ID,
            tenant_id=TENANT_ID,
            equipment_id=EQUIPMENT_ID,
            meas_code="vibration_rms",
            risk_key=RISK_KEY,
            episode=1,
            prediction_run_id=RUN_ID,
            correlation_id=CORRELATION_ID,
            model_profile_id="pilot-profile",
            model_info_id="pilot-model",
            policy_version="pilot-policy-v1",
            threshold_direction="ABOVE",
            threshold_value=Decimal("7.50"),
            threshold_unit="mm/s",
            forecast_summary={
                "minimum": "7.00",
                "maximum": "8.50",
                "mean": "7.60",
                "crossing_count": 2,
            },
            risk_state="ACTIVE",
            maintenance_state="PENDING_APPROVAL",
            work_order_action_allowed=True,
            version=1,
            created_at=NOW,
            updated_at=NOW,
        )
        session.add(alert)
        await session.flush()
        session.add(
            AlarmProjection(
                alert_id=ALERT_ID,
                tenant_id=TENANT_ID,
                risk_key=RISK_KEY,
                tb_device_id=TB_DEVICE_ID,
                tb_alarm_id=ALARM_ID,
                desired_active=True,
                aggregate_version=1,
                details={"alert_id": str(ALERT_ID)},
                updated_at=NOW,
            )
        )
        session.add(
            RiskEvaluationState(
                tenant_id=TENANT_ID,
                equipment_id=EQUIPMENT_ID,
                meas_code="vibration_rms",
                policy_version="pilot-policy-v1",
                consecutive_risk_count=2,
                consecutive_healthy_count=0,
                internal_active=True,
                last_prediction_run_id=RUN_ID,
                alert_id=ALERT_ID,
                alarm_aggregate_version=1,
                version=2,
            )
        )


class FakeThingsBoard:
    def __init__(self, user_id=APPROVER_ID):
        self.user_id = user_id

    async def authenticated_user(self, authorization):
        assert authorization == "Bearer browser-token"
        return ThingsBoardUserIdentity(user_id=self.user_id, tenant_id=TB_TENANT_ID)

    async def alarm_info(self, alarm_id, authorization):
        assert alarm_id == ALARM_ID
        assert authorization == "Bearer browser-token"
        return ThingsBoardAlarm(
            alarm_id=ALARM_ID,
            tenant_id=TB_TENANT_ID,
            device_id=TB_DEVICE_ID,
            alarm_type="PDM_FORECAST_RISK",
            severity="WARNING",
            status="ACTIVE_ACK",
            details={
                "alert_id": str(ALERT_ID),
                "risk_key": RISK_KEY,
                "equipment_id": str(EQUIPMENT_ID),
                "meas_code": "vibration_rms",
            },
            update_fields={},
        )


async def test_preview_binds_exact_provider_identity_and_action_is_replay_safe(database_url):
    """One confirmed hash must enqueue one immutable CMMS command under concurrent-safe replay."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.closed_loop import (
        AlertAction,
        MaintenanceWorkOrder,
        OutboxEvent,
    )

    sessions = create_async_sessionmaker(database_url)
    try:
        await _seed(sessions)
        service = MaintenanceActionService(
            sessions=sessions,
            thingsboard=FakeThingsBoard(),
            approver_tb_user_id=APPROVER_ID,
            pilot_work_order_equipment_id=EQUIPMENT_ID,
        )
        preview = await service.preview(ALERT_ID, "Bearer browser-token")
        expected_key = f"alert-action:{ALERT_ID}:CREATE_WORK_ORDER"
        request = {
            "action": "CREATE_WORK_ORDER",
            "expected_version": 1,
            "confirmed_plan_hash": preview["plan_hash"],
        }

        first, replayed = await service.act(
            ALERT_ID,
            "Bearer browser-token",
            idempotency_key=expected_key,
            request=request,
        )
        replay, was_replay = await service.act(
            ALERT_ID,
            "Bearer browser-token",
            idempotency_key=expected_key,
            request=request,
        )

        assert not replayed and was_replay
        assert replay == first
        assert set(preview) == {
            "alert_id",
            "equipment_id",
            "meas_code",
            "risk_state",
            "maintenance_state",
            "expected_version",
            "priority",
            "forecast_summary",
            "threshold",
            "plan_hash",
        }
        assert preview["expected_version"] == 1
        assert preview["forecast_summary"]["minimum"] == "7.00"
        assert len(preview["plan_hash"]) == 64
        assert UUID(first["action_id"])
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(AlertAction)) == 1
            assert await session.scalar(select(func.count()).select_from(MaintenanceWorkOrder)) == 1
            commands = await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.event_type == "CMMS_WORK_ORDER_CREATE")
            )
        assert commands == 1

        conflicting = dict(request, expected_version=2)
        with pytest.raises(MaintenanceActionError) as exc_info:
            await service.act(
                ALERT_ID,
                "Bearer browser-token",
                idempotency_key=expected_key,
                request=conflicting,
            )
        assert exc_info.value.code == "IDEMPOTENCY_CONFLICT"
        assert canonical_sha256(request) != canonical_sha256(conflicting)
    finally:
        await sessions.kw["bind"].dispose()


async def test_wrong_approver_fails_before_any_action_write(database_url):
    """A valid alarm-visible user outside the exact allowlist must not reach idempotency state."""
    from platform_integration.db import create_async_sessionmaker
    from platform_integration.models.closed_loop import AlertAction

    sessions = create_async_sessionmaker(database_url)
    try:
        service = MaintenanceActionService(
            sessions=sessions,
            thingsboard=FakeThingsBoard(UUID("afffffff-0000-4000-8000-000000000001")),
            approver_tb_user_id=APPROVER_ID,
            pilot_work_order_equipment_id=EQUIPMENT_ID,
        )
        with pytest.raises(MaintenanceActionError) as exc_info:
            await service.preview(ALERT_ID, "Bearer browser-token")
        assert exc_info.value.code == "APPROVER_NOT_ALLOWED"
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(AlertAction)) == 1
    finally:
        await sessions.kw["bind"].dispose()
