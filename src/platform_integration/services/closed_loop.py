from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import rfc8785
from sqlalchemy import func, select, update

from platform_integration.models.bindings import EquipmentMapping, TenantBinding
from platform_integration.models.closed_loop import (
    AlarmProjection,
    MaintenanceAlert,
    MaintenanceWorkOrder,
    OutboxEvent,
)


SYSTEM_ACTOR_ID = UUID(int=0)


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


def risk_key(equipment_id: UUID, meas_code: str) -> str:
    return canonical_sha256(
        {
            "equipment_id": str(equipment_id),
            "meas_code": meas_code,
        }
    )


def decimal_string(value: Decimal) -> str:
    return format(value, "f")


class ClosedLoopTransitionService:
    """Project a committed prediction transition into durable integration intent."""

    def __init__(self, *, pilot_work_order_equipment_id: UUID | None) -> None:
        self._pilot_equipment_id = pilot_work_order_equipment_id

    async def apply(
        self,
        session,
        *,
        run,
        state,
        binding,
        risk,
        was_active: bool,
        now: datetime | None = None,
    ) -> None:
        now = now or datetime.now(UTC)
        became_active = not was_active and state.internal_active
        became_clear = was_active and not state.internal_active
        if not state.internal_active and not became_clear:
            return

        key = risk_key(run.equipment_id, run.meas_code)
        alert = None
        if became_active:
            alert = await self._create_alert(
                session,
                run=run,
                state=state,
                binding=binding,
                risk=risk,
                key=key,
                now=now,
            )
        elif state.alert_id is not None:
            alert = await session.get(MaintenanceAlert, state.alert_id)
        if alert is None:
            return

        alert.prediction_run_id = run.run_id
        alert.forecast_summary = self._forecast_summary(risk)
        alert.updated_at = now
        if became_clear:
            alert.risk_state = "CLEARED"
            alert.cleared_at = now
            alert.version += 1
        elif not became_active:
            alert.version += 1

        mapping = await session.get(EquipmentMapping, (run.tenant_id, run.equipment_id))
        if mapping is None:
            raise RuntimeError("CLOSED_LOOP_EQUIPMENT_MAPPING_MISSING")
        await self._replace_projection(
            session,
            state=state,
            alert=alert,
            tb_device_id=mapping.tb_device_id,
            desired_active=state.internal_active,
            now=now,
        )

    async def _create_alert(self, session, *, run, state, binding, risk, key, now):
        episode = (
            int(
                (
                    await session.scalar(
                        select(func.max(MaintenanceAlert.episode)).where(
                            MaintenanceAlert.tenant_id == run.tenant_id,
                            MaintenanceAlert.risk_key == key,
                        )
                    )
                )
                or 0
            )
            + 1
        )
        active_work_order = await session.scalar(
            select(MaintenanceWorkOrder)
            .join(MaintenanceAlert, MaintenanceAlert.alert_id == MaintenanceWorkOrder.alert_id)
            .where(
                MaintenanceAlert.tenant_id == run.tenant_id,
                MaintenanceAlert.risk_key == key,
                MaintenanceWorkOrder.maintenance_state.in_(
                    {
                        "WORK_ORDER_CREATING",
                        "WORK_ORDER_OPEN",
                        "WORK_ORDER_IN_PROGRESS",
                        "WORK_ORDER_ON_HOLD",
                    }
                ),
            )
            .limit(1)
        )
        allowed = (
            self._pilot_equipment_id is not None
            and run.equipment_id == self._pilot_equipment_id
            and active_work_order is None
        )
        maintenance_state = (
            "SUPPRESSED_ACTIVE_WORK_ORDER" if active_work_order is not None else "PENDING_APPROVAL"
        )
        alert = MaintenanceAlert(
            alert_id=uuid4(),
            tenant_id=run.tenant_id,
            equipment_id=run.equipment_id,
            meas_code=run.meas_code,
            risk_key=key,
            episode=episode,
            prediction_run_id=run.run_id,
            correlation_id=run.correlation_id,
            model_profile_id=run.model_profile_id,
            model_info_id=run.model_info_id,
            policy_version=run.policy_version,
            threshold_direction=binding.risk_direction,
            threshold_value=binding.risk_threshold,
            threshold_unit=binding.unit,
            forecast_summary=self._forecast_summary(risk),
            risk_state="ACTIVE",
            maintenance_state=maintenance_state,
            work_order_action_allowed=allowed,
            version=1,
            created_at=now,
            updated_at=now,
        )
        session.add(alert)
        state.alert_id = alert.alert_id
        return alert

    async def _replace_projection(
        self,
        session,
        *,
        state,
        alert,
        tb_device_id,
        desired_active,
        now,
    ) -> None:
        existing = await session.scalar(
            select(AlarmProjection)
            .where(
                AlarmProjection.tenant_id == alert.tenant_id,
                AlarmProjection.risk_key == alert.risk_key,
            )
            .with_for_update()
        )
        superseded_alert_ids = {alert.alert_id}
        if existing is not None and existing.alert_id != alert.alert_id:
            superseded_alert_ids.add(existing.alert_id)
            await session.delete(existing)
            await session.flush()
            existing = None
        state.alarm_aggregate_version += 1
        details = await self._details(session, alert)
        if existing is None:
            existing = AlarmProjection(
                alert_id=alert.alert_id,
                tenant_id=alert.tenant_id,
                risk_key=alert.risk_key,
                tb_device_id=tb_device_id,
                desired_active=desired_active,
                aggregate_version=state.alarm_aggregate_version,
                details=details,
                updated_at=now,
            )
            session.add(existing)
        else:
            existing.desired_active = desired_active
            existing.aggregate_version = state.alarm_aggregate_version
            existing.details = details
            existing.updated_at = now

        await session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.tenant_id == alert.tenant_id,
                OutboxEvent.event_type == "TB_ALARM_SYNC",
                OutboxEvent.aggregate_type == "ALARM_PROJECTION",
                OutboxEvent.aggregate_id.in_(superseded_alert_ids),
                OutboxEvent.status.in_({"PENDING", "RETRY_WAIT"}),
            )
            .values(status="SUPERSEDED", lease_owner=None, lease_expires_at=None)
        )
        session.add(
            OutboxEvent(
                event_id=uuid4(),
                tenant_id=alert.tenant_id,
                aggregate_type="ALARM_PROJECTION",
                aggregate_id=alert.alert_id,
                event_type="TB_ALARM_SYNC",
                delivery_kind="SUPERSEDABLE_STATE",
                aggregate_version=state.alarm_aggregate_version,
                status="PENDING",
                available_at=now,
            )
        )

    @staticmethod
    def _forecast_summary(risk) -> dict[str, object]:
        return {
            "minimum": decimal_string(risk.forecast_min),
            "maximum": decimal_string(risk.forecast_max),
            "mean": decimal_string(risk.forecast_mean),
            "crossing_count": risk.threshold_crossing_count,
        }

    @staticmethod
    async def _details(session, alert: MaintenanceAlert) -> dict[str, object]:
        work_order = await session.scalar(
            select(MaintenanceWorkOrder).where(MaintenanceWorkOrder.alert_id == alert.alert_id)
        )
        result: dict[str, object] = {
            "alert_id": str(alert.alert_id),
            "risk_key": alert.risk_key,
            "equipment_id": str(alert.equipment_id),
            "meas_code": alert.meas_code,
            "prediction_run_id": str(alert.prediction_run_id),
            "model_profile_id": alert.model_profile_id,
            "model_info_id": alert.model_info_id,
            "policy_version": alert.policy_version,
            "forecast_summary": alert.forecast_summary,
            "threshold": {
                "direction": alert.threshold_direction,
                "value": decimal_string(alert.threshold_value),
                "unit": alert.threshold_unit,
            },
            "risk_state": alert.risk_state,
            "maintenance_state": alert.maintenance_state,
            "maintenance_alert_version": alert.version,
            "work_order_action_allowed": alert.work_order_action_allowed,
            "correlation_id": str(alert.correlation_id),
        }
        if work_order is not None:
            result.update(
                {
                    "cmms_work_order_id": work_order.cmms_work_order_id,
                    "cmms_status": work_order.cmms_status,
                    "cmms_event_version": work_order.cmms_event_version,
                    "cmms_status_updated_at": (
                        work_order.status_updated_at.isoformat()
                        if work_order.status_updated_at is not None
                        else None
                    ),
                }
            )
        return result


async def tenant_for_alert(session, alert: MaintenanceAlert) -> TenantBinding:
    tenant = await session.get(TenantBinding, alert.tenant_id)
    if tenant is None or not tenant.enabled:
        raise RuntimeError("TENANT_BINDING_INACTIVE")
    return tenant
