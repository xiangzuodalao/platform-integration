from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update

from platform_integration.clients.thingsboard import (
    BrowserThingsBoardClient,
    ThingsBoardAlarm,
    ThingsBoardClientError,
    ThingsBoardUserIdentity,
)
from platform_integration.models.audit import AuditEvent
from platform_integration.models.bindings import EquipmentMapping, TenantBinding
from platform_integration.models.closed_loop import (
    AlarmProjection,
    AlertAction,
    MaintenanceAlert,
    MaintenanceWorkOrder,
    OutboxEvent,
)
from platform_integration.models.prediction import RiskEvaluationState
from platform_integration.services.closed_loop import (
    ClosedLoopTransitionService,
    canonical_sha256,
)


ACTION_KEY_RE = re.compile(
    r"^alert-action:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}:CREATE_WORK_ORDER$"
)


class MaintenanceActionError(RuntimeError):
    def __init__(self, code: str, status_code: int) -> None:
        self.code = code
        self.status_code = status_code
        super().__init__(code)


@dataclass(frozen=True)
class AuthorizedAlert:
    alert: MaintenanceAlert
    projection: AlarmProjection
    tenant: TenantBinding
    mapping: EquipmentMapping
    user: ThingsBoardUserIdentity
    alarm: ThingsBoardAlarm


class MaintenanceActionService:
    def __init__(
        self,
        *,
        sessions,
        thingsboard: BrowserThingsBoardClient,
        approver_tb_user_id: UUID,
        pilot_work_order_equipment_id: UUID,
    ) -> None:
        self._sessions = sessions
        self._thingsboard = thingsboard
        self._approver_id = approver_tb_user_id
        self._pilot_equipment_id = pilot_work_order_equipment_id

    async def preview(self, alert_id: UUID, authorization: str) -> dict[str, Any]:
        authorized = await self._authorize(alert_id, authorization)
        self._require_actionable(authorized)
        return self._plan(authorized)

    async def act(
        self,
        alert_id: UUID,
        authorization: str,
        *,
        idempotency_key: str,
        request: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        authorized = await self._authorize(alert_id, authorization)
        action = request.get("action")
        if action == "CREATE_WORK_ORDER":
            return await self._create_work_order(
                authorized,
                idempotency_key=idempotency_key,
                request=request,
            )
        if action == "CLOSE_RISK":
            return await self._close_risk(
                authorized,
                idempotency_key=idempotency_key,
                request=request,
            )
        raise MaintenanceActionError("ACTION_INVALID", 400)

    async def _authorize(self, alert_id: UUID, authorization: str) -> AuthorizedAlert:
        try:
            user = await self._thingsboard.authenticated_user(authorization)
        except ThingsBoardClientError as exc:
            raise self._map_tb_error(exc) from None
        async with self._sessions() as session:
            tenant = await session.scalar(
                select(TenantBinding).where(
                    TenantBinding.tb_tenant_id == user.tenant_id,
                    TenantBinding.enabled.is_(True),
                )
            )
            if tenant is None:
                raise MaintenanceActionError("TENANT_BINDING_NOT_FOUND", 403)
            alert = await session.scalar(
                select(MaintenanceAlert).where(
                    MaintenanceAlert.alert_id == alert_id,
                    MaintenanceAlert.tenant_id == tenant.tenant_id,
                )
            )
            if alert is None:
                raise MaintenanceActionError("MAINTENANCE_ALERT_NOT_FOUND", 404)
            projection = await session.get(AlarmProjection, alert.alert_id)
            mapping = await session.get(
                EquipmentMapping,
                (alert.tenant_id, alert.equipment_id),
            )
            if projection is None or projection.tb_alarm_id is None or mapping is None:
                raise MaintenanceActionError("ALARM_PROJECTION_NOT_READY", 409)
        try:
            alarm = await self._thingsboard.alarm_info(projection.tb_alarm_id, authorization)
        except ThingsBoardClientError as exc:
            raise self._map_tb_error(exc) from None
        if (
            alarm.tenant_id != tenant.tb_tenant_id
            or alarm.device_id != mapping.tb_device_id
            or alarm.alarm_type != "PDM_FORECAST_RISK"
            or alarm.details.get("alert_id") != str(alert.alert_id)
            or alarm.details.get("risk_key") != alert.risk_key
            or alarm.details.get("equipment_id") != str(alert.equipment_id)
            or alarm.details.get("meas_code") != alert.meas_code
        ):
            raise MaintenanceActionError("ALARM_IDENTITY_MISMATCH", 409)
        if user.user_id != self._approver_id:
            raise MaintenanceActionError("APPROVER_NOT_ALLOWED", 403)
        if alert.equipment_id != self._pilot_equipment_id:
            raise MaintenanceActionError("EQUIPMENT_ACTION_NOT_ALLOWED", 403)
        return AuthorizedAlert(
            alert=alert,
            projection=projection,
            tenant=tenant,
            mapping=mapping,
            user=user,
            alarm=alarm,
        )

    @staticmethod
    def _map_tb_error(exc: ThingsBoardClientError) -> MaintenanceActionError:
        status = (
            401
            if exc.code
            in {
                "THINGSBOARD_BROWSER_AUTH_INVALID",
                "THINGSBOARD_BROWSER_AUTH_REJECTED",
            }
            else 403
            if exc.code == "THINGSBOARD_ALARM_FORBIDDEN"
            else 502
        )
        return MaintenanceActionError(exc.code, status)

    @staticmethod
    def _require_actionable(authorized: AuthorizedAlert) -> None:
        alert = authorized.alert
        if (
            alert.risk_state != "ACTIVE"
            or alert.maintenance_state != "PENDING_APPROVAL"
            or not alert.work_order_action_allowed
            or not authorized.alarm.status.startswith("ACTIVE")
        ):
            raise MaintenanceActionError("MAINTENANCE_ALERT_NOT_ACTIONABLE", 409)
        if authorized.mapping.cmms_asset_id is None:
            raise MaintenanceActionError("CMMS_ASSET_MAPPING_MISSING", 409)

    @staticmethod
    def _work_order_request(authorized: AuthorizedAlert) -> dict[str, Any]:
        alert = authorized.alert
        return {
            "title": f"Predictive maintenance risk: {alert.equipment_id}/{alert.meas_code}",
            "description": (
                f"Forecast crossed {alert.threshold_direction} "
                f"{format(alert.threshold_value, 'f')} {alert.threshold_unit}; "
                f"correlation_id={alert.correlation_id}."
            ),
            "priority": "HIGH",
            "asset": {"id": authorized.mapping.cmms_asset_id},
            "external_source": "PDM_FORECAST",
            "external_ref": str(alert.alert_id),
            "correlation_id": str(alert.correlation_id),
            "equipment_id": str(alert.equipment_id),
            "tb_alarm_id": str(authorized.alarm.alarm_id),
            "model_profile_id": alert.model_profile_id,
            "model_info_id": alert.model_info_id,
            "policy_version": alert.policy_version,
        }

    def _plan(self, authorized: AuthorizedAlert) -> dict[str, Any]:
        alert = authorized.alert
        work_order = self._work_order_request(authorized)
        identity = {
            "schema_version": "1",
            "tenant_id": str(alert.tenant_id),
            "tb_tenant_id": str(authorized.tenant.tb_tenant_id),
            "cmms_company_id": authorized.tenant.cmms_company_id,
            "alert_id": str(alert.alert_id),
            "alert_version": alert.version,
            "approver_tb_user_id": str(authorized.user.user_id),
            "tb_alarm_id": str(authorized.alarm.alarm_id),
            "work_order": work_order,
        }
        return {
            "alert_id": str(alert.alert_id),
            "equipment_id": str(alert.equipment_id),
            "meas_code": alert.meas_code,
            "risk_state": alert.risk_state,
            "maintenance_state": alert.maintenance_state,
            "priority": "HIGH",
            "expected_version": alert.version,
            "forecast_summary": alert.forecast_summary,
            "threshold": {
                "direction": alert.threshold_direction,
                "value": format(alert.threshold_value, "f"),
                "unit": alert.threshold_unit,
            },
            "plan_hash": canonical_sha256(identity),
        }

    async def _create_work_order(
        self,
        authorized: AuthorizedAlert,
        *,
        idempotency_key: str,
        request: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        expected_key = f"alert-action:{authorized.alert.alert_id}:CREATE_WORK_ORDER"
        if idempotency_key != expected_key or ACTION_KEY_RE.fullmatch(idempotency_key) is None:
            raise MaintenanceActionError("IDEMPOTENCY_KEY_INVALID", 400)
        digest = canonical_sha256(request)
        now = datetime.now(UTC)
        async with self._sessions() as session, session.begin():
            state = await session.scalar(
                select(RiskEvaluationState)
                .where(
                    RiskEvaluationState.tenant_id == authorized.alert.tenant_id,
                    RiskEvaluationState.equipment_id == authorized.alert.equipment_id,
                    RiskEvaluationState.meas_code == authorized.alert.meas_code,
                )
                .with_for_update()
            )
            if state is None:
                raise MaintenanceActionError("ALARM_PROJECTION_NOT_READY", 409)
            alert = await session.scalar(
                select(MaintenanceAlert)
                .where(
                    MaintenanceAlert.alert_id == authorized.alert.alert_id,
                    MaintenanceAlert.tenant_id == authorized.alert.tenant_id,
                )
                .with_for_update()
            )
            if alert is None:
                raise MaintenanceActionError("MAINTENANCE_ALERT_NOT_FOUND", 404)
            replay = await session.scalar(
                select(AlertAction)
                .where(
                    AlertAction.tenant_id == alert.tenant_id,
                    AlertAction.idempotency_key == idempotency_key,
                )
                .with_for_update()
            )
            if replay is not None:
                if replay.request_digest != digest or replay.actor_id != authorized.user.user_id:
                    raise MaintenanceActionError("IDEMPOTENCY_CONFLICT", 409)
                return replay.result_summary or {}, True
            projection = await session.get(AlarmProjection, alert.alert_id, with_for_update=True)
            mapping = await session.get(
                EquipmentMapping,
                (alert.tenant_id, alert.equipment_id),
                with_for_update=True,
            )
            tenant = await session.get(TenantBinding, alert.tenant_id)
            if (
                projection is None
                or projection.tb_alarm_id != authorized.alarm.alarm_id
                or mapping is None
                or not mapping.enabled
                or mapping.status != "ACTIVE"
                or mapping.tb_device_id != authorized.alarm.device_id
                or tenant is None
                or not tenant.enabled
                or tenant.tb_tenant_id != authorized.user.tenant_id
            ):
                raise MaintenanceActionError("WORK_ORDER_PLAN_DRIFT", 409)
            current = AuthorizedAlert(
                alert=alert,
                projection=projection,
                tenant=tenant,
                mapping=mapping,
                user=authorized.user,
                alarm=authorized.alarm,
            )
            self._require_actionable(current)
            if request.get("expected_version") != alert.version:
                raise MaintenanceActionError("MAINTENANCE_ALERT_VERSION_STALE", 409)
            plan = self._plan(current)
            if request.get("confirmed_plan_hash") != plan["plan_hash"]:
                raise MaintenanceActionError("WORK_ORDER_PLAN_DRIFT", 409)
            work_order_body = self._work_order_request(current)
            work_order_digest = canonical_sha256(work_order_body)
            action_id = uuid4()
            result = {
                "action_id": str(action_id),
                "action": "CREATE_WORK_ORDER",
                "alert_id": str(alert.alert_id),
                "status": "ACCEPTED",
                "correlation_id": str(alert.correlation_id),
            }
            action = AlertAction(
                action_id=action_id,
                tenant_id=alert.tenant_id,
                alert_id=alert.alert_id,
                actor_id=authorized.user.user_id,
                action="CREATE_WORK_ORDER",
                idempotency_key=idempotency_key,
                request_digest=digest,
                expected_version=alert.version,
                correlation_id=alert.correlation_id,
                result_code="ACCEPTED",
                result_summary=result,
                created_at=now,
            )
            session.add(action)
            session.add(
                MaintenanceWorkOrder(
                    work_order_ref_id=uuid4(),
                    tenant_id=alert.tenant_id,
                    alert_id=alert.alert_id,
                    external_ref=alert.alert_id,
                    correlation_id=alert.correlation_id,
                    request_body=work_order_body,
                    request_digest=work_order_digest,
                    maintenance_state="WORK_ORDER_CREATING",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                OutboxEvent(
                    event_id=uuid4(),
                    tenant_id=alert.tenant_id,
                    aggregate_type="MAINTENANCE_WORK_ORDER",
                    aggregate_id=alert.alert_id,
                    event_type="CMMS_WORK_ORDER_CREATE",
                    delivery_kind="IMMUTABLE_COMMAND",
                    aggregate_version=1,
                    status="PENDING",
                    available_at=now,
                )
            )
            alert.maintenance_state = "WORK_ORDER_CREATING"
            alert.work_order_action_allowed = False
            alert.version += 1
            alert.updated_at = now
            await session.flush()
            await self._queue_alarm_refresh(session, alert, now)
            session.add(
                AuditEvent(
                    event_id=uuid4(),
                    tenant_id=alert.tenant_id,
                    actor_id=authorized.user.user_id,
                    action="CREATE_WORK_ORDER",
                    target_type="MAINTENANCE_ALERT",
                    target_id=str(alert.alert_id),
                    correlation_id=alert.correlation_id,
                    result_code="ACCEPTED",
                    result_summary={"idempotency_key": idempotency_key},
                    occurred_at=now,
                )
            )
            return result, False

    async def _close_risk(
        self,
        authorized: AuthorizedAlert,
        *,
        idempotency_key: str,
        request: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        reason = request.get("reason")
        if type(reason) is not str or not reason.strip() or len(reason) > 500:
            raise MaintenanceActionError("CLOSE_RISK_REASON_REQUIRED", 400)
        expected_key = f"alert-action:{authorized.alert.alert_id}:CLOSE_RISK"
        if idempotency_key != expected_key:
            raise MaintenanceActionError("IDEMPOTENCY_KEY_INVALID", 400)
        digest = canonical_sha256(request)
        now = datetime.now(UTC)
        async with self._sessions() as session, session.begin():
            state = await session.scalar(
                select(RiskEvaluationState)
                .where(
                    RiskEvaluationState.tenant_id == authorized.alert.tenant_id,
                    RiskEvaluationState.equipment_id == authorized.alert.equipment_id,
                    RiskEvaluationState.meas_code == authorized.alert.meas_code,
                )
                .with_for_update()
            )
            if state is None:
                raise MaintenanceActionError("ALARM_PROJECTION_NOT_READY", 409)
            alert = await session.scalar(
                select(MaintenanceAlert)
                .where(MaintenanceAlert.alert_id == authorized.alert.alert_id)
                .with_for_update()
            )
            if alert is None or alert.tenant_id != authorized.alert.tenant_id:
                raise MaintenanceActionError("MAINTENANCE_ALERT_NOT_FOUND", 404)
            replay = await session.scalar(
                select(AlertAction).where(
                    AlertAction.tenant_id == alert.tenant_id,
                    AlertAction.idempotency_key == idempotency_key,
                )
            )
            if replay is not None:
                if replay.request_digest != digest or replay.actor_id != authorized.user.user_id:
                    raise MaintenanceActionError("IDEMPOTENCY_CONFLICT", 409)
                return replay.result_summary or {}, True
            if request.get("expected_version") != alert.version or alert.risk_state != "ACTIVE":
                raise MaintenanceActionError("MAINTENANCE_ALERT_VERSION_STALE", 409)
            action_id = uuid4()
            result = {
                "action_id": str(action_id),
                "action": "CLOSE_RISK",
                "alert_id": str(alert.alert_id),
                "status": "ACCEPTED",
                "correlation_id": str(alert.correlation_id),
            }
            session.add(
                AlertAction(
                    action_id=action_id,
                    tenant_id=alert.tenant_id,
                    alert_id=alert.alert_id,
                    actor_id=authorized.user.user_id,
                    action="CLOSE_RISK",
                    idempotency_key=idempotency_key,
                    request_digest=digest,
                    expected_version=alert.version,
                    correlation_id=alert.correlation_id,
                    result_code="ACCEPTED",
                    result_summary=result,
                    created_at=now,
                )
            )
            alert.risk_state = "MANUALLY_CLOSED"
            alert.work_order_action_allowed = False
            alert.version += 1
            alert.cleared_at = now
            alert.updated_at = now
            state.internal_active = False
            state.consecutive_risk_count = 0
            state.consecutive_healthy_count = 0
            state.version += 1
            await self._queue_alarm_refresh(session, alert, now, desired_active=False)
            session.add(
                AuditEvent(
                    event_id=uuid4(),
                    tenant_id=alert.tenant_id,
                    actor_id=authorized.user.user_id,
                    action="CLOSE_RISK",
                    target_type="MAINTENANCE_ALERT",
                    target_id=str(alert.alert_id),
                    correlation_id=alert.correlation_id,
                    result_code="ACCEPTED",
                    result_summary={"reason": reason.strip()},
                    occurred_at=now,
                )
            )
            return result, False

    @staticmethod
    async def _queue_alarm_refresh(
        session,
        alert: MaintenanceAlert,
        now: datetime,
        *,
        desired_active: bool | None = None,
    ) -> None:
        state = await session.scalar(
            select(RiskEvaluationState)
            .where(
                RiskEvaluationState.tenant_id == alert.tenant_id,
                RiskEvaluationState.equipment_id == alert.equipment_id,
                RiskEvaluationState.meas_code == alert.meas_code,
            )
            .with_for_update()
        )
        projection = await session.get(AlarmProjection, alert.alert_id, with_for_update=True)
        if projection is None or state is None:
            raise MaintenanceActionError("ALARM_PROJECTION_NOT_READY", 409)
        state.alarm_aggregate_version += 1
        projection.aggregate_version = state.alarm_aggregate_version
        projection.desired_active = (
            alert.risk_state == "ACTIVE" if desired_active is None else desired_active
        )
        projection.details = await ClosedLoopTransitionService._details(session, alert)
        projection.updated_at = now
        await session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.tenant_id == alert.tenant_id,
                OutboxEvent.event_type == "TB_ALARM_SYNC",
                OutboxEvent.aggregate_type == "ALARM_PROJECTION",
                OutboxEvent.aggregate_id == alert.alert_id,
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
