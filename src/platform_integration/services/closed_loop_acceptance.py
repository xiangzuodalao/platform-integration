from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

import rfc8785
from sqlalchemy import func, select

from platform_integration.models.bindings import (
    EquipmentMapping,
    MeasurementBinding,
    TenantBinding,
)
from platform_integration.models.closed_loop import (
    AlarmProjection,
    MaintenanceAlert,
    MaintenanceWorkOrder,
    OutboxEvent,
)
from platform_integration.models.prediction import RiskEvaluationState
from platform_integration.services.closed_loop import risk_key
from platform_integration.services.closed_loop_delivery import work_order_identity_matches


EXPECTED_STAGES = frozenset({"CONSISTENT", "ACTIVE", "IN_PROGRESS", "COMPLETE", "CLEARED"})
OUTBOX_STATUSES = (
    "PENDING",
    "IN_FLIGHT",
    "RETRY_WAIT",
    "DELIVERED",
    "SUPERSEDED",
    "DEAD_LETTER",
)
UNSETTLED_OUTBOX_STATUSES = frozenset({"PENDING", "IN_FLIGHT", "RETRY_WAIT"})


class ClosedLoopAcceptanceError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ClosedLoopAcceptanceSnapshot:
    tenant_id: UUID
    tenant_alias: str
    tb_tenant_id: UUID
    cmms_company_id: int
    tb_credential_ref: str
    cmms_credential_ref: str
    equipment_id: UUID
    tb_device_id: UUID
    cmms_asset_id: int
    meas_code: str
    state_alert_id: UUID
    state_internal_active: bool
    consecutive_healthy_count: int
    state_alarm_aggregate_version: int
    alert_id: UUID
    risk_key: str
    episode: int
    correlation_id: UUID
    model_profile_id: str
    model_info_id: str
    policy_version: str
    alert_risk_state: str
    alert_maintenance_state: str
    alert_version: int
    alarm_id: UUID
    alarm_desired_active: bool
    alarm_type: str
    alarm_severity: str
    alarm_aggregate_version: int
    alarm_details: dict[str, Any]
    external_ref: UUID
    work_order_request_body: dict[str, Any]
    cmms_work_order_id: int
    cmms_status: str
    cmms_event_version: int
    work_order_maintenance_state: str
    outbox: dict[str, int]


class ClosedLoopAcceptanceStore:
    """Read one exact selected-equipment aggregate without taking locks or writing state."""

    def __init__(self, *, sessions, tenant_id: UUID) -> None:
        self._sessions = sessions
        self._tenant_id = tenant_id

    async def read(
        self,
        *,
        tenant_alias: str,
        equipment_id: UUID,
    ) -> ClosedLoopAcceptanceSnapshot:
        async with self._sessions() as session:
            tenant = await session.scalar(
                select(TenantBinding).where(
                    TenantBinding.tenant_id == self._tenant_id,
                    TenantBinding.alias == tenant_alias,
                )
            )
            if tenant is None or not tenant.enabled:
                raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_TENANT_MISMATCH")

            mapping = await session.get(EquipmentMapping, (self._tenant_id, equipment_id))
            if (
                mapping is None
                or not mapping.enabled
                or mapping.status != "ACTIVE"
                or mapping.cmms_asset_id is None
            ):
                raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_EQUIPMENT_MISMATCH")

            bindings = (
                await session.scalars(
                    select(MeasurementBinding)
                    .where(
                        MeasurementBinding.tenant_id == self._tenant_id,
                        MeasurementBinding.equipment_id == equipment_id,
                        MeasurementBinding.enabled.is_(True),
                    )
                    .order_by(MeasurementBinding.meas_code)
                )
            ).all()
            if len(bindings) != 1:
                raise ClosedLoopAcceptanceError(
                    "CLOSED_LOOP_ACCEPTANCE_MEASUREMENT_BINDING_MISMATCH"
                )
            binding = bindings[0]

            state = await session.get(
                RiskEvaluationState,
                (self._tenant_id, equipment_id, binding.meas_code),
            )
            if state is None or state.alert_id is None:
                raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")

            alert = await session.get(MaintenanceAlert, state.alert_id)
            if (
                alert is None
                or alert.tenant_id != self._tenant_id
                or alert.equipment_id != equipment_id
                or alert.meas_code != binding.meas_code
            ):
                raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_AGGREGATE_MISMATCH")

            projection = await session.get(AlarmProjection, alert.alert_id)
            if projection is None or projection.tb_alarm_id is None or projection.details is None:
                raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")

            work_order = await session.scalar(
                select(MaintenanceWorkOrder).where(
                    MaintenanceWorkOrder.tenant_id == self._tenant_id,
                    MaintenanceWorkOrder.alert_id == alert.alert_id,
                )
            )
            if (
                work_order is None
                or work_order.cmms_work_order_id is None
                or work_order.cmms_status is None
                or work_order.cmms_event_version is None
            ):
                raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")

            outbox_rows = (
                await session.execute(
                    select(OutboxEvent.status, func.count())
                    .where(OutboxEvent.tenant_id == self._tenant_id)
                    .group_by(OutboxEvent.status)
                )
            ).all()
            outbox = {status: 0 for status in OUTBOX_STATUSES}
            for status, count in outbox_rows:
                if status not in outbox or type(count) is not int or count < 0:
                    raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_OUTBOX_INVALID")
                outbox[status] = count

            return ClosedLoopAcceptanceSnapshot(
                tenant_id=tenant.tenant_id,
                tenant_alias=tenant.alias,
                tb_tenant_id=tenant.tb_tenant_id,
                cmms_company_id=tenant.cmms_company_id,
                tb_credential_ref=tenant.tb_credential_ref,
                cmms_credential_ref=tenant.cmms_credential_ref,
                equipment_id=mapping.equipment_id,
                tb_device_id=mapping.tb_device_id,
                cmms_asset_id=mapping.cmms_asset_id,
                meas_code=binding.meas_code,
                state_alert_id=state.alert_id,
                state_internal_active=state.internal_active,
                consecutive_healthy_count=state.consecutive_healthy_count,
                state_alarm_aggregate_version=state.alarm_aggregate_version,
                alert_id=alert.alert_id,
                risk_key=alert.risk_key,
                episode=alert.episode,
                correlation_id=alert.correlation_id,
                model_profile_id=alert.model_profile_id,
                model_info_id=alert.model_info_id,
                policy_version=alert.policy_version,
                alert_risk_state=alert.risk_state,
                alert_maintenance_state=alert.maintenance_state,
                alert_version=alert.version,
                alarm_id=projection.tb_alarm_id,
                alarm_desired_active=projection.desired_active,
                alarm_type=projection.alarm_type,
                alarm_severity=projection.severity,
                alarm_aggregate_version=projection.aggregate_version,
                alarm_details=dict(projection.details),
                external_ref=work_order.external_ref,
                work_order_request_body=dict(work_order.request_body),
                cmms_work_order_id=work_order.cmms_work_order_id,
                cmms_status=work_order.cmms_status,
                cmms_event_version=work_order.cmms_event_version,
                work_order_maintenance_state=work_order.maintenance_state,
                outbox=outbox,
            )


class ThingsBoardAcceptanceReader(Protocol):
    async def authenticated_tenant_id(self) -> UUID: ...

    async def alarm_state(self, alarm_id: UUID): ...


class CmmsAcceptanceReader(Protocol):
    async def authenticated_company_id(self) -> int: ...

    async def find_work_order_by_external_ref(self, external_ref: UUID): ...


class ClosedLoopAcceptanceVerifier:
    def __init__(
        self,
        *,
        store: ClosedLoopAcceptanceStore,
        thingsboard: ThingsBoardAcceptanceReader,
        cmms: CmmsAcceptanceReader,
        tenant_id: UUID,
        tb_tenant_id: UUID,
        cmms_company_id: int,
        tb_credential_ref: str,
        cmms_credential_ref: str,
        equipment_id: UUID,
    ) -> None:
        self._store = store
        self._thingsboard = thingsboard
        self._cmms = cmms
        self._tenant_id = tenant_id
        self._tb_tenant_id = tb_tenant_id
        self._cmms_company_id = cmms_company_id
        self._tb_credential_ref = tb_credential_ref
        self._cmms_credential_ref = cmms_credential_ref
        self._equipment_id = equipment_id

    async def verify(self, *, tenant_alias: str, expected_stage: str) -> dict[str, Any]:
        if expected_stage not in EXPECTED_STAGES:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_STAGE_INVALID")
        snapshot = await self._store.read(
            tenant_alias=tenant_alias,
            equipment_id=self._equipment_id,
        )
        self._validate_local_identity(snapshot, tenant_alias)
        self._require_settled_outbox(snapshot)

        authenticated_tb_tenant = await self._thingsboard.authenticated_tenant_id()
        if authenticated_tb_tenant != self._tb_tenant_id:
            raise ClosedLoopAcceptanceError("THINGSBOARD_TENANT_ID_MISMATCH")
        alarm = await self._thingsboard.alarm_state(snapshot.alarm_id)
        self._validate_alarm(snapshot, alarm)

        authenticated_cmms_company = await self._cmms.authenticated_company_id()
        if authenticated_cmms_company != self._cmms_company_id:
            raise ClosedLoopAcceptanceError("CMMS_COMPANY_ID_MISMATCH")
        work_order = await self._cmms.find_work_order_by_external_ref(snapshot.external_ref)
        if work_order is None:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
        self._validate_work_order(snapshot, work_order)

        confirmed_snapshot = await self._store.read(
            tenant_alias=tenant_alias,
            equipment_id=self._equipment_id,
        )
        if confirmed_snapshot != snapshot:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
        self._require_settled_outbox(confirmed_snapshot)

        observed_stage = self._observed_stage(snapshot, alarm)
        self._validate_expected_stage(snapshot, alarm, expected_stage, observed_stage)
        return self._evidence(snapshot, alarm, work_order, expected_stage, observed_stage)

    def _validate_local_identity(
        self,
        snapshot: ClosedLoopAcceptanceSnapshot,
        tenant_alias: str,
    ) -> None:
        if (
            snapshot.tenant_id != self._tenant_id
            or snapshot.tenant_alias != tenant_alias
            or snapshot.tb_tenant_id != self._tb_tenant_id
            or snapshot.cmms_company_id != self._cmms_company_id
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_TENANT_MISMATCH")
        if (
            snapshot.tb_credential_ref != self._tb_credential_ref
            or snapshot.cmms_credential_ref != self._cmms_credential_ref
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_CREDENTIAL_REF_MISMATCH")
        if snapshot.equipment_id != self._equipment_id:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_EQUIPMENT_MISMATCH")
        if (
            snapshot.state_alert_id != snapshot.alert_id
            or snapshot.risk_key != risk_key(snapshot.equipment_id, snapshot.meas_code)
            or snapshot.alarm_aggregate_version != snapshot.state_alarm_aggregate_version
            or snapshot.external_ref != snapshot.alert_id
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_AGGREGATE_MISMATCH")
        if snapshot.alarm_desired_active != (snapshot.alert_risk_state == "ACTIVE"):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_AGGREGATE_MISMATCH")
        if snapshot.state_internal_active != snapshot.alarm_desired_active:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_AGGREGATE_MISMATCH")
        if snapshot.work_order_maintenance_state != snapshot.alert_maintenance_state:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_WORK_ORDER_MISMATCH")
        asset = snapshot.work_order_request_body.get("asset")
        if (
            snapshot.work_order_request_body.get("external_source") != "PDM_FORECAST"
            or snapshot.work_order_request_body.get("external_ref") != str(snapshot.alert_id)
            or snapshot.work_order_request_body.get("correlation_id")
            != str(snapshot.correlation_id)
            or snapshot.work_order_request_body.get("equipment_id") != str(snapshot.equipment_id)
            or snapshot.work_order_request_body.get("tb_alarm_id") != str(snapshot.alarm_id)
            or snapshot.work_order_request_body.get("model_profile_id") != snapshot.model_profile_id
            or snapshot.work_order_request_body.get("model_info_id") != snapshot.model_info_id
            or snapshot.work_order_request_body.get("policy_version") != snapshot.policy_version
            or type(asset) is not dict
            or asset.get("id") != snapshot.cmms_asset_id
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_WORK_ORDER_MISMATCH")
        expected_maintenance_state = {
            "OPEN": "WORK_ORDER_OPEN",
            "IN_PROGRESS": "WORK_ORDER_IN_PROGRESS",
            "ON_HOLD": "WORK_ORDER_ON_HOLD",
            "COMPLETE": "WORK_ORDER_COMPLETE",
        }.get(snapshot.cmms_status)
        if expected_maintenance_state != snapshot.work_order_maintenance_state:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_WORK_ORDER_MISMATCH")

    @staticmethod
    def _require_settled_outbox(snapshot: ClosedLoopAcceptanceSnapshot) -> None:
        if snapshot.outbox.get("DEAD_LETTER", 0) != 0:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_OUTBOX_DEAD_LETTER")
        if any(snapshot.outbox.get(status, 0) != 0 for status in UNSETTLED_OUTBOX_STATUSES):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")

    def _validate_alarm(self, snapshot: ClosedLoopAcceptanceSnapshot, alarm) -> None:
        acknowledged = alarm.update_fields.get("acknowledged")
        cleared = alarm.update_fields.get("cleared")
        if type(acknowledged) is not bool or type(cleared) is not bool:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_ALARM_MISMATCH")
        expected_status = (
            f"{'CLEARED' if cleared else 'ACTIVE'}_{'ACK' if acknowledged else 'UNACK'}"
        )
        if (
            alarm.alarm_id != snapshot.alarm_id
            or alarm.tenant_id != self._tb_tenant_id
            or alarm.device_id != snapshot.tb_device_id
            or alarm.alarm_type != snapshot.alarm_type
            or alarm.severity != snapshot.alarm_severity
            or alarm.status != expected_status
            or alarm.details != snapshot.alarm_details
            or cleared == snapshot.alarm_desired_active
            or alarm.details.get("alert_id") != str(snapshot.alert_id)
            or alarm.details.get("risk_key") != snapshot.risk_key
            or alarm.details.get("equipment_id") != str(snapshot.equipment_id)
            or alarm.details.get("meas_code") != snapshot.meas_code
            or alarm.details.get("maintenance_alert_version") != snapshot.alert_version
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_ALARM_MISMATCH")

    @staticmethod
    def _validate_work_order(snapshot: ClosedLoopAcceptanceSnapshot, work_order) -> None:
        if (
            not work_order_identity_matches(
                work_order,
                external_ref=snapshot.external_ref,
                request_body=snapshot.work_order_request_body,
            )
            or work_order.id != snapshot.cmms_work_order_id
            or work_order.status != snapshot.cmms_status
            or work_order.event_version != snapshot.cmms_event_version
            or work_order.asset_id != snapshot.cmms_asset_id
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_WORK_ORDER_MISMATCH")

    @staticmethod
    def _observed_stage(snapshot: ClosedLoopAcceptanceSnapshot, alarm) -> str:
        if (
            snapshot.cmms_status == "COMPLETE"
            and snapshot.alert_risk_state == "CLEARED"
            and snapshot.consecutive_healthy_count >= 2
            and alarm.update_fields["cleared"] is True
        ):
            return "CLEARED"
        if snapshot.cmms_status == "COMPLETE":
            return "COMPLETE"
        if snapshot.cmms_status == "IN_PROGRESS":
            return "IN_PROGRESS"
        if (
            snapshot.cmms_status == "OPEN"
            and snapshot.alert_risk_state == "ACTIVE"
            and alarm.update_fields["cleared"] is False
        ):
            return "ACTIVE"
        return "CONSISTENT"

    @staticmethod
    def _validate_expected_stage(
        snapshot: ClosedLoopAcceptanceSnapshot,
        alarm,
        expected_stage: str,
        observed_stage: str,
    ) -> None:
        if expected_stage == "CONSISTENT":
            return
        if expected_stage != observed_stage:
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
        if expected_stage == "ACTIVE" and (
            snapshot.alert_risk_state != "ACTIVE"
            or not snapshot.state_internal_active
            or not snapshot.alarm_desired_active
            or alarm.update_fields["cleared"] is not False
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
        if expected_stage == "IN_PROGRESS" and (
            snapshot.cmms_status != "IN_PROGRESS"
            or snapshot.work_order_maintenance_state != "WORK_ORDER_IN_PROGRESS"
            or snapshot.alert_risk_state != "ACTIVE"
            or not snapshot.state_internal_active
            or not snapshot.alarm_desired_active
            or alarm.update_fields["cleared"] is not False
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
        if expected_stage == "COMPLETE" and (
            snapshot.cmms_status != "COMPLETE"
            or snapshot.work_order_maintenance_state != "WORK_ORDER_COMPLETE"
            or snapshot.alert_risk_state != "ACTIVE"
            or not snapshot.state_internal_active
            or snapshot.consecutive_healthy_count >= 2
            or not snapshot.alarm_desired_active
            or alarm.update_fields["cleared"] is not False
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
        if expected_stage == "CLEARED" and (
            snapshot.cmms_status != "COMPLETE"
            or snapshot.work_order_maintenance_state != "WORK_ORDER_COMPLETE"
            or snapshot.alert_risk_state != "CLEARED"
            or snapshot.state_internal_active
            or snapshot.consecutive_healthy_count < 2
            or snapshot.alarm_desired_active
            or alarm.update_fields["cleared"] is not True
        ):
            raise ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")

    @staticmethod
    def _evidence(snapshot, alarm, work_order, expected_stage, observed_stage) -> dict[str, Any]:
        details_sha256 = hashlib.sha256(rfc8785.dumps(snapshot.alarm_details)).hexdigest()
        return {
            "status": "VERIFIED",
            "tenant_alias": snapshot.tenant_alias,
            "expected_stage": expected_stage,
            "observed_stage": observed_stage,
            "aggregate": {
                "alert_id": str(snapshot.alert_id),
                "equipment_id": str(snapshot.equipment_id),
                "meas_code": snapshot.meas_code,
                "risk_key": snapshot.risk_key,
                "episode": snapshot.episode,
                "risk_state": snapshot.alert_risk_state,
                "maintenance_state": snapshot.alert_maintenance_state,
                "maintenance_alert_version": snapshot.alert_version,
                "alarm_aggregate_version": snapshot.alarm_aggregate_version,
                "consecutive_healthy_count": snapshot.consecutive_healthy_count,
            },
            "outbox": {status: snapshot.outbox.get(status, 0) for status in OUTBOX_STATUSES},
            "thingsboard_alarm": {
                "id": str(alarm.alarm_id),
                "device_id": str(alarm.device_id),
                "type": alarm.alarm_type,
                "severity": alarm.severity,
                "status": alarm.status,
                "acknowledged": alarm.update_fields["acknowledged"],
                "cleared": alarm.update_fields["cleared"],
                "details_sha256": details_sha256,
                "maintenance_alert_version": snapshot.alert_version,
            },
            "cmms_work_order": {
                "id": work_order.id,
                "external_source": work_order.external_source,
                "external_ref": str(work_order.external_ref),
                "correlation_id": str(work_order.correlation_id),
                "equipment_id": str(work_order.equipment_id),
                "tb_alarm_id": str(work_order.tb_alarm_id),
                "model_profile_id": work_order.model_profile_id,
                "model_info_id": work_order.model_info_id,
                "policy_version": work_order.policy_version,
                "status": work_order.status,
                "event_version": work_order.event_version,
                "asset_id": work_order.asset_id,
            },
        }
