from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select, update

from platform_integration.clients.cmms import CmmsClientError
from platform_integration.clients.thingsboard import ThingsBoardClientError
from platform_integration.models.audit import AuditEvent
from platform_integration.models.bindings import TenantBinding
from platform_integration.models.closed_loop import (
    AlarmProjection,
    MaintenanceAlert,
    MaintenanceWorkOrder,
    OutboxEvent,
)
from platform_integration.models.prediction import RiskEvaluationState
from platform_integration.services.closed_loop import (
    ClosedLoopTransitionService,
    SYSTEM_ACTOR_ID,
)


LEASE_SECONDS = 30
MAX_ATTEMPTS = 8


@dataclass(frozen=True)
class OutboxClaim:
    event_id: UUID
    tenant_id: UUID
    aggregate_id: UUID
    event_type: str
    aggregate_version: int
    attempt: int
    owner: str


@dataclass(frozen=True)
class AlarmDelivery:
    claim: OutboxClaim
    alert_id: UUID
    risk_key: str
    tb_tenant_id: UUID
    device_id: UUID
    alarm_id: UUID | None
    desired_active: bool
    details: dict[str, Any]


@dataclass(frozen=True)
class WorkOrderDelivery:
    claim: OutboxClaim
    alert_id: UUID
    credential_ref: str
    cmms_company_id: int
    request_body: dict[str, Any]


@dataclass(frozen=True)
class StatusPollClaim:
    work_order_ref_id: UUID
    tenant_id: UUID
    alert_id: UUID
    external_ref: UUID
    credential_ref: str
    cmms_company_id: int
    request_body: dict[str, Any]
    owner: str


class ClosedLoopDeliveryStore:
    def __init__(self, *, sessions, tenant_id: UUID, feedback_poll_seconds: int = 300) -> None:
        self._sessions = sessions
        self._tenant_id = tenant_id
        self._poll_interval = timedelta(seconds=feedback_poll_seconds)

    async def claim_outbox(
        self,
        event_type: str,
        owner: str,
        now: datetime,
    ) -> OutboxClaim | None:
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(OutboxEvent)
                .where(
                    OutboxEvent.tenant_id == self._tenant_id,
                    OutboxEvent.event_type == event_type,
                    OutboxEvent.status == "IN_FLIGHT",
                    OutboxEvent.lease_expires_at < now,
                )
                .values(
                    status="RETRY_WAIT",
                    lease_owner=None,
                    lease_expires_at=None,
                    available_at=now,
                    last_error_code="DELIVERY_LEASE_EXPIRED",
                )
            )
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.tenant_id == self._tenant_id,
                    OutboxEvent.event_type == event_type,
                    OutboxEvent.status.in_({"PENDING", "RETRY_WAIT"}),
                    OutboxEvent.available_at <= now,
                    OutboxEvent.attempt < MAX_ATTEMPTS,
                )
                .order_by(OutboxEvent.available_at, OutboxEvent.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return None
            event.status = "IN_FLIGHT"
            event.attempt += 1
            event.lease_owner = owner
            event.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
            return OutboxClaim(
                event_id=event.event_id,
                tenant_id=event.tenant_id,
                aggregate_id=event.aggregate_id,
                event_type=event.event_type,
                aggregate_version=event.aggregate_version,
                attempt=event.attempt,
                owner=owner,
            )

    async def alarm_delivery(self, claim: OutboxClaim) -> AlarmDelivery | None:
        async with self._sessions() as session:
            event = await session.get(OutboxEvent, claim.event_id)
            projection = await session.get(AlarmProjection, claim.aggregate_id)
            tenant = await session.get(TenantBinding, claim.tenant_id)
            if (
                event is None
                or event.status != "IN_FLIGHT"
                or event.lease_owner != claim.owner
                or projection is None
                or tenant is None
                or not tenant.enabled
            ):
                return None
            return AlarmDelivery(
                claim=claim,
                alert_id=projection.alert_id,
                risk_key=projection.risk_key,
                tb_tenant_id=tenant.tb_tenant_id,
                device_id=projection.tb_device_id,
                alarm_id=projection.tb_alarm_id,
                desired_active=projection.desired_active,
                details=projection.details or {},
            )

    async def work_order_delivery(self, claim: OutboxClaim) -> WorkOrderDelivery | None:
        async with self._sessions() as session:
            event = await session.get(OutboxEvent, claim.event_id)
            work_order = await session.scalar(
                select(MaintenanceWorkOrder).where(
                    MaintenanceWorkOrder.tenant_id == claim.tenant_id,
                    MaintenanceWorkOrder.alert_id == claim.aggregate_id,
                )
            )
            tenant = await session.get(TenantBinding, claim.tenant_id)
            if (
                event is None
                or event.status != "IN_FLIGHT"
                or event.lease_owner != claim.owner
                or work_order is None
                or tenant is None
                or not tenant.enabled
            ):
                return None
            return WorkOrderDelivery(
                claim=claim,
                alert_id=work_order.alert_id,
                credential_ref=tenant.cmms_credential_ref,
                cmms_company_id=tenant.cmms_company_id,
                request_body=work_order.request_body,
            )

    async def supersede(self, claim: OutboxClaim) -> None:
        await self._finish_event(claim, "SUPERSEDED", None)

    async def delivered(self, claim: OutboxClaim, now: datetime) -> None:
        await self._finish_event(claim, "DELIVERED", now)

    async def retry(self, claim: OutboxClaim, code: str, now: datetime) -> None:
        async with self._sessions() as session, session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.event_id == claim.event_id,
                    OutboxEvent.status == "IN_FLIGHT",
                    OutboxEvent.lease_owner == claim.owner,
                )
                .with_for_update()
            )
            if event is None:
                return
            event.last_error_code = code
            event.lease_owner = None
            event.lease_expires_at = None
            if event.attempt >= MAX_ATTEMPTS:
                event.status = "DEAD_LETTER"
                await self._audit_delivery(session, event, code, now)
            else:
                event.status = "RETRY_WAIT"
                delay = min(300, 2 ** min(event.attempt, 8))
                event.available_at = now + timedelta(seconds=delay)

    async def dead_letter(self, claim: OutboxClaim, code: str, now: datetime) -> None:
        async with self._sessions() as session, session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.event_id == claim.event_id,
                    OutboxEvent.status == "IN_FLIGHT",
                    OutboxEvent.lease_owner == claim.owner,
                )
                .with_for_update()
            )
            if event is None:
                return
            event.status = "DEAD_LETTER"
            event.last_error_code = code
            event.lease_owner = None
            event.lease_expires_at = None
            await self._audit_delivery(session, event, code, now)

    async def _finish_event(
        self,
        claim: OutboxClaim,
        status: str,
        delivered_at: datetime | None,
    ) -> None:
        async with self._sessions() as session, session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.event_id == claim.event_id,
                    OutboxEvent.status == "IN_FLIGHT",
                    OutboxEvent.lease_owner == claim.owner,
                )
                .with_for_update()
            )
            if event is None:
                return
            event.status = status
            event.delivered_at = delivered_at
            event.lease_owner = None
            event.lease_expires_at = None
            event.last_error_code = None

    async def save_alarm_id(
        self,
        delivery: AlarmDelivery,
        alarm_id: UUID,
        now: datetime,
    ) -> None:
        async with self._sessions() as session, session.begin():
            projection = await session.get(AlarmProjection, delivery.alert_id, with_for_update=True)
            event = await session.get(OutboxEvent, delivery.claim.event_id, with_for_update=True)
            if (
                projection is None
                or event is None
                or event.status != "IN_FLIGHT"
                or event.lease_owner != delivery.claim.owner
            ):
                return
            if projection.tb_alarm_id is not None and projection.tb_alarm_id != alarm_id:
                raise RuntimeError("THINGSBOARD_ALARM_IDENTITY_CONFLICT")
            projection.tb_alarm_id = alarm_id
            projection.updated_at = now
            event.status = "DELIVERED"
            event.delivered_at = now
            event.lease_owner = None
            event.lease_expires_at = None

    async def save_created_work_order(self, delivery: WorkOrderDelivery, response, now) -> None:
        async with self._sessions() as session, session.begin():
            snapshot = await session.get(MaintenanceAlert, delivery.alert_id)
            if snapshot is None:
                return
            state = await session.scalar(
                select(RiskEvaluationState)
                .where(
                    RiskEvaluationState.tenant_id == snapshot.tenant_id,
                    RiskEvaluationState.equipment_id == snapshot.equipment_id,
                    RiskEvaluationState.meas_code == snapshot.meas_code,
                )
                .with_for_update()
            )
            if state is None:
                return
            work_order = await session.scalar(
                select(MaintenanceWorkOrder)
                .where(MaintenanceWorkOrder.alert_id == delivery.alert_id)
                .with_for_update()
            )
            event = await session.get(OutboxEvent, delivery.claim.event_id, with_for_update=True)
            alert = await session.get(MaintenanceAlert, delivery.alert_id, with_for_update=True)
            if work_order is None or event is None or alert is None:
                return
            if event.status != "IN_FLIGHT" or event.lease_owner != delivery.claim.owner:
                return
            work_order.cmms_work_order_id = response.id
            work_order.cmms_status = response.status
            work_order.cmms_event_version = response.event_version
            work_order.status_updated_at = response.updated_at
            work_order.maintenance_state = self._maintenance_state(response.status)
            work_order.next_poll_at = (
                None
                if work_order.maintenance_state == "WORK_ORDER_COMPLETE"
                else now + self._poll_interval
            )
            work_order.updated_at = now
            alert.maintenance_state = work_order.maintenance_state
            alert.version += 1
            alert.updated_at = now
            event.status = "DELIVERED"
            event.delivered_at = now
            event.lease_owner = None
            event.lease_expires_at = None
            await self._queue_alarm_details(session, alert, now)

    async def claim_status_poll(self, owner: str, now: datetime) -> StatusPollClaim | None:
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(MaintenanceWorkOrder)
                .where(
                    MaintenanceWorkOrder.tenant_id == self._tenant_id,
                    MaintenanceWorkOrder.poll_lease_expires_at < now,
                )
                .values(poll_lease_owner=None, poll_lease_expires_at=None)
            )
            row = await session.scalar(
                select(MaintenanceWorkOrder)
                .where(
                    MaintenanceWorkOrder.tenant_id == self._tenant_id,
                    MaintenanceWorkOrder.cmms_work_order_id.is_not(None),
                    MaintenanceWorkOrder.maintenance_state != "WORK_ORDER_COMPLETE",
                    MaintenanceWorkOrder.next_poll_at <= now,
                    MaintenanceWorkOrder.poll_lease_owner.is_(None),
                )
                .order_by(MaintenanceWorkOrder.next_poll_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if row is None:
                return None
            tenant = await session.get(TenantBinding, row.tenant_id)
            if tenant is None or not tenant.enabled:
                return None
            row.poll_lease_owner = owner
            row.poll_lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
            return StatusPollClaim(
                work_order_ref_id=row.work_order_ref_id,
                tenant_id=row.tenant_id,
                alert_id=row.alert_id,
                external_ref=row.external_ref,
                credential_ref=tenant.cmms_credential_ref,
                cmms_company_id=tenant.cmms_company_id,
                request_body=row.request_body,
                owner=owner,
            )

    async def apply_status(self, claim: StatusPollClaim, response, now: datetime) -> str:
        async with self._sessions() as session, session.begin():
            snapshot = await session.get(MaintenanceAlert, claim.alert_id)
            if snapshot is None:
                return "LEASE_LOST"
            state = await session.scalar(
                select(RiskEvaluationState)
                .where(
                    RiskEvaluationState.tenant_id == snapshot.tenant_id,
                    RiskEvaluationState.equipment_id == snapshot.equipment_id,
                    RiskEvaluationState.meas_code == snapshot.meas_code,
                )
                .with_for_update()
            )
            if state is None:
                return "LEASE_LOST"
            row = await session.scalar(
                select(MaintenanceWorkOrder)
                .where(
                    MaintenanceWorkOrder.work_order_ref_id == claim.work_order_ref_id,
                    MaintenanceWorkOrder.poll_lease_owner == claim.owner,
                )
                .with_for_update()
            )
            alert = await session.get(MaintenanceAlert, claim.alert_id, with_for_update=True)
            if row is None or alert is None:
                return "LEASE_LOST"
            current_version = row.cmms_event_version
            if current_version is not None and response.event_version < current_version:
                result = "STALE_IGNORED"
            elif current_version == response.event_version:
                if row.cmms_status != response.status:
                    result = "VERSION_CONFLICT"
                    await self._audit_status(session, row, result, now)
                else:
                    result = "DUPLICATE"
            else:
                row.cmms_event_version = response.event_version
                row.cmms_status = response.status
                row.status_updated_at = response.updated_at
                row.maintenance_state = self._maintenance_state(response.status)
                row.updated_at = now
                alert.maintenance_state = row.maintenance_state
                alert.version += 1
                alert.updated_at = now
                await self._queue_alarm_details(session, alert, now)
                result = "APPLIED"
            row.next_poll_at = (
                None
                if row.maintenance_state == "WORK_ORDER_COMPLETE"
                else now + self._poll_interval
            )
            row.poll_lease_owner = None
            row.poll_lease_expires_at = None
            return result

    async def fail_status_poll(self, claim: StatusPollClaim, code: str, now: datetime) -> None:
        async with self._sessions() as session, session.begin():
            row = await session.scalar(
                select(MaintenanceWorkOrder)
                .where(
                    MaintenanceWorkOrder.work_order_ref_id == claim.work_order_ref_id,
                    MaintenanceWorkOrder.poll_lease_owner == claim.owner,
                )
                .with_for_update()
            )
            if row is None:
                return
            row.next_poll_at = now + self._poll_interval
            row.poll_lease_owner = None
            row.poll_lease_expires_at = None
            await self._audit_status(session, row, code, now)

    async def summary(self, tenant_alias: str) -> dict[str, Any]:
        async with self._sessions() as session:
            tenant = await session.scalar(
                select(TenantBinding).where(
                    TenantBinding.tenant_id == self._tenant_id,
                    TenantBinding.alias == tenant_alias,
                )
            )
            if tenant is None:
                raise RuntimeError("TENANT_BINDING_NOT_FOUND")
            alert_rows = (
                await session.execute(
                    select(MaintenanceAlert.risk_state, func.count())
                    .where(MaintenanceAlert.tenant_id == self._tenant_id)
                    .group_by(MaintenanceAlert.risk_state)
                )
            ).all()
            work_order_rows = (
                await session.execute(
                    select(MaintenanceWorkOrder.maintenance_state, func.count())
                    .where(MaintenanceWorkOrder.tenant_id == self._tenant_id)
                    .group_by(MaintenanceWorkOrder.maintenance_state)
                )
            ).all()
            outbox_rows = (
                await session.execute(
                    select(OutboxEvent.status, func.count())
                    .where(OutboxEvent.tenant_id == self._tenant_id)
                    .group_by(OutboxEvent.status)
                )
            ).all()
            return {
                "tenant_alias": tenant.alias,
                "tenant_id": str(tenant.tenant_id),
                "alerts": {key: value for key, value in sorted(alert_rows)},
                "work_orders": {key: value for key, value in sorted(work_order_rows)},
                "outbox": {key: value for key, value in sorted(outbox_rows)},
            }

    async def _queue_alarm_details(self, session, alert: MaintenanceAlert, now: datetime) -> None:
        state = await session.scalar(
            select(RiskEvaluationState)
            .where(
                RiskEvaluationState.tenant_id == alert.tenant_id,
                RiskEvaluationState.equipment_id == alert.equipment_id,
                RiskEvaluationState.meas_code == alert.meas_code,
            )
            .with_for_update()
        )
        if state is None:
            raise RuntimeError("RISK_STATE_MISSING")
        projection = await session.get(AlarmProjection, alert.alert_id, with_for_update=True)
        if projection is None:
            return
        state.alarm_aggregate_version += 1
        projection.aggregate_version = state.alarm_aggregate_version
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
                aggregate_version=projection.aggregate_version,
                status="PENDING",
                available_at=now,
            )
        )

    @staticmethod
    def _maintenance_state(status: str) -> str:
        mapping = {
            "OPEN": "WORK_ORDER_OPEN",
            "IN_PROGRESS": "WORK_ORDER_IN_PROGRESS",
            "ON_HOLD": "WORK_ORDER_ON_HOLD",
            "COMPLETE": "WORK_ORDER_COMPLETE",
        }
        try:
            return mapping[status]
        except KeyError:
            raise RuntimeError("CMMS_WORK_ORDER_STATUS_INVALID") from None

    @staticmethod
    async def _audit_delivery(session, event: OutboxEvent, code: str, now: datetime) -> None:
        session.add(
            AuditEvent(
                event_id=uuid4(),
                tenant_id=event.tenant_id,
                actor_id=SYSTEM_ACTOR_ID,
                action="DELIVERY_DEAD_LETTER",
                target_type="OUTBOX_EVENT",
                target_id=str(event.event_id),
                correlation_id=event.event_id,
                occurred_at=now,
                result_code=code,
                result_summary={"event_type": event.event_type, "attempt": event.attempt},
            )
        )

    @staticmethod
    async def _audit_status(session, row: MaintenanceWorkOrder, code: str, now: datetime) -> None:
        session.add(
            AuditEvent(
                event_id=uuid4(),
                tenant_id=row.tenant_id,
                actor_id=SYSTEM_ACTOR_ID,
                action="CMMS_STATUS_POLL",
                target_type="MAINTENANCE_WORK_ORDER",
                target_id=str(row.external_ref),
                correlation_id=row.correlation_id,
                occurred_at=now,
                result_code=code,
                result_summary={
                    "cmms_event_version": row.cmms_event_version,
                    "cmms_status": row.cmms_status,
                },
            )
        )


class AlarmDeliveryWorker:
    def __init__(self, *, store: ClosedLoopDeliveryStore, thingsboard, owner: str) -> None:
        self._store = store
        self._thingsboard = thingsboard
        self._owner = owner

    async def run_once(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        claim = await self._store.claim_outbox("TB_ALARM_SYNC", self._owner, now)
        if claim is None:
            return False
        delivery = await self._store.alarm_delivery(claim)
        if delivery is None:
            await self._store.supersede(claim)
            return True
        equipment_id = delivery.details.get("equipment_id")
        meas_code = delivery.details.get("meas_code")
        if type(equipment_id) is not str or type(meas_code) is not str:
            await self._store.dead_letter(claim, "ALARM_PROJECTION_IDENTITY_INVALID", now)
            return True
        try:
            authenticated_tenant_id = await self._thingsboard.authenticated_tenant_id()
            if authenticated_tenant_id != delivery.tb_tenant_id:
                await self._store.retry(claim, "THINGSBOARD_TENANT_ID_MISMATCH", now)
                return True
            existing = await self._thingsboard.find_pdm_alarm(
                delivery.device_id,
                risk_key=delivery.risk_key,
                alert_id=delivery.alert_id,
                equipment_id=equipment_id,
                meas_code=meas_code,
            )
            if existing is not None and (
                existing.details.get("alert_id") != str(delivery.alert_id)
                or existing.details.get("equipment_id") != delivery.details.get("equipment_id")
                or existing.details.get("meas_code") != delivery.details.get("meas_code")
            ):
                await self._store.dead_letter(claim, "THINGSBOARD_ALARM_IDENTITY_MISMATCH", now)
                return True
            alarm_id = existing.alarm_id if existing is not None else delivery.alarm_id
            if delivery.desired_active:
                alarm_id = await self._thingsboard.upsert_pdm_alarm(
                    device_id=delivery.device_id,
                    details=delivery.details,
                    existing_alarm=existing,
                )
                await self._store.save_alarm_id(delivery, alarm_id, now)
            else:
                if alarm_id is not None:
                    await self._thingsboard.clear_alarm(alarm_id)
                await self._store.delivered(claim, now)
        except ThingsBoardClientError as exc:
            await self._store.retry(claim, exc.code, now)
        return True


class WorkOrderDeliveryWorker:
    def __init__(self, *, store: ClosedLoopDeliveryStore, cmms_factory, owner: str) -> None:
        self._store = store
        self._cmms_factory = cmms_factory
        self._owner = owner

    async def run_once(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        claim = await self._store.claim_outbox("CMMS_WORK_ORDER_CREATE", self._owner, now)
        if claim is None:
            return False
        delivery = await self._store.work_order_delivery(claim)
        if delivery is None:
            await self._store.retry(claim, "CMMS_WORK_ORDER_INTENT_MISSING", now)
            return True
        client = self._cmms_factory(delivery.credential_ref)
        try:
            company_id = await client.authenticated_company_id()
            if company_id != delivery.cmms_company_id:
                await self._store.dead_letter(claim, "CMMS_COMPANY_IDENTITY_MISMATCH", now)
                return True
            response = await client.find_work_order_by_external_ref(delivery.alert_id)
            if response is None:
                response = await client.create_work_order(
                    delivery.request_body,
                    idempotency_key=f"wo:{delivery.alert_id}",
                )
            if not work_order_identity_matches(
                response,
                external_ref=delivery.alert_id,
                request_body=delivery.request_body,
            ):
                await self._store.dead_letter(claim, "CMMS_WORK_ORDER_IDENTITY_MISMATCH", now)
                return True
            await self._store.save_created_work_order(delivery, response, now)
        except CmmsClientError as exc:
            await self._store.retry(claim, exc.code, now)
        except RuntimeError:
            await self._store.dead_letter(claim, "CMMS_WORK_ORDER_STATUS_INVALID", now)
        return True


class StatusPollWorker:
    def __init__(self, *, store: ClosedLoopDeliveryStore, cmms_factory, owner: str) -> None:
        self._store = store
        self._cmms_factory = cmms_factory
        self._owner = owner

    async def run_once(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        claim = await self._store.claim_status_poll(self._owner, now)
        if claim is None:
            return False
        client = self._cmms_factory(claim.credential_ref)
        response = None
        error_code = "CMMS_UNAVAILABLE"
        try:
            company_id = await client.authenticated_company_id()
        except CmmsClientError as exc:
            await self._store.fail_status_poll(claim, exc.code, now)
            return True
        if company_id != claim.cmms_company_id:
            await self._store.fail_status_poll(claim, "CMMS_COMPANY_IDENTITY_MISMATCH", now)
            return True
        for _ in range(3):
            try:
                response = await client.find_work_order_by_external_ref(claim.external_ref)
                if response is None:
                    error_code = "CMMS_WORK_ORDER_NOT_FOUND"
                break
            except CmmsClientError as exc:
                error_code = exc.code
        if response is None:
            await self._store.fail_status_poll(claim, error_code, now)
            return True
        if not work_order_identity_matches(
            response,
            external_ref=claim.external_ref,
            request_body=claim.request_body,
        ):
            await self._store.fail_status_poll(claim, "CMMS_WORK_ORDER_IDENTITY_MISMATCH", now)
            return True
        try:
            await self._store.apply_status(claim, response, now)
        except RuntimeError:
            await self._store.fail_status_poll(claim, "CMMS_WORK_ORDER_STATUS_INVALID", now)
        return True


def work_order_identity_matches(
    response,
    *,
    external_ref: UUID,
    request_body: dict[str, Any],
) -> bool:
    asset = request_body.get("asset")
    return (
        str(response.external_ref) == str(external_ref)
        and response.external_source == "PDM_FORECAST"
        and str(response.correlation_id) == request_body.get("correlation_id")
        and str(response.equipment_id) == request_body.get("equipment_id")
        and str(response.tb_alarm_id) == request_body.get("tb_alarm_id")
        and response.model_profile_id == request_body.get("model_profile_id")
        and response.model_info_id == request_body.get("model_info_id")
        and response.policy_version == request_body.get("policy_version")
        and isinstance(asset, dict)
        and response.asset_id == asset.get("id")
    )
