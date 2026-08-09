from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from platform_integration.db import Base


RISK_STATES = "'ACTIVE', 'CLEARED', 'MANUALLY_CLOSED'"
MAINTENANCE_STATES = (
    "'PENDING_APPROVAL', 'APPROVED', 'WORK_ORDER_CREATING', "
    "'WORK_ORDER_OPEN', 'WORK_ORDER_IN_PROGRESS', 'WORK_ORDER_ON_HOLD', "
    "'WORK_ORDER_COMPLETE', 'WORK_ORDER_CREATE_FAILED', "
    "'SUPPRESSED_ACTIVE_WORK_ORDER'"
)


class MaintenanceAlert(Base):
    __tablename__ = "maintenance_alert"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "risk_key",
            "episode",
            name="uq_maintenance_alert_episode",
        ),
        CheckConstraint(f"risk_state IN ({RISK_STATES})", name="ck_maintenance_alert_risk_state"),
        CheckConstraint(
            f"maintenance_state IN ({MAINTENANCE_STATES})",
            name="ck_maintenance_alert_maintenance_state",
        ),
        CheckConstraint("episode > 0 AND version >= 1", name="ck_maintenance_alert_versions"),
        CheckConstraint(
            "forecast_summary IS NULL OR pg_column_size(forecast_summary) <= 4096",
            name="ck_maintenance_alert_forecast_summary_size",
        ),
        CheckConstraint(
            "risk_key ~ '^[0-9a-f]{64}$'",
            name="ck_maintenance_alert_risk_key",
        ),
    )

    alert_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    equipment_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    meas_code: Mapped[str] = mapped_column(String(100), nullable=False)
    risk_key: Mapped[str] = mapped_column(String(160), nullable=False)
    episode: Mapped[int] = mapped_column(Integer, nullable=False)
    prediction_run_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    model_profile_id: Mapped[str] = mapped_column(String(160), nullable=False)
    model_info_id: Mapped[str] = mapped_column(String(160), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    threshold_direction: Mapped[str] = mapped_column(String(8), nullable=False)
    threshold_value: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    threshold_unit: Mapped[str] = mapped_column(String(40), nullable=False)
    forecast_summary: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    risk_state: Mapped[str] = mapped_column(String(32), nullable=False)
    maintenance_state: Mapped[str] = mapped_column(String(48), nullable=False)
    work_order_action_allowed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    cleared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AlarmProjection(Base):
    __tablename__ = "alarm_projection"
    __table_args__ = (
        UniqueConstraint("tenant_id", "risk_key", name="uq_alarm_projection_risk_key"),
        CheckConstraint("aggregate_version > 0", name="ck_alarm_projection_version"),
        CheckConstraint(
            "details IS NULL OR pg_column_size(details) <= 16384",
            name="ck_alarm_projection_details_size",
        ),
        CheckConstraint(
            "risk_key ~ '^[0-9a-f]{64}$'",
            name="ck_alarm_projection_risk_key",
        ),
    )

    alert_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("maintenance_alert.alert_id", ondelete="CASCADE"),
        primary_key=True,
    )
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    risk_key: Mapped[str] = mapped_column(String(160), nullable=False)
    tb_device_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    tb_alarm_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    desired_active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    alarm_type: Mapped[str] = mapped_column(
        String(100), nullable=False, default="PDM_FORECAST_RISK"
    )
    severity: Mapped[str] = mapped_column(String(32), nullable=False, default="WARNING")
    aggregate_version: Mapped[int] = mapped_column(Integer, nullable=False)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class AlertAction(Base):
    __tablename__ = "alert_action"
    __table_args__ = (
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_alert_action_idempotency"),
        CheckConstraint(
            "action IN ('CREATE_WORK_ORDER', 'CLOSE_RISK')",
            name="ck_alert_action_action",
        ),
        CheckConstraint(
            "char_length(request_digest) = 64",
            name="ck_alert_action_request_digest",
        ),
        CheckConstraint(
            "result_summary IS NULL OR pg_column_size(result_summary) <= 16384",
            name="ck_alert_action_result_summary_size",
        ),
        CheckConstraint("expected_version >= 1", name="ck_alert_action_expected_version"),
    )

    action_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    alert_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("maintenance_alert.alert_id", ondelete="CASCADE"),
        nullable=False,
    )
    actor_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_version: Mapped[int] = mapped_column(Integer, nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    result_code: Mapped[str] = mapped_column(String(100), nullable=False)
    result_summary: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MaintenanceWorkOrder(Base):
    __tablename__ = "maintenance_work_order"
    __table_args__ = (
        UniqueConstraint("tenant_id", "external_ref", name="uq_maintenance_work_order_ref"),
        CheckConstraint(
            f"maintenance_state IN ({MAINTENANCE_STATES})",
            name="ck_maintenance_work_order_state",
        ),
        CheckConstraint(
            "cmms_event_version IS NULL OR cmms_event_version >= 0",
            name="ck_maintenance_work_order_event_version",
        ),
        CheckConstraint(
            "pg_column_size(request_body) <= 16384",
            name="ck_maintenance_work_order_request_size",
        ),
    )

    work_order_ref_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    alert_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("maintenance_alert.alert_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    external_ref: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    request_body: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    cmms_work_order_id: Mapped[int | None] = mapped_column(BigInteger)
    cmms_status: Mapped[str | None] = mapped_column(String(48))
    cmms_event_version: Mapped[int | None] = mapped_column(Integer)
    maintenance_state: Mapped[str] = mapped_column(String(48), nullable=False)
    next_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    poll_lease_owner: Mapped[str | None] = mapped_column(String(160))
    poll_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_event"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "aggregate_type",
            "aggregate_id",
            "event_type",
            "aggregate_version",
            name="uq_outbox_event_aggregate_version",
        ),
        CheckConstraint(
            "event_type IN ('TB_ALARM_SYNC', 'CMMS_WORK_ORDER_CREATE')",
            name="ck_outbox_event_type",
        ),
        CheckConstraint(
            "delivery_kind IN ('SUPERSEDABLE_STATE', 'IMMUTABLE_COMMAND')",
            name="ck_outbox_event_delivery_kind",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'IN_FLIGHT', 'RETRY_WAIT', 'DELIVERED', "
            "'SUPERSEDED', 'DEAD_LETTER')",
            name="ck_outbox_event_status",
        ),
        CheckConstraint("attempt BETWEEN 0 AND 8", name="ck_outbox_event_attempt"),
        CheckConstraint(
            "payload IS NULL OR pg_column_size(payload) <= 16384",
            name="ck_outbox_event_payload_size",
        ),
    )

    event_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    delivery_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    aggregate_version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="PENDING", server_default="PENDING"
    )
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    lease_owner: Mapped[str | None] = mapped_column(String(160))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
