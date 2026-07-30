from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
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


class PredictionRun(Base):
    __tablename__ = "prediction_run"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "equipment_id",
            "meas_code",
            "scheduled_at",
            name="uq_prediction_run_slot",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', "
            "'SKIPPED_DATA_QUALITY', 'FAILED', 'FAILED_STALE')",
            name="ck_prediction_run_status",
        ),
        CheckConstraint(
            "attempt BETWEEN 0 AND 3",
            name="ck_prediction_run_attempt",
        ),
        CheckConstraint(
            "quality_summary IS NULL OR pg_column_size(quality_summary) <= 16384",
            name="ck_prediction_run_quality_summary_size",
        ),
    )

    run_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    equipment_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    meas_code: Mapped[str] = mapped_column(String(100), nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
        default=uuid4,
    )
    model_profile_id: Mapped[str | None] = mapped_column(String(160))
    model_info_id: Mapped[str | None] = mapped_column(String(160))
    preprocessing_version: Mapped[str | None] = mapped_column(String(100))
    policy_version: Mapped[str | None] = mapped_column(String(100))
    request_digest: Mapped[str | None] = mapped_column(String(64))
    input_digest: Mapped[str | None] = mapped_column(String(64))
    model_artifact_sha256: Mapped[str | None] = mapped_column(String(64))
    quality_summary: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    forecast_min: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    forecast_max: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    forecast_mean: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    threshold_crossing_count: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="PENDING",
        server_default="PENDING",
    )
    lease_owner: Mapped[str | None] = mapped_column(String(160))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RiskEvaluationState(Base):
    __tablename__ = "risk_evaluation_state"
    __table_args__ = (
        CheckConstraint(
            "consecutive_risk_count >= 0 AND consecutive_healthy_count >= 0",
            name="ck_risk_evaluation_state_counts",
        ),
        CheckConstraint(
            "alarm_aggregate_version >= 0 AND version >= 0",
            name="ck_risk_evaluation_state_versions",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    equipment_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    meas_code: Mapped[str] = mapped_column(String(100), primary_key=True)
    policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    consecutive_risk_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    consecutive_healthy_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    last_prediction_run_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    alert_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    alarm_aggregate_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
