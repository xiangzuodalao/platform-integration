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


REFERENCE_CHECK = r"^[A-Z][A-Z0-9_]{2,127}$"


class TenantBinding(Base):
    __tablename__ = "tenant_binding"
    __table_args__ = (
        UniqueConstraint("alias", name="uq_tenant_binding_alias"),
        UniqueConstraint("tb_tenant_id", name="uq_tenant_binding_tb_tenant_id"),
        UniqueConstraint("cmms_company_id", name="uq_tenant_binding_cmms_company_id"),
        CheckConstraint(
            f"pdm_credential_ref ~ '{REFERENCE_CHECK}'",
            name="ck_tenant_binding_pdm_credential_ref",
        ),
        CheckConstraint(
            f"tb_credential_ref ~ '{REFERENCE_CHECK}'",
            name="ck_tenant_binding_tb_credential_ref",
        ),
        CheckConstraint(
            f"cmms_credential_ref ~ '{REFERENCE_CHECK}'",
            name="ck_tenant_binding_cmms_credential_ref",
        ),
        CheckConstraint(
            f"cmms_webhook_secret_ref IS NULL OR cmms_webhook_secret_ref ~ '{REFERENCE_CHECK}'",
            name="ck_tenant_binding_cmms_webhook_secret_ref",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    alias: Mapped[str] = mapped_column(String(100), nullable=False)
    tb_tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    cmms_company_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pdm_credential_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    tb_credential_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    cmms_credential_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    cmms_webhook_secret_ref: Mapped[str | None] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)


class EquipmentMapping(Base):
    __tablename__ = "equipment_mapping"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "tb_device_id",
            name="uq_equipment_mapping_tenant_tb_device",
        ),
        UniqueConstraint(
            "tenant_id",
            "cmms_asset_id",
            name="uq_equipment_mapping_tenant_cmms_asset",
        ),
        CheckConstraint(
            "status IN ('RESERVED', 'ACTIVE')",
            name="ck_equipment_mapping_status",
        ),
        CheckConstraint(
            "cmms_request_summary IS NULL OR pg_column_size(cmms_request_summary) <= 16384",
            name="ck_equipment_mapping_summary_size",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
    )
    equipment_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
    )
    tb_device_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    cmms_asset_id: Mapped[int | None] = mapped_column(BigInteger)
    cmms_request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    cmms_request_summary: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)


class MeasurementBinding(Base):
    __tablename__ = "measurement_binding"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "equipment_id",
            "telemetry_key",
            name="uq_measurement_binding_tenant_equipment_telemetry",
        ),
        CheckConstraint(
            "request_window_points > 0 AND context_points > 0 AND horizon_points > 0",
            name="ck_measurement_binding_point_counts",
        ),
        CheckConstraint(
            "value_scale BETWEEN 0 AND 6",
            name="ck_measurement_binding_value_scale",
        ),
        CheckConstraint(
            "risk_direction IN ('ABOVE', 'BELOW')",
            name="ck_measurement_binding_risk_direction",
        ),
        CheckConstraint(
            "risk_threshold = round(risk_threshold, value_scale)",
            name="ck_measurement_binding_threshold_scale",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    equipment_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    meas_code: Mapped[str] = mapped_column(String(100), primary_key=True)
    telemetry_key: Mapped[str] = mapped_column(String(100), nullable=False)
    unit: Mapped[str] = mapped_column(String(40), nullable=False)
    sampling_frequency: Mapped[str] = mapped_column(String(40), nullable=False)
    request_window_points: Mapped[int] = mapped_column(Integer, nullable=False)
    context_points: Mapped[int] = mapped_column(Integer, nullable=False)
    horizon_points: Mapped[int] = mapped_column(Integer, nullable=False)
    value_scale: Mapped[int] = mapped_column(Integer, nullable=False)
    model_profile_id: Mapped[str] = mapped_column(String(160), nullable=False)
    model_info_id: Mapped[str] = mapped_column(String(160), nullable=False)
    preprocessing_version: Mapped[str] = mapped_column(String(100), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    risk_direction: Mapped[str] = mapped_column(String(8), nullable=False)
    risk_threshold: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)


class ProvisioningPlan(Base):
    __tablename__ = "provisioning_plan"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "plan_hash",
            name="uq_provisioning_plan_tenant_hash",
        ),
        CheckConstraint(
            "char_length(plan_hash) = 64",
            name="ck_provisioning_plan_hash_length",
        ),
        CheckConstraint(
            "pg_column_size(canonical_plan) <= 262144",
            name="ck_provisioning_plan_canonical_size",
        ),
        CheckConstraint(
            "pg_column_size(tenant_snapshot) <= 16384",
            name="ck_provisioning_plan_snapshot_size",
        ),
        CheckConstraint(
            "target_results IS NULL OR pg_column_size(target_results) <= 131072",
            name="ck_provisioning_plan_results_size",
        ),
        CheckConstraint(
            "expires_at = created_at + INTERVAL '30 minutes'",
            name="ck_provisioning_plan_expiry",
        ),
    )

    plan_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_plan: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    tenant_snapshot: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    tb_tenant_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    cmms_company_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actor_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    target_results: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    terminal_result: Mapped[str | None] = mapped_column(String(64))
