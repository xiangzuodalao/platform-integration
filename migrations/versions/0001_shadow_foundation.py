"""Create the isolated shadow-prediction persistence foundation.

Revision ID: 0001_shadow_foundation
Revises:
Create Date: 2026-07-30
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0001_shadow_foundation"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tenant_binding",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alias", sa.String(length=100), nullable=False),
        sa.Column("tb_tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cmms_company_id", sa.BigInteger(), nullable=False),
        sa.Column("pdm_credential_ref", sa.String(length=128), nullable=False),
        sa.Column("tb_credential_ref", sa.String(length=128), nullable=False),
        sa.Column("cmms_credential_ref", sa.String(length=128), nullable=False),
        sa.Column("cmms_webhook_secret_ref", sa.String(length=128), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.CheckConstraint(
            "pdm_credential_ref ~ '^[A-Z][A-Z0-9_]{2,127}$'",
            name="ck_tenant_binding_pdm_credential_ref",
        ),
        sa.CheckConstraint(
            "tb_credential_ref ~ '^[A-Z][A-Z0-9_]{2,127}$'",
            name="ck_tenant_binding_tb_credential_ref",
        ),
        sa.CheckConstraint(
            "cmms_credential_ref ~ '^[A-Z][A-Z0-9_]{2,127}$'",
            name="ck_tenant_binding_cmms_credential_ref",
        ),
        sa.CheckConstraint(
            "cmms_webhook_secret_ref IS NULL OR "
            "cmms_webhook_secret_ref ~ '^[A-Z][A-Z0-9_]{2,127}$'",
            name="ck_tenant_binding_cmms_webhook_secret_ref",
        ),
        sa.PrimaryKeyConstraint("tenant_id", name="pk_tenant_binding"),
        sa.UniqueConstraint("alias", name="uq_tenant_binding_alias"),
        sa.UniqueConstraint("tb_tenant_id", name="uq_tenant_binding_tb_tenant_id"),
        sa.UniqueConstraint(
            "cmms_company_id",
            name="uq_tenant_binding_cmms_company_id",
        ),
    )

    op.create_table(
        "equipment_mapping",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("equipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tb_device_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cmms_asset_id", sa.BigInteger(), nullable=True),
        sa.Column("cmms_request_digest", sa.String(length=64), nullable=False),
        sa.Column("cmms_request_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.CheckConstraint(
            "status IN ('RESERVED', 'ACTIVE')",
            name="ck_equipment_mapping_status",
        ),
        sa.CheckConstraint(
            "cmms_request_summary IS NULL OR pg_column_size(cmms_request_summary) <= 16384",
            name="ck_equipment_mapping_summary_size",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "equipment_id",
            name="pk_equipment_mapping",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "tb_device_id",
            name="uq_equipment_mapping_tenant_tb_device",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "cmms_asset_id",
            name="uq_equipment_mapping_tenant_cmms_asset",
        ),
    )

    op.create_table(
        "measurement_binding",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("equipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("meas_code", sa.String(length=100), nullable=False),
        sa.Column("telemetry_key", sa.String(length=100), nullable=False),
        sa.Column("unit", sa.String(length=40), nullable=False),
        sa.Column("sampling_frequency", sa.String(length=40), nullable=False),
        sa.Column("request_window_points", sa.Integer(), nullable=False),
        sa.Column("context_points", sa.Integer(), nullable=False),
        sa.Column("horizon_points", sa.Integer(), nullable=False),
        sa.Column("value_scale", sa.Integer(), nullable=False),
        sa.Column("model_profile_id", sa.String(length=160), nullable=False),
        sa.Column("model_info_id", sa.String(length=160), nullable=False),
        sa.Column("preprocessing_version", sa.String(length=100), nullable=False),
        sa.Column("policy_version", sa.String(length=100), nullable=False),
        sa.Column("risk_direction", sa.String(length=8), nullable=False),
        sa.Column("risk_threshold", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.CheckConstraint(
            "request_window_points > 0 AND context_points > 0 AND horizon_points > 0",
            name="ck_measurement_binding_point_counts",
        ),
        sa.CheckConstraint(
            "value_scale BETWEEN 0 AND 6",
            name="ck_measurement_binding_value_scale",
        ),
        sa.CheckConstraint(
            "risk_direction IN ('ABOVE', 'BELOW')",
            name="ck_measurement_binding_risk_direction",
        ),
        sa.CheckConstraint(
            "risk_threshold = round(risk_threshold, value_scale)",
            name="ck_measurement_binding_threshold_scale",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "equipment_id",
            "meas_code",
            name="pk_measurement_binding",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "equipment_id",
            "telemetry_key",
            name="uq_measurement_binding_tenant_equipment_telemetry",
        ),
    )

    op.create_table(
        "provisioning_plan",
        sa.Column(
            "plan_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("plan_hash", sa.String(length=64), nullable=False),
        sa.Column("canonical_plan", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("tenant_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("tb_tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cmms_company_id", sa.BigInteger(), nullable=False),
        sa.Column("actor_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("target_results", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("terminal_result", sa.String(length=64), nullable=True),
        sa.CheckConstraint(
            "char_length(plan_hash) = 64",
            name="ck_provisioning_plan_hash_length",
        ),
        sa.CheckConstraint(
            "pg_column_size(canonical_plan) <= 262144",
            name="ck_provisioning_plan_canonical_size",
        ),
        sa.CheckConstraint(
            "pg_column_size(tenant_snapshot) <= 16384",
            name="ck_provisioning_plan_snapshot_size",
        ),
        sa.CheckConstraint(
            "target_results IS NULL OR pg_column_size(target_results) <= 131072",
            name="ck_provisioning_plan_results_size",
        ),
        sa.CheckConstraint(
            "expires_at = created_at + INTERVAL '30 minutes'",
            name="ck_provisioning_plan_expiry",
        ),
        sa.PrimaryKeyConstraint("plan_id", name="pk_provisioning_plan"),
        sa.UniqueConstraint(
            "tenant_id",
            "plan_hash",
            name="uq_provisioning_plan_tenant_hash",
        ),
    )

    op.create_table(
        "prediction_run",
        sa.Column(
            "run_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("equipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("meas_code", sa.String(length=100), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "correlation_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("model_profile_id", sa.String(length=160), nullable=True),
        sa.Column("model_info_id", sa.String(length=160), nullable=True),
        sa.Column("preprocessing_version", sa.String(length=100), nullable=True),
        sa.Column("policy_version", sa.String(length=100), nullable=True),
        sa.Column("request_digest", sa.String(length=64), nullable=True),
        sa.Column("input_digest", sa.String(length=64), nullable=True),
        sa.Column("model_artifact_sha256", sa.String(length=64), nullable=True),
        sa.Column("quality_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("forecast_min", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("forecast_max", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("forecast_mean", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("threshold_crossing_count", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.String(length=32),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column("lease_owner", sa.String(length=160), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "attempt",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', "
            "'SKIPPED_DATA_QUALITY', 'FAILED', 'FAILED_STALE')",
            name="ck_prediction_run_status",
        ),
        sa.CheckConstraint(
            "attempt BETWEEN 0 AND 3",
            name="ck_prediction_run_attempt",
        ),
        sa.CheckConstraint(
            "quality_summary IS NULL OR pg_column_size(quality_summary) <= 16384",
            name="ck_prediction_run_quality_summary_size",
        ),
        sa.PrimaryKeyConstraint("run_id", name="pk_prediction_run"),
        sa.UniqueConstraint(
            "tenant_id",
            "equipment_id",
            "meas_code",
            "scheduled_at",
            name="uq_prediction_run_slot",
        ),
    )

    op.create_table(
        "risk_evaluation_state",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("equipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("meas_code", sa.String(length=100), nullable=False),
        sa.Column("policy_version", sa.String(length=100), nullable=False),
        sa.Column(
            "consecutive_risk_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "consecutive_healthy_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_prediction_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("alert_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "alarm_aggregate_version",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "version",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "consecutive_risk_count >= 0 AND consecutive_healthy_count >= 0",
            name="ck_risk_evaluation_state_counts",
        ),
        sa.CheckConstraint(
            "alarm_aggregate_version >= 0 AND version >= 0",
            name="ck_risk_evaluation_state_versions",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "equipment_id",
            "meas_code",
            name="pk_risk_evaluation_state",
        ),
    )

    op.create_table(
        "audit_event",
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("actor_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(length=100), nullable=False),
        sa.Column("target_type", sa.String(length=100), nullable=False),
        sa.Column("target_id", sa.String(length=160), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("result_code", sa.String(length=100), nullable=False),
        sa.Column("result_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint(
            "result_summary IS NULL OR pg_column_size(result_summary) <= 16384",
            name="ck_audit_event_result_summary_size",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_audit_event"),
    )


def downgrade() -> None:
    op.drop_table("audit_event")
    op.drop_table("risk_evaluation_state")
    op.drop_table("prediction_run")
    op.drop_table("provisioning_plan")
    op.drop_table("measurement_binding")
    op.drop_table("equipment_mapping")
    op.drop_table("tenant_binding")
