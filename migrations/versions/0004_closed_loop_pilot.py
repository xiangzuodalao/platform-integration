"""Add the isolated predictive-maintenance closed-loop persistence boundary.

Revision ID: 0004_closed_loop_pilot
Revises: 0003_internal_risk_state
Create Date: 2026-08-04
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0004_closed_loop_pilot"
down_revision: str | None = "0003_internal_risk_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


RISK_STATES = "'ACTIVE', 'CLEARED', 'MANUALLY_CLOSED'"
MAINTENANCE_STATES = (
    "'PENDING_APPROVAL', 'APPROVED', 'WORK_ORDER_CREATING', "
    "'WORK_ORDER_OPEN', 'WORK_ORDER_IN_PROGRESS', 'WORK_ORDER_ON_HOLD', "
    "'WORK_ORDER_COMPLETE', 'WORK_ORDER_CREATE_FAILED', "
    "'SUPPRESSED_ACTIVE_WORK_ORDER'"
)


def _timestamps() -> tuple[sa.Column, sa.Column]:
    return (
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )


def upgrade() -> None:
    op.create_table(
        "maintenance_alert",
        sa.Column(
            "alert_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("equipment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("meas_code", sa.String(length=100), nullable=False),
        sa.Column("risk_key", sa.String(length=160), nullable=False),
        sa.Column("episode", sa.Integer(), nullable=False),
        sa.Column("prediction_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("model_profile_id", sa.String(length=160), nullable=False),
        sa.Column("model_info_id", sa.String(length=160), nullable=False),
        sa.Column("policy_version", sa.String(length=100), nullable=False),
        sa.Column("threshold_direction", sa.String(length=8), nullable=False),
        sa.Column("threshold_value", sa.Numeric(20, 6), nullable=False),
        sa.Column("threshold_unit", sa.String(length=40), nullable=False),
        sa.Column("forecast_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("risk_state", sa.String(length=32), nullable=False),
        sa.Column("maintenance_state", sa.String(length=48), nullable=False),
        sa.Column(
            "work_order_action_allowed",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        *_timestamps(),
        sa.Column("cleared_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            f"risk_state IN ({RISK_STATES})", name="ck_maintenance_alert_risk_state"
        ),
        sa.CheckConstraint(
            f"maintenance_state IN ({MAINTENANCE_STATES})",
            name="ck_maintenance_alert_maintenance_state",
        ),
        sa.CheckConstraint("episode > 0 AND version >= 1", name="ck_maintenance_alert_versions"),
        sa.CheckConstraint(
            "forecast_summary IS NULL OR pg_column_size(forecast_summary) <= 4096",
            name="ck_maintenance_alert_forecast_summary_size",
        ),
        sa.CheckConstraint(
            "risk_key ~ '^[0-9a-f]{64}$'",
            name="ck_maintenance_alert_risk_key",
        ),
        sa.PrimaryKeyConstraint("alert_id", name="pk_maintenance_alert"),
        sa.UniqueConstraint(
            "tenant_id", "risk_key", "episode", name="uq_maintenance_alert_episode"
        ),
    )
    op.create_index("ix_maintenance_alert_tenant_id", "maintenance_alert", ["tenant_id"])

    op.create_table(
        "alarm_projection",
        sa.Column("alert_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("risk_key", sa.String(length=160), nullable=False),
        sa.Column("tb_device_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tb_alarm_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("desired_active", sa.Boolean(), nullable=False),
        sa.Column(
            "alarm_type",
            sa.String(length=100),
            server_default=sa.text("'PDM_FORECAST_RISK'"),
            nullable=False,
        ),
        sa.Column(
            "severity",
            sa.String(length=32),
            server_default=sa.text("'WARNING'"),
            nullable=False,
        ),
        sa.Column("aggregate_version", sa.Integer(), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("aggregate_version > 0", name="ck_alarm_projection_version"),
        sa.CheckConstraint(
            "details IS NULL OR pg_column_size(details) <= 16384",
            name="ck_alarm_projection_details_size",
        ),
        sa.CheckConstraint(
            "risk_key ~ '^[0-9a-f]{64}$'",
            name="ck_alarm_projection_risk_key",
        ),
        sa.ForeignKeyConstraint(["alert_id"], ["maintenance_alert.alert_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("alert_id", name="pk_alarm_projection"),
        sa.UniqueConstraint("tenant_id", "risk_key", name="uq_alarm_projection_risk_key"),
    )
    op.create_index("ix_alarm_projection_tenant_id", "alarm_projection", ["tenant_id"])

    op.create_table(
        "alert_action",
        sa.Column(
            "action_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alert_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("actor_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("idempotency_key", sa.String(length=200), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("expected_version", sa.Integer(), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("result_code", sa.String(length=100), nullable=False),
        sa.Column("result_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action IN ('CREATE_WORK_ORDER', 'CLOSE_RISK')", name="ck_alert_action_action"
        ),
        sa.CheckConstraint(
            "char_length(request_digest) = 64", name="ck_alert_action_request_digest"
        ),
        sa.CheckConstraint(
            "result_summary IS NULL OR pg_column_size(result_summary) <= 16384",
            name="ck_alert_action_result_summary_size",
        ),
        sa.CheckConstraint("expected_version >= 1", name="ck_alert_action_expected_version"),
        sa.ForeignKeyConstraint(["alert_id"], ["maintenance_alert.alert_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("action_id", name="pk_alert_action"),
        sa.UniqueConstraint("tenant_id", "idempotency_key", name="uq_alert_action_idempotency"),
    )
    op.create_index("ix_alert_action_tenant_id", "alert_action", ["tenant_id"])

    op.create_table(
        "maintenance_work_order",
        sa.Column(
            "work_order_ref_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alert_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("external_ref", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("request_body", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("cmms_work_order_id", sa.BigInteger(), nullable=True),
        sa.Column("cmms_status", sa.String(length=48), nullable=True),
        sa.Column("cmms_event_version", sa.Integer(), nullable=True),
        sa.Column("maintenance_state", sa.String(length=48), nullable=False),
        sa.Column("next_poll_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("poll_lease_owner", sa.String(length=160), nullable=True),
        sa.Column("poll_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status_updated_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            f"maintenance_state IN ({MAINTENANCE_STATES})",
            name="ck_maintenance_work_order_state",
        ),
        sa.CheckConstraint(
            "cmms_event_version IS NULL OR cmms_event_version >= 0",
            name="ck_maintenance_work_order_event_version",
        ),
        sa.CheckConstraint(
            "pg_column_size(request_body) <= 16384",
            name="ck_maintenance_work_order_request_size",
        ),
        sa.ForeignKeyConstraint(["alert_id"], ["maintenance_alert.alert_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("work_order_ref_id", name="pk_maintenance_work_order"),
        sa.UniqueConstraint("alert_id", name="uq_maintenance_work_order_alert_id"),
        sa.UniqueConstraint("tenant_id", "external_ref", name="uq_maintenance_work_order_ref"),
    )
    op.create_index("ix_maintenance_work_order_tenant_id", "maintenance_work_order", ["tenant_id"])

    op.create_table(
        "outbox_event",
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("delivery_kind", sa.String(length=32), nullable=False),
        sa.Column("aggregate_version", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=24),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("attempt", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("lease_owner", sa.String(length=160), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "event_type IN ('TB_ALARM_SYNC', 'CMMS_WORK_ORDER_CREATE')",
            name="ck_outbox_event_type",
        ),
        sa.CheckConstraint(
            "delivery_kind IN ('SUPERSEDABLE_STATE', 'IMMUTABLE_COMMAND')",
            name="ck_outbox_event_delivery_kind",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'IN_FLIGHT', 'RETRY_WAIT', 'DELIVERED', "
            "'SUPERSEDED', 'DEAD_LETTER')",
            name="ck_outbox_event_status",
        ),
        sa.CheckConstraint("attempt BETWEEN 0 AND 8", name="ck_outbox_event_attempt"),
        sa.CheckConstraint(
            "payload IS NULL OR pg_column_size(payload) <= 16384",
            name="ck_outbox_event_payload_size",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_outbox_event"),
        sa.UniqueConstraint(
            "tenant_id",
            "aggregate_type",
            "aggregate_id",
            "event_type",
            "aggregate_version",
            name="uq_outbox_event_aggregate_version",
        ),
    )
    op.create_index("ix_outbox_event_tenant_id", "outbox_event", ["tenant_id"])
    op.create_index("ix_outbox_event_event_type", "outbox_event", ["event_type"])
    op.create_index(
        "ix_outbox_event_claim",
        "outbox_event",
        ["event_type", "status", "available_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_event_claim", table_name="outbox_event")
    op.drop_index("ix_outbox_event_event_type", table_name="outbox_event")
    op.drop_index("ix_outbox_event_tenant_id", table_name="outbox_event")
    op.drop_table("outbox_event")
    op.drop_index("ix_maintenance_work_order_tenant_id", table_name="maintenance_work_order")
    op.drop_table("maintenance_work_order")
    op.drop_index("ix_alert_action_tenant_id", table_name="alert_action")
    op.drop_table("alert_action")
    op.drop_index("ix_alarm_projection_tenant_id", table_name="alarm_projection")
    op.drop_table("alarm_projection")
    op.drop_index("ix_maintenance_alert_tenant_id", table_name="maintenance_alert")
    op.drop_table("maintenance_alert")
