"""Persist the actor that confirms and applies a provisioning plan.

Revision ID: 0002_provisioning_apply_actor
Revises: 0001_shadow_foundation
Create Date: 2026-07-30
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0002_provisioning_apply_actor"
down_revision: str | None = "0001_shadow_foundation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "provisioning_plan",
        sa.Column(
            "apply_actor_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE provisioning_plan AS plan
            SET apply_actor_id = (
                SELECT audit.actor_id
                FROM audit_event AS audit
                WHERE audit.tenant_id = plan.tenant_id
                  AND audit.action = 'PILOT_PROVISIONING_COMPLETED'
                  AND audit.target_type = 'PROVISIONING_PLAN'
                  AND audit.target_id = plan.plan_hash
                ORDER BY audit.occurred_at DESC, audit.event_id DESC
                LIMIT 1
            )
            WHERE plan.terminal_result = 'ACTIVE'
            """
        )
    )
    op.create_check_constraint(
        "ck_provisioning_plan_apply_actor",
        "provisioning_plan",
        "terminal_result IS DISTINCT FROM 'ACTIVE' OR apply_actor_id IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_provisioning_plan_apply_actor",
        "provisioning_plan",
        type_="check",
    )
    op.drop_column("provisioning_plan", "apply_actor_id")
