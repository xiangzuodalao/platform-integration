"""Persist the current internal shadow risk state.

Revision ID: 0003_internal_risk_state
Revises: 0002_provisioning_apply_actor
Create Date: 2026-07-30
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0003_internal_risk_state"
down_revision: str | None = "0002_provisioning_apply_actor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "risk_evaluation_state",
        sa.Column(
            "internal_active",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("risk_evaluation_state", "internal_active")
