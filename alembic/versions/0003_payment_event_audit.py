"""Create immutable payment lifecycle audit rows.

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payment_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("old_status", sa.String(16)),
        sa.Column("new_status", sa.String(16)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_payment_events_payment_created", "payment_events", ["payment_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_payment_events_payment_created", table_name="payment_events")
    op.drop_table("payment_events")
