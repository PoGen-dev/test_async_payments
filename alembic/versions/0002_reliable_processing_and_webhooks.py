"""Add processing leases and durable webhook delivery attempts.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("processing_token", "webhook_token"):
        op.add_column("payments", sa.Column(name, sa.Uuid()))
    for name in ("processing_lease_until", "webhook_lease_until"):
        op.add_column("payments", sa.Column(name, sa.DateTime(timezone=True)))
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("response_status", sa.Integer()),
        sa.Column("error", sa.Text()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("payment_id", "attempt", name="uq_webhook_delivery_attempt"),
    )
    op.create_index("ix_webhook_delivery_payment", "webhook_deliveries", ["payment_id", "attempt"])


def downgrade() -> None:
    op.drop_index("ix_webhook_delivery_payment", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
    for name in ("processing_token", "webhook_token", "processing_lease_until", "webhook_lease_until"):
        op.drop_column("payments", name)
