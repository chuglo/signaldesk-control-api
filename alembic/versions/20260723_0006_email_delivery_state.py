"""Add durable authoritative email delivery state.

Revision ID: 20260723_0006
Revises: 20260723_0005
Create Date: 2026-07-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260723_0006"
down_revision: str | None = "20260723_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "email_deliveries",
        sa.Column("recipient_email_snapshot", sa.String(length=320), nullable=True),
    )
    op.add_column(
        "email_deliveries",
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.drop_constraint("ck_email_deliveries_status", "email_deliveries", type_="check")
    op.create_check_constraint(
        "ck_email_deliveries_status",
        "email_deliveries",
        "status IN ('pending', 'sending', 'sent', 'failed')",
    )
    op.execute(
        """
        UPDATE email_deliveries
        SET recipient_email_snapshot = NULL,
            attempted_at = NULL,
            sent_at = NULL
        WHERE status = 'pending'
        """
    )
    op.execute(
        """
        UPDATE email_deliveries AS delivery
        SET recipient_email_snapshot = users.email,
            attempted_at = COALESCE(delivery.attempted_at, CURRENT_TIMESTAMP),
            sent_at = COALESCE(delivery.attempted_at, CURRENT_TIMESTAMP)
        FROM users
        WHERE delivery.recipient_user_id = users.id
          AND delivery.status = 'sent'
        """
    )
    op.execute(
        """
        UPDATE email_deliveries AS delivery
        SET recipient_email_snapshot = users.email,
            attempted_at = COALESCE(delivery.attempted_at, CURRENT_TIMESTAMP),
            sent_at = NULL
        FROM users
        WHERE delivery.recipient_user_id = users.id
          AND delivery.status = 'failed'
        """
    )
    op.create_check_constraint(
        "ck_email_deliveries_delivery_state",
        "email_deliveries",
        "(status = 'pending' AND recipient_email_snapshot IS NULL AND attempted_at IS NULL AND sent_at IS NULL) OR "
        "(status = 'sending' AND recipient_email_snapshot IS NOT NULL AND attempted_at IS NOT NULL AND sent_at IS NULL) OR "
        "(status = 'sent' AND recipient_email_snapshot IS NOT NULL AND attempted_at IS NOT NULL AND sent_at IS NOT NULL) OR "
        "(status = 'failed' AND recipient_email_snapshot IS NOT NULL AND attempted_at IS NOT NULL AND sent_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_email_deliveries_delivery_state", "email_deliveries", type_="check"
    )
    op.execute(
        "UPDATE email_deliveries SET status = 'pending', attempted_at = NULL "
        "WHERE status = 'sending'"
    )
    op.drop_constraint("ck_email_deliveries_status", "email_deliveries", type_="check")
    op.create_check_constraint(
        "ck_email_deliveries_status",
        "email_deliveries",
        "status IN ('pending', 'sent', 'failed')",
    )
    op.drop_column("email_deliveries", "sent_at")
    op.drop_column("email_deliveries", "recipient_email_snapshot")
