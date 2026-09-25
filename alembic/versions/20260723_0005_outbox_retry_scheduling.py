"""Add durable outbox retry scheduling.

Revision ID: 20260723_0005
Revises: 20260723_0004
Create Date: 2026-07-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260723_0005"
down_revision: str | None = "20260723_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "outbox_events",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_outbox_events_attempt_count",
        "outbox_events",
        "attempt_count >= 0",
    )
    op.drop_index(
        "ix_outbox_events_published_created_at", table_name="outbox_events"
    )
    op.create_index(
        "ix_outbox_events_publication_eligibility",
        "outbox_events",
        ["published_at", "next_attempt_at", "created_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_publication_eligibility", table_name="outbox_events"
    )
    op.create_index(
        "ix_outbox_events_published_created_at",
        "outbox_events",
        ["published_at", "created_at"],
        unique=False,
    )
    op.drop_constraint(
        "ck_outbox_events_attempt_count", "outbox_events", type_="check"
    )
    op.drop_column("outbox_events", "next_attempt_at")
    op.drop_column("outbox_events", "attempt_count")