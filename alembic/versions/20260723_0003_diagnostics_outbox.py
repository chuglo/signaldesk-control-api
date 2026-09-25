"""Add tenant-scoped diagnostic jobs and transaction outbox.

Revision ID: 20260723_0003
Revises: 20260722_0002
Create Date: 2026-07-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260723_0003"
down_revision: str | None = "20260722_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "diagnostic_jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("requested_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("target", sa.String(length=2048), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
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
        sa.CheckConstraint(
            "status IN ('pending', 'claimed', 'completed')",
            name="ck_diagnostic_jobs_status",
        ),
        sa.CheckConstraint(
            "char_length(btrim(target)) > 0",
            name="ck_diagnostic_jobs_target_nonblank",
        ),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["requested_by_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_diagnostic_jobs_organization_created_at",
        "diagnostic_jobs",
        ["organization_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_diagnostic_jobs_status", "diagnostic_jobs", ["status"], unique=False
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "event_type", "aggregate_id", name="uq_outbox_events_type_aggregate"
        ),
    )
    op.create_index(
        "ix_outbox_events_published_created_at",
        "outbox_events",
        ["published_at", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_published_created_at", table_name="outbox_events"
    )
    op.drop_table("outbox_events")
    op.drop_index("ix_diagnostic_jobs_status", table_name="diagnostic_jobs")
    op.drop_index(
        "ix_diagnostic_jobs_organization_created_at", table_name="diagnostic_jobs"
    )
    op.drop_table("diagnostic_jobs")
