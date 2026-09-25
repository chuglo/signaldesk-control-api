"""Add tenant-scoped export jobs and authoritative email deliveries.

Revision ID: 20260723_0004
Revises: 20260723_0003
Create Date: 2026-07-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260723_0004"
down_revision: str | None = "20260723_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "export_jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("requested_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("format", sa.String(length=4), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("object_key", sa.String(length=1024), nullable=True),
        sa.Column("object_sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
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
        sa.CheckConstraint("format IN ('csv', 'json')", name="ck_export_jobs_format"),
        sa.CheckConstraint(
            "status IN ('pending', 'claimed', 'completed')",
            name="ck_export_jobs_status",
        ),
        sa.CheckConstraint(
            "object_sha256 IS NULL OR object_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_export_jobs_object_sha256",
        ),
        sa.CheckConstraint(
            "size_bytes IS NULL OR (size_bytes >= 0 AND size_bytes <= 1073741824)",
            name="ck_export_jobs_size_bytes",
        ),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["requested_by_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_export_jobs_organization_created_at",
        "export_jobs",
        ["organization_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_export_jobs_status", "export_jobs", ["status"], unique=False
    )

    op.create_table(
        "email_deliveries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("recipient_user_id", sa.Uuid(), nullable=False),
        sa.Column("template_name", sa.String(length=100), nullable=False),
        sa.Column(
            "template_data_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'sent', 'failed')",
            name="ck_email_deliveries_status",
        ),
        sa.CheckConstraint(
            "char_length(btrim(template_name)) > 0",
            name="ck_email_deliveries_template_name_nonblank",
        ),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["recipient_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "correlation_id",
            "template_name",
            name="uq_email_deliveries_correlation_template",
        ),
    )
    op.create_index(
        "ix_email_deliveries_organization_id",
        "email_deliveries",
        ["organization_id"],
        unique=False,
    )
    op.create_index(
        "ix_email_deliveries_status",
        "email_deliveries",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_email_deliveries_status", table_name="email_deliveries")
    op.drop_index(
        "ix_email_deliveries_organization_id", table_name="email_deliveries"
    )
    op.drop_table("email_deliveries")
    op.drop_index("ix_export_jobs_status", table_name="export_jobs")
    op.drop_index(
        "ix_export_jobs_organization_created_at", table_name="export_jobs"
    )
    op.drop_table("export_jobs")
