"""Freeze immutable diagnostic snapshots for exports.

Revision ID: 20260723_0007
Revises: 20260723_0006
Create Date: 2026-07-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260723_0007"
down_revision: str | None = "20260723_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("export_jobs", sa.Column("snapshot_id", sa.Uuid(), nullable=True))
    op.execute("UPDATE export_jobs SET snapshot_id = gen_random_uuid()")
    op.create_table(
        "export_diagnostic_snapshots",
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("export_job_id", sa.Uuid(), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("requested_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("target", sa.String(length=2048), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column(
            "result_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "position >= 0", name="ck_export_diagnostic_snapshots_position"
        ),
        sa.ForeignKeyConstraint(
            ["export_job_id"], ["export_jobs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("snapshot_id", "position"),
        sa.UniqueConstraint(
            "snapshot_id",
            "id",
            name="uq_export_diagnostic_snapshots_snapshot_diagnostic",
        ),
    )
    op.create_index(
        "ix_export_diagnostic_snapshots_export_job_id",
        "export_diagnostic_snapshots",
        ["export_job_id"],
        unique=False,
    )
    op.execute(
        """
        INSERT INTO export_diagnostic_snapshots (
            snapshot_id,
            position,
            export_job_id,
            id,
            organization_id,
            requested_by_user_id,
            target,
            status,
            result_json,
            correlation_id,
            created_at,
            updated_at
        )
        SELECT
            export.snapshot_id,
            row_number() OVER (
                PARTITION BY export.id
                ORDER BY diagnostic.created_at ASC, diagnostic.id ASC
            ),
            export.id,
            diagnostic.id,
            diagnostic.organization_id,
            diagnostic.requested_by_user_id,
            diagnostic.target,
            diagnostic.status,
            diagnostic.result_json,
            diagnostic.correlation_id,
            diagnostic.created_at,
            diagnostic.updated_at
        FROM export_jobs AS export
        JOIN diagnostic_jobs AS diagnostic
          ON diagnostic.organization_id = export.organization_id
        """
    )
    op.alter_column("export_jobs", "snapshot_id", nullable=False)
    op.create_unique_constraint(
        "uq_export_jobs_snapshot_id", "export_jobs", ["snapshot_id"]
    )


def downgrade() -> None:
    op.drop_index(
        "ix_export_diagnostic_snapshots_export_job_id",
        table_name="export_diagnostic_snapshots",
    )
    op.drop_table("export_diagnostic_snapshots")
    op.drop_constraint("uq_export_jobs_snapshot_id", "export_jobs", type_="unique")
    op.drop_column("export_jobs", "snapshot_id")
