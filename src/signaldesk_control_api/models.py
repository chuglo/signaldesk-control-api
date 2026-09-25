"""SQLAlchemy models owned by the control API."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, validates


class Base(DeclarativeBase):
    """Declarative base owned by the control API."""


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "email = lower(btrim(email))", name="ck_users_email_normalized"
        ),
        UniqueConstraint("email", name="uq_users_email"),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    @validates("email")
    def _normalize_email(self, _key: str, value: str) -> str:
        return value.strip().lower()


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)


class Membership(Base):
    __tablename__ = "memberships"

    user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    organization_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role: Mapped[str] = mapped_column(String(50), nullable=False)


class DiagnosticJob(Base):
    __tablename__ = "diagnostic_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'claimed', 'completed')",
            name="ck_diagnostic_jobs_status",
        ),
        CheckConstraint(
            "char_length(btrim(target)) > 0",
            name="ck_diagnostic_jobs_target_nonblank",
        ),
        Index(
            "ix_diagnostic_jobs_organization_created_at",
            "organization_id",
            "created_at",
        ),
        Index("ix_diagnostic_jobs_status", "status"),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    organization_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("organizations.id"), nullable=False
    )
    requested_by_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    target: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    result_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    correlation_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), nullable=False, default=uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ExportJob(Base):
    __tablename__ = "export_jobs"
    __table_args__ = (
        CheckConstraint("format IN ('csv', 'json')", name="ck_export_jobs_format"),
        CheckConstraint(
            "status IN ('pending', 'claimed', 'completed')",
            name="ck_export_jobs_status",
        ),
        CheckConstraint(
            "object_sha256 IS NULL OR object_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_export_jobs_object_sha256",
        ),
        CheckConstraint(
            "size_bytes IS NULL OR (size_bytes >= 0 AND size_bytes <= 1073741824)",
            name="ck_export_jobs_size_bytes",
        ),
        Index(
            "ix_export_jobs_organization_created_at",
            "organization_id",
            "created_at",
        ),
        Index("ix_export_jobs_status", "status"),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    organization_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("organizations.id"), nullable=False
    )
    requested_by_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    format: Mapped[str] = mapped_column(String(4), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    object_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    object_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(nullable=True)
    snapshot_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), nullable=False, default=uuid4, unique=True
    )
    correlation_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), nullable=False, default=uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ExportDiagnosticSnapshot(Base):
    __tablename__ = "export_diagnostic_snapshots"
    __table_args__ = (
        CheckConstraint(
            "position >= 0", name="ck_export_diagnostic_snapshots_position"
        ),
        UniqueConstraint(
            "snapshot_id",
            "id",
            name="uq_export_diagnostic_snapshots_snapshot_diagnostic",
        ),
        Index("ix_export_diagnostic_snapshots_export_job_id", "export_job_id"),
    )

    snapshot_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    export_job_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("export_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    organization_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    requested_by_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), nullable=False
    )
    target: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    result_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    correlation_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class EmailDelivery(Base):
    __tablename__ = "email_deliveries"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'sending', 'sent', 'failed')",
            name="ck_email_deliveries_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND recipient_email_snapshot IS NULL AND attempted_at IS NULL AND sent_at IS NULL) OR "
            "(status = 'sending' AND recipient_email_snapshot IS NOT NULL AND attempted_at IS NOT NULL AND sent_at IS NULL) OR "
            "(status = 'sent' AND recipient_email_snapshot IS NOT NULL AND attempted_at IS NOT NULL AND sent_at IS NOT NULL) OR "
            "(status = 'failed' AND recipient_email_snapshot IS NOT NULL AND attempted_at IS NOT NULL AND sent_at IS NULL)",
            name="ck_email_deliveries_delivery_state",
        ),
        CheckConstraint(
            "char_length(btrim(template_name)) > 0",
            name="ck_email_deliveries_template_name_nonblank",
        ),
        UniqueConstraint(
            "correlation_id",
            "template_name",
            name="uq_email_deliveries_correlation_template",
        ),
        Index("ix_email_deliveries_organization_id", "organization_id"),
        Index("ix_email_deliveries_status", "status"),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    organization_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("organizations.id"), nullable=False
    )
    recipient_user_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    template_name: Mapped[str] = mapped_column(String(100), nullable=False)
    template_data_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    correlation_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    recipient_email_snapshot: Mapped[str | None] = mapped_column(
        String(320), nullable=True
    )
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint(
            "event_type", "aggregate_id", name="uq_outbox_events_type_aggregate"
        ),
        CheckConstraint("attempt_count >= 0", name="ck_outbox_events_attempt_count"),
        Index(
            "ix_outbox_events_publication_eligibility",
            "published_at",
            "next_attempt_at",
            "created_at",
            "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
