"""Transactional export lifecycle operations."""

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, insert, literal, select
from sqlalchemy.orm import Session

from signaldesk_control_api.auth import TenantContext
from signaldesk_control_api.models import (
    DiagnosticJob,
    ExportDiagnosticSnapshot,
    ExportJob,
)
from signaldesk_control_api.services.email_deliveries import (
    EmailRecipientUnavailableError,
    create_email_delivery,
)
from signaldesk_control_api.services.outbox import add_export_event


class ExportNotFoundError(Exception):
    pass


class ExportStateConflictError(Exception):
    pass


class ExportObjectKeyError(Exception):
    pass


class ExportSnapshotConflictError(Exception):
    pass


def create_export(
    session: Session, *, context: TenantContext, export_format: str
) -> ExportJob:
    job = ExportJob(
        organization_id=context.organization_id,
        requested_by_user_id=context.user_id,
        format=export_format,
    )
    session.add(job)
    try:
        session.flush()
        snapshot_rows = select(
            literal(job.snapshot_id),
            func.row_number()
            .over(order_by=(DiagnosticJob.created_at.asc(), DiagnosticJob.id.asc()))
            .label("position"),
            literal(job.id),
            DiagnosticJob.id,
            DiagnosticJob.organization_id,
            DiagnosticJob.requested_by_user_id,
            DiagnosticJob.target,
            DiagnosticJob.status,
            DiagnosticJob.result_json,
            DiagnosticJob.correlation_id,
            DiagnosticJob.created_at,
            DiagnosticJob.updated_at,
        ).where(DiagnosticJob.organization_id == context.organization_id)
        session.execute(
            insert(ExportDiagnosticSnapshot).from_select(
                [
                    "snapshot_id",
                    "position",
                    "export_job_id",
                    "id",
                    "organization_id",
                    "requested_by_user_id",
                    "target",
                    "status",
                    "result_json",
                    "correlation_id",
                    "created_at",
                    "updated_at",
                ],
                snapshot_rows,
            )
        )
        add_export_event(
            session,
            event_type="export.requested.v1",
            export_job_id=job.id,
            correlation_id=job.correlation_id,
            organization_id=job.organization_id,
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return job


def get_export(
    session: Session, *, organization_id: UUID, job_id: UUID
) -> ExportJob | None:
    return session.scalar(
        select(ExportJob).where(
            ExportJob.id == job_id,
            ExportJob.organization_id == organization_id,
        )
    )


def claim_export(session: Session, *, job_id: UUID) -> ExportJob:
    try:
        job = session.scalar(
            select(ExportJob).where(ExportJob.id == job_id).with_for_update()
        )
        if job is None:
            raise ExportNotFoundError
        if job.status != "pending":
            raise ExportStateConflictError
        job.status = "claimed"
        job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return job
    except Exception:
        session.rollback()
        raise


def get_worker_export(session: Session, *, job_id: UUID) -> ExportJob:
    job = session.get(ExportJob, job_id)
    if job is None:
        raise ExportNotFoundError
    if job.status not in {"claimed", "completed"}:
        raise ExportStateConflictError
    return job


def complete_export(
    session: Session,
    *,
    job_id: UUID,
    object_key: str,
    object_sha256: str,
    size_bytes: int,
    snapshot_id: UUID,
) -> ExportJob:
    try:
        job = session.scalar(
            select(ExportJob).where(ExportJob.id == job_id).with_for_update()
        )
        if job is None:
            raise ExportNotFoundError
        if job.snapshot_id != snapshot_id:
            raise ExportSnapshotConflictError
        if job.status != "claimed":
            raise ExportStateConflictError
        if object_key != expected_object_key(job):
            raise ExportObjectKeyError
        job.status = "completed"
        job.object_key = object_key
        job.object_sha256 = object_sha256
        job.size_bytes = size_bytes
        job.updated_at = datetime.now(timezone.utc)
        add_export_event(
            session,
            event_type="export.completed.v1",
            export_job_id=job.id,
            correlation_id=job.correlation_id,
            organization_id=job.organization_id,
        )
        create_email_delivery(
            session,
            organization_id=job.organization_id,
            recipient_user_id=job.requested_by_user_id,
            template_name="export_completed",
            template_data={
                "export_job_id": str(job.id),
                "status": "completed",
                "format": job.format,
                "object_key": object_key,
                "object_sha256": object_sha256,
                "size_bytes": size_bytes,
            },
            correlation_id=job.correlation_id,
        )
        session.commit()
        return job
    except EmailRecipientUnavailableError as error:
        session.rollback()
        raise ExportStateConflictError from error
    except Exception:
        session.rollback()
        raise


def list_export_diagnostics(
    session: Session, *, job_id: UUID, snapshot_id: UUID, limit: int, offset: int
) -> list[ExportDiagnosticSnapshot]:
    export_job = session.get(ExportJob, job_id)
    if export_job is None:
        raise ExportNotFoundError
    if export_job.status not in {"claimed", "completed"}:
        raise ExportStateConflictError
    if export_job.snapshot_id != snapshot_id:
        raise ExportSnapshotConflictError
    return list(
        session.scalars(
            select(ExportDiagnosticSnapshot)
            .where(
                ExportDiagnosticSnapshot.export_job_id == export_job.id,
                ExportDiagnosticSnapshot.snapshot_id == snapshot_id,
            )
            .order_by(ExportDiagnosticSnapshot.position.asc())
            .offset(offset)
            .limit(limit)
        )
    )


def expected_object_key(job: ExportJob) -> str:
    return f"exports/{job.organization_id}/{job.id}.{job.format}"
