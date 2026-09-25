"""Transactional diagnostic lifecycle operations."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from signaldesk_control_api.auth import TenantContext
from signaldesk_control_api.models import DiagnosticJob
from signaldesk_control_api.services.email_deliveries import (
    EmailRecipientUnavailableError,
    create_email_delivery,
)
from signaldesk_control_api.services.outbox import add_diagnostic_event


class DiagnosticNotFoundError(Exception):
    pass


class DiagnosticStateConflictError(Exception):
    pass


def create_diagnostic(
    session: Session,
    *,
    context: TenantContext,
    target: str,
) -> DiagnosticJob:
    job = DiagnosticJob(
        organization_id=context.organization_id,
        requested_by_user_id=context.user_id,
        target=target,
    )
    session.add(job)
    try:
        session.flush()
        add_diagnostic_event(
            session,
            event_type="diagnostic.requested.v1",
            diagnostic_job_id=job.id,
            correlation_id=job.correlation_id,
            organization_id=job.organization_id,
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return job


def get_diagnostic(
    session: Session,
    *,
    organization_id: UUID,
    job_id: UUID,
) -> DiagnosticJob | None:
    return session.scalar(
        select(DiagnosticJob).where(
            DiagnosticJob.id == job_id,
            DiagnosticJob.organization_id == organization_id,
        )
    )


def claim_diagnostic(session: Session, *, job_id: UUID) -> DiagnosticJob:
    try:
        job = session.scalar(
            select(DiagnosticJob)
            .where(DiagnosticJob.id == job_id)
            .with_for_update()
        )
        if job is None:
            raise DiagnosticNotFoundError
        if job.status != "pending":
            raise DiagnosticStateConflictError
        job.status = "claimed"
        job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return job
    except Exception:
        session.rollback()
        raise


def complete_diagnostic(
    session: Session,
    *,
    job_id: UUID,
    result: dict[str, Any],
) -> DiagnosticJob:
    try:
        job = session.scalar(
            select(DiagnosticJob)
            .where(DiagnosticJob.id == job_id)
            .with_for_update()
        )
        if job is None:
            raise DiagnosticNotFoundError
        if job.status != "claimed":
            raise DiagnosticStateConflictError
        job.status = "completed"
        job.result_json = result
        job.updated_at = datetime.now(timezone.utc)
        add_diagnostic_event(
            session,
            event_type="diagnostic.completed.v1",
            diagnostic_job_id=job.id,
            correlation_id=job.correlation_id,
            organization_id=job.organization_id,
        )
        create_email_delivery(
            session,
            organization_id=job.organization_id,
            recipient_user_id=job.requested_by_user_id,
            template_name="diagnostic_completed",
            template_data={
                "diagnostic_job_id": str(job.id),
                "status": "completed",
            },
            correlation_id=job.correlation_id,
        )
        session.commit()
        return job
    except EmailRecipientUnavailableError as error:
        session.rollback()
        raise DiagnosticStateConflictError from error
    except Exception:
        session.rollback()
        raise
