"""Transaction-outbox row creation helpers."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from signaldesk_control_api.models import OutboxEvent


def _add_event(
    session: Session,
    *,
    event_type: str,
    aggregate_id: UUID,
    aggregate_field: str,
    correlation_id: UUID,
    organization_id: UUID,
) -> OutboxEvent:
    event_id = uuid4()
    payload = {
        "schema_version": 1,
        "event_id": str(event_id),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": str(correlation_id),
        "organization_id": str(organization_id),
        aggregate_field: str(aggregate_id),
    }
    event = OutboxEvent(
        id=event_id,
        event_type=event_type,
        aggregate_id=aggregate_id,
        payload_json=payload,
    )
    session.add(event)
    return event


def add_diagnostic_event(
    session: Session,
    *,
    event_type: str,
    diagnostic_job_id: UUID,
    correlation_id: UUID,
    organization_id: UUID,
) -> OutboxEvent:
    return _add_event(
        session,
        event_type=event_type,
        aggregate_id=diagnostic_job_id,
        aggregate_field="diagnostic_job_id",
        correlation_id=correlation_id,
        organization_id=organization_id,
    )


def add_export_event(
    session: Session,
    *,
    event_type: str,
    export_job_id: UUID,
    correlation_id: UUID,
    organization_id: UUID,
) -> OutboxEvent:
    return _add_event(
        session,
        event_type=event_type,
        aggregate_id=export_job_id,
        aggregate_field="export_job_id",
        correlation_id=correlation_id,
        organization_id=organization_id,
    )


def add_email_event(
    session: Session,
    *,
    email_delivery_id: UUID,
    correlation_id: UUID,
    organization_id: UUID,
) -> OutboxEvent:
    return _add_event(
        session,
        event_type="email.requested.v1",
        aggregate_id=email_delivery_id,
        aggregate_field="email_delivery_id",
        correlation_id=correlation_id,
        organization_id=organization_id,
    )
