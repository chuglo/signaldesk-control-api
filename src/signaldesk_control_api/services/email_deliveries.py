"""Authoritative email-delivery record and durable state operations."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from signaldesk_control_api.models import EmailDelivery, Membership, User
from signaldesk_control_api.services.outbox import add_email_event


class EmailRecipientUnavailableError(Exception):
    pass


class EmailDeliveryNotFoundError(Exception):
    pass


class EmailDeliveryStateConflictError(Exception):
    pass


@dataclass(frozen=True)
class EmailDeliveryAuthority:
    delivery: EmailDelivery
    recipient_email: str
    observed_at: datetime


def message_id_for_delivery(delivery_id: UUID) -> str:
    """Return the stable RFC-safe fixture Message-ID for one delivery."""

    return f"<{delivery_id}@signaldesk.local>"


def create_email_delivery(
    session: Session,
    *,
    organization_id: UUID,
    recipient_user_id: UUID,
    template_name: str,
    template_data: dict[str, Any],
    correlation_id: UUID,
) -> EmailDelivery:
    recipient = session.scalar(
        select(User)
        .join(
            Membership,
            and_(
                Membership.user_id == User.id,
                Membership.organization_id == organization_id,
            ),
        )
        .where(User.id == recipient_user_id, User.active.is_(True))
        .with_for_update(read=True)
    )
    if recipient is None:
        raise EmailRecipientUnavailableError
    delivery = EmailDelivery(
        organization_id=organization_id,
        recipient_user_id=recipient_user_id,
        template_name=template_name,
        template_data_json=template_data,
        correlation_id=correlation_id,
    )
    session.add(delivery)
    session.flush()
    add_email_event(
        session,
        email_delivery_id=delivery.id,
        correlation_id=delivery.correlation_id,
        organization_id=delivery.organization_id,
    )
    return delivery


def _current_recipient(
    session: Session, delivery: EmailDelivery, *, lock: bool = False
) -> User | None:
    statement = (
        select(User)
        .join(
            Membership,
            and_(
                Membership.user_id == User.id,
                Membership.organization_id == delivery.organization_id,
            ),
        )
        .where(User.id == delivery.recipient_user_id, User.active.is_(True))
    )
    if lock:
        statement = statement.with_for_update(read=True)
    return session.scalar(statement)


def _observed_at(session: Session) -> datetime:
    observed_at = session.scalar(select(func.now()))
    if observed_at is None:
        raise EmailDeliveryStateConflictError
    return observed_at


def _frozen_authority(
    session: Session, delivery: EmailDelivery
) -> EmailDeliveryAuthority:
    if delivery.recipient_email_snapshot is None:
        raise EmailDeliveryStateConflictError
    return EmailDeliveryAuthority(
        delivery=delivery,
        recipient_email=delivery.recipient_email_snapshot,
        observed_at=_observed_at(session),
    )


def claim_email_delivery(
    session: Session, *, delivery_id: UUID
) -> EmailDeliveryAuthority:
    """Atomically authorize and freeze the recipient before SMTP work."""

    try:
        delivery = session.scalar(
            select(EmailDelivery)
            .where(EmailDelivery.id == delivery_id)
            .with_for_update()
        )
        if delivery is None:
            raise EmailDeliveryNotFoundError
        if delivery.status != "pending":
            raise EmailDeliveryStateConflictError
        recipient = _current_recipient(session, delivery, lock=True)
        if recipient is None:
            raise EmailDeliveryNotFoundError
        delivery.status = "sending"
        delivery.recipient_email_snapshot = recipient.email
        delivery.attempted_at = _observed_at(session)
        session.commit()
        return _frozen_authority(session, delivery)
    except Exception:
        session.rollback()
        raise


def get_authoritative_delivery(
    session: Session, *, delivery_id: UUID
) -> EmailDeliveryAuthority | None:
    """Return current authority for pending, or frozen authority after claim."""

    delivery = session.get(EmailDelivery, delivery_id)
    if delivery is None:
        return None
    if delivery.status == "pending":
        recipient = _current_recipient(session, delivery)
        if recipient is None:
            return None
        return EmailDeliveryAuthority(
            delivery=delivery,
            recipient_email=recipient.email,
            observed_at=_observed_at(session),
        )
    if delivery.status in {"sending", "sent", "failed"}:
        return _frozen_authority(session, delivery)
    raise EmailDeliveryStateConflictError


def mark_email_delivery_sent(
    session: Session, *, delivery_id: UUID
) -> EmailDeliveryAuthority:
    """Persist SMTP acceptance; repeated sent calls preserve the original timestamp."""

    try:
        delivery = session.scalar(
            select(EmailDelivery)
            .where(EmailDelivery.id == delivery_id)
            .with_for_update()
        )
        if delivery is None:
            raise EmailDeliveryNotFoundError
        if delivery.status == "sent":
            session.commit()
            return _frozen_authority(session, delivery)
        if delivery.status != "sending":
            raise EmailDeliveryStateConflictError
        delivery.status = "sent"
        delivery.sent_at = _observed_at(session)
        session.commit()
        return _frozen_authority(session, delivery)
    except Exception:
        session.rollback()
        raise


def mark_email_delivery_failed(
    session: Session, *, delivery_id: UUID
) -> EmailDeliveryAuthority:
    """Persist a definite terminal failure while preserving frozen authority."""

    try:
        delivery = session.scalar(
            select(EmailDelivery)
            .where(EmailDelivery.id == delivery_id)
            .with_for_update()
        )
        if delivery is None:
            raise EmailDeliveryNotFoundError
        if delivery.status == "failed":
            session.commit()
            return _frozen_authority(session, delivery)
        if delivery.status != "sending":
            raise EmailDeliveryStateConflictError
        delivery.status = "failed"
        delivery.sent_at = None
        session.commit()
        return _frozen_authority(session, delivery)
    except Exception:
        session.rollback()
        raise
