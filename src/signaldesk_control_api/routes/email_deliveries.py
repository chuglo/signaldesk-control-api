"""Email-worker authoritative delivery state routes."""

from datetime import datetime
from typing import Any, Literal, Self
from uuid import UUID

from anyio import fail_after
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, model_validator
from sqlalchemy.orm import Session

from signaldesk_control_api.auth import require_email_worker_service
from signaldesk_control_api.database import get_session
from signaldesk_control_api.services.email_deliveries import (
    EmailDeliveryAuthority,
    EmailDeliveryNotFoundError,
    EmailDeliveryStateConflictError,
    claim_email_delivery,
    get_authoritative_delivery,
    mark_email_delivery_failed,
    mark_email_delivery_sent,
    message_id_for_delivery,
)

router = APIRouter(
    prefix="/internal/email-deliveries",
    tags=["internal-email-deliveries"],
    dependencies=[Depends(require_email_worker_service)],
)


class EmailDeliveryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    email_delivery_id: UUID
    organization_id: UUID
    recipient_email: str
    template_name: str
    template_data: dict[str, Any]
    correlation_id: UUID
    status: Literal["pending", "sending", "sent", "failed"]
    message_id: str
    attempted_at: datetime | None
    sent_at: datetime | None
    observed_at: datetime

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.message_id != message_id_for_delivery(self.email_delivery_id):
            raise ValueError("message ID does not match delivery")
        for value in (self.attempted_at, self.sent_at, self.observed_at):
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise ValueError("delivery timestamps must be timezone-aware")
        if self.status == "pending" and (
            self.attempted_at is not None or self.sent_at is not None
        ):
            raise ValueError("invalid pending timestamps")
        if self.status in {"sending", "failed"} and (
            self.attempted_at is None or self.sent_at is not None
        ):
            raise ValueError("invalid unsent terminal timestamps")
        if self.status == "sent" and (
            self.attempted_at is None or self.sent_at is None
        ):
            raise ValueError("invalid sent timestamps")
        return self


def _path_only_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="Email delivery operation accepts path authority only",
    )


async def _require_path_only(request: Request) -> None:
    """Bound and reject every non-path source of operation authority."""

    content_length = request.headers.get("content-length")
    invalid = bool(
        request.query_params
        or (content_length is not None and content_length != "0")
        or request.headers.get("transfer-encoding") is not None
    )
    if not invalid:
        try:
            chunk_count = 0
            with fail_after(0.1):
                async for chunk in request.stream():
                    chunk_count += 1
                    if chunk or chunk_count > 4:
                        invalid = True
                        break
        except Exception:
            invalid = True
    if invalid:
        raise _path_only_error()


def _response(authority: EmailDeliveryAuthority) -> EmailDeliveryResponse:
    delivery = authority.delivery
    return EmailDeliveryResponse(
        email_delivery_id=delivery.id,
        organization_id=delivery.organization_id,
        recipient_email=authority.recipient_email,
        template_name=delivery.template_name,
        template_data=delivery.template_data_json,
        correlation_id=delivery.correlation_id,
        status=delivery.status,
        message_id=message_id_for_delivery(delivery.id),
        attempted_at=delivery.attempted_at,
        sent_at=delivery.sent_at,
        observed_at=authority.observed_at,
    )


@router.get("/{delivery_id}", response_model=EmailDeliveryResponse)
def get_delivery(
    delivery_id: UUID,
    _path_only: None = Depends(_require_path_only),
    session: Session = Depends(get_session),
) -> EmailDeliveryResponse:
    try:
        authoritative = get_authoritative_delivery(session, delivery_id=delivery_id)
    except EmailDeliveryStateConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email delivery state conflict",
        ) from error
    if authoritative is None:
        raise HTTPException(status_code=404, detail="Email delivery not found")
    return _response(authoritative)


@router.post("/{delivery_id}/claim", response_model=EmailDeliveryResponse)
def claim_delivery(
    delivery_id: UUID,
    _path_only: None = Depends(_require_path_only),
    session: Session = Depends(get_session),
) -> EmailDeliveryResponse:
    try:
        authority = claim_email_delivery(session, delivery_id=delivery_id)
    except EmailDeliveryNotFoundError as error:
        raise HTTPException(
            status_code=404, detail="Email delivery not found"
        ) from error
    except EmailDeliveryStateConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email delivery state conflict",
        ) from error
    return _response(authority)


@router.post("/{delivery_id}/sent", response_model=EmailDeliveryResponse)
def sent_delivery(
    delivery_id: UUID,
    _path_only: None = Depends(_require_path_only),
    session: Session = Depends(get_session),
) -> EmailDeliveryResponse:
    try:
        authority = mark_email_delivery_sent(session, delivery_id=delivery_id)
    except EmailDeliveryNotFoundError as error:
        raise HTTPException(
            status_code=404, detail="Email delivery not found"
        ) from error
    except EmailDeliveryStateConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email delivery state conflict",
        ) from error
    return _response(authority)


@router.post("/{delivery_id}/failed", response_model=EmailDeliveryResponse)
def failed_delivery(
    delivery_id: UUID,
    _path_only: None = Depends(_require_path_only),
    session: Session = Depends(get_session),
) -> EmailDeliveryResponse:
    try:
        authority = mark_email_delivery_failed(session, delivery_id=delivery_id)
    except EmailDeliveryNotFoundError as error:
        raise HTTPException(
            status_code=404, detail="Email delivery not found"
        ) from error
    except EmailDeliveryStateConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email delivery state conflict",
        ) from error
    return _response(authority)
