"""Bounded identity and tenant-membership authentication helpers."""

from dataclasses import dataclass
import secrets
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from werkzeug.security import check_password_hash, generate_password_hash

from signaldesk_control_api.database import get_session
from signaldesk_control_api.models import Membership, User
from signaldesk_control_api.settings import Settings

_INVALID_CREDENTIALS = "Invalid credentials"
_MEMBERSHIP_REQUIRED = "Organization membership required"
_DUMMY_PASSWORD_HASH = generate_password_hash("synthetic-non-user-password")


@dataclass(frozen=True)
class TenantContext:
    user_id: UUID
    organization_id: UUID
    role: str


def normalize_email(email: str) -> str:
    return email.strip().lower()


def authenticate_user(
    session: Session,
    *,
    email: str,
    password: str,
    organization_id: UUID,
) -> TenantContext:
    user = session.scalar(select(User).where(User.email == normalize_email(email)))
    password_hash = user.password_hash if user is not None else _DUMMY_PASSWORD_HASH
    if (
        not check_password_hash(password_hash, password)
        or user is None
        or not user.active
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=_INVALID_CREDENTIALS,
        )

    membership = session.get(Membership, (user.id, organization_id))
    if membership is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=_MEMBERSHIP_REQUIRED,
        )

    return TenantContext(
        user_id=user.id,
        organization_id=membership.organization_id,
        role=membership.role,
    )


def get_tenant_context(
    asserted_user_id: UUID = Header(alias="X-SignalDesk-User-ID"),
    asserted_organization_id: UUID = Header(alias="X-SignalDesk-Organization-ID"),
    session: Session = Depends(get_session),
) -> TenantContext:
    user = session.get(User, asserted_user_id)
    if user is None or not user.active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authoritative tenant context required",
        )
    membership = session.get(
        Membership,
        (user.id, asserted_organization_id),
    )
    if membership is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authoritative tenant context required",
        )
    return TenantContext(
        user_id=user.id,
        organization_id=membership.organization_id,
        role=membership.role,
    )


def get_settings(request: Request) -> Settings:
    settings: Settings | None = request.app.state.settings
    if settings is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is not configured",
        )
    return settings


def _require_service_credential(
    *,
    expected: str,
    presented: str | None,
) -> None:
    if (
        not expected
        or len(expected) < 32
        or expected != expected.strip()
        or not expected.isascii()
        or presented is None
        or not presented
        or len(presented) < 32
        or presented != presented.strip()
        or not presented.isascii()
        or not secrets.compare_digest(presented, expected)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid service credential",
        )


def require_web_bff_service(
    settings: Settings = Depends(get_settings),
    credential: str | None = Header(
        default=None,
        alias="X-SignalDesk-Service-Credential",
    ),
) -> None:
    _require_service_credential(
        expected=settings.web_bff_service_credential.get_secret_value(),
        presented=credential,
    )


def require_diagnostic_worker_service(
    settings: Settings = Depends(get_settings),
    credential: str | None = Header(
        default=None,
        alias="X-SignalDesk-Service-Credential",
    ),
) -> None:
    _require_service_credential(
        expected=settings.diagnostic_worker_service_credential.get_secret_value(),
        presented=credential,
    )


def require_export_worker_service(
    settings: Settings = Depends(get_settings),
    credential: str | None = Header(
        default=None,
        alias="X-SignalDesk-Service-Credential",
    ),
) -> None:
    _require_service_credential(
        expected=settings.export_worker_service_credential.get_secret_value(),
        presented=credential,
    )


def require_email_worker_service(
    settings: Settings = Depends(get_settings),
    credential: str | None = Header(
        default=None,
        alias="X-SignalDesk-Service-Credential",
    ),
) -> None:
    _require_service_credential(
        expected=settings.email_worker_service_credential.get_secret_value(),
        presented=credential,
    )
