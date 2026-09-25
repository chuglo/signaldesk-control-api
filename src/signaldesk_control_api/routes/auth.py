from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy.orm import Session

from signaldesk_control_api.auth import (
    TenantContext,
    authenticate_user,
    require_web_bff_service,
)
from signaldesk_control_api.database import get_session

router = APIRouter(prefix="/internal", tags=["internal-authentication"])


class AuthenticationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(min_length=1, max_length=320)
    password: SecretStr
    organization_id: UUID


class TenantContextResponse(BaseModel):
    user_id: UUID
    organization_id: UUID
    role: str


@router.post(
    "/authenticate",
    response_model=TenantContextResponse,
    dependencies=[Depends(require_web_bff_service)],
)
def authenticate(
    request: AuthenticationRequest,
    session: Session = Depends(get_session),
) -> TenantContext:
    return authenticate_user(
        session,
        email=request.email,
        password=request.password.get_secret_value(),
        organization_id=request.organization_id,
    )
