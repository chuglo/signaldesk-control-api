from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from signaldesk_control_api.auth import (
    TenantContext,
    get_tenant_context,
    require_web_bff_service,
)

router = APIRouter(prefix="/internal", tags=["internal"])


class TenantContextResponse(BaseModel):
    user_id: UUID
    organization_id: UUID
    role: str


@router.get(
    "/tenant-context",
    response_model=TenantContextResponse,
    dependencies=[Depends(require_web_bff_service)],
)
def tenant_context(
    context: TenantContext = Depends(get_tenant_context),
) -> TenantContext:
    return context
