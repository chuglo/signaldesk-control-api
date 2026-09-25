"""Tenant-scoped BFF diagnostic routes."""

from datetime import datetime
import json
from typing import Annotated, Any, Literal
from uuid import UUID

from anyio.from_thread import run as run_async_from_thread
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
    JsonValue,
    StringConstraints,
)
from sqlalchemy.orm import Session

from signaldesk_control_api.auth import (
    TenantContext,
    get_tenant_context,
    require_diagnostic_worker_service,
    require_web_bff_service,
)
from signaldesk_control_api.database import get_session
from signaldesk_control_api.models import DiagnosticJob
from signaldesk_control_api.services.diagnostics import (
    DiagnosticNotFoundError,
    DiagnosticStateConflictError,
    claim_diagnostic,
    complete_diagnostic,
    create_diagnostic,
    get_diagnostic,
)

router = APIRouter(
    prefix="/diagnostics",
    tags=["diagnostics"],
    dependencies=[Depends(require_web_bff_service)],
)
worker_router = APIRouter(
    prefix="/internal/diagnostics",
    tags=["internal-diagnostics"],
    dependencies=[Depends(require_diagnostic_worker_service)],
)

Target = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2048),
]


class DiagnosticCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    target: Target


class DiagnosticResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    organization_id: UUID
    requested_by_user_id: UUID
    target: str
    status: str
    result_json: dict[str, Any] | None
    correlation_id: UUID
    created_at: datetime
    updated_at: datetime


class DiagnosticClaimResponse(BaseModel):
    diagnostic_job_id: UUID
    organization_id: UUID
    correlation_id: UUID
    target: str


class DiagnosticWorkerScopeResponse(DiagnosticClaimResponse):
    status: Literal["claimed", "completed"]


class DiagnosticComplete(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    result: dict[str, JsonValue]

    @field_validator("result")
    @classmethod
    def require_bounded_result(
        cls, value: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        try:
            encoded = json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise ValueError("result must contain valid JSON values") from error
        if len(encoded) > 16_384:
            raise ValueError("result must not exceed 16384 UTF-8 bytes")
        return value


class DiagnosticCompleteResponse(BaseModel):
    diagnostic_job_id: UUID
    status: Literal["completed"]


async def _request_has_nonempty_body(request: Request) -> bool:
    async for chunk in request.stream():
        if chunk:
            return True
    return False


@router.post("", response_model=DiagnosticResponse, status_code=status.HTTP_201_CREATED)
def create(
    body: DiagnosticCreate,
    context: TenantContext = Depends(get_tenant_context),
    session: Session = Depends(get_session),
) -> DiagnosticJob:
    return create_diagnostic(session, context=context, target=body.target)


@router.get("/{job_id}", response_model=DiagnosticResponse)
def get(
    job_id: UUID,
    context: TenantContext = Depends(get_tenant_context),
    session: Session = Depends(get_session),
) -> DiagnosticJob:
    job = get_diagnostic(
        session,
        organization_id=context.organization_id,
        job_id=job_id,
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnostic not found",
        )
    return job


@worker_router.get("/{job_id}", response_model=DiagnosticWorkerScopeResponse)
def get_worker_scope(
    job_id: UUID,
    request: Request,
    session: Session = Depends(get_session),
) -> DiagnosticWorkerScopeResponse:
    content_length = request.headers.get("content-length")
    if (
        request.query_params
        or content_length not in {None, "0"}
        or request.headers.get("transfer-encoding") is not None
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Worker scope accepts path authority only",
        )
    if run_async_from_thread(_request_has_nonempty_body, request):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Worker scope accepts path authority only",
        )
    job = session.get(DiagnosticJob, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnostic not found",
        )
    if job.status not in {"claimed", "completed"}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Diagnostic state conflict",
        )
    return DiagnosticWorkerScopeResponse(
        diagnostic_job_id=job.id,
        organization_id=job.organization_id,
        correlation_id=job.correlation_id,
        target=job.target,
        status=job.status,
    )


@worker_router.post("/{job_id}/claim", response_model=DiagnosticClaimResponse)
def claim(
    job_id: UUID,
    session: Session = Depends(get_session),
) -> DiagnosticClaimResponse:
    try:
        job = claim_diagnostic(session, job_id=job_id)
    except DiagnosticNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnostic not found",
        ) from error
    except DiagnosticStateConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Diagnostic state conflict",
        ) from error
    return DiagnosticClaimResponse(
        diagnostic_job_id=job.id,
        organization_id=job.organization_id,
        correlation_id=job.correlation_id,
        target=job.target,
    )


@worker_router.post("/{job_id}/complete", response_model=DiagnosticCompleteResponse)
def complete(
    job_id: UUID,
    body: DiagnosticComplete,
    session: Session = Depends(get_session),
) -> DiagnosticCompleteResponse:
    try:
        job = complete_diagnostic(session, job_id=job_id, result=body.result)
    except DiagnosticNotFoundError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnostic not found",
        ) from error
    except DiagnosticStateConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Diagnostic state conflict",
        ) from error
    return DiagnosticCompleteResponse(
        diagnostic_job_id=job.id,
        status="completed",
    )
