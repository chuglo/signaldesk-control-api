"""Tenant-scoped export routes and export-worker operations."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from anyio import fail_after
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from signaldesk_control_api.auth import (
    TenantContext,
    get_tenant_context,
    require_export_worker_service,
    require_web_bff_service,
)
from signaldesk_control_api.database import get_session
from signaldesk_control_api.models import ExportJob
from signaldesk_control_api.services.exports import (
    ExportNotFoundError,
    ExportObjectKeyError,
    ExportSnapshotConflictError,
    ExportStateConflictError,
    claim_export,
    complete_export,
    create_export,
    expected_object_key,
    get_export,
    get_worker_export,
    list_export_diagnostics,
)

router = APIRouter(
    prefix="/exports",
    tags=["exports"],
    dependencies=[Depends(require_web_bff_service)],
)
worker_router = APIRouter(
    prefix="/internal/exports",
    tags=["internal-exports"],
    dependencies=[Depends(require_export_worker_service)],
)


def _path_only_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="Export recovery accepts path authority only",
    )


async def _require_path_only(request: Request) -> None:
    """Bound and reject every non-path source of recovery authority."""

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


class ExportCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    format: Literal["csv", "json"]


class ExportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    organization_id: UUID
    requested_by_user_id: UUID
    format: Literal["csv", "json"]
    status: Literal["pending", "claimed", "completed"]
    object_key: str | None
    object_sha256: str | None
    size_bytes: int | None
    correlation_id: UUID
    created_at: datetime
    updated_at: datetime


class ExportDownloadResponse(BaseModel):
    export_job_id: UUID
    organization_id: UUID
    format: Literal["csv", "json"]
    status: Literal["pending", "claimed", "completed"]
    object_key: str | None
    object_sha256: str | None
    size_bytes: int | None


class ExportClaimResponse(BaseModel):
    export_job_id: UUID
    organization_id: UUID
    requested_by_user_id: UUID
    correlation_id: UUID
    format: Literal["csv", "json"]
    expected_object_key: str
    snapshot_id: UUID


@worker_router.post("/{job_id}/claim", response_model=ExportClaimResponse)
def claim(job_id: UUID, session: Session = Depends(get_session)) -> ExportClaimResponse:
    try:
        job = claim_export(session, job_id=job_id)
    except ExportNotFoundError as error:
        raise HTTPException(status_code=404, detail="Export not found") from error
    except ExportStateConflictError as error:
        raise HTTPException(status_code=409, detail="Export state conflict") from error
    return ExportClaimResponse(
        export_job_id=job.id,
        organization_id=job.organization_id,
        requested_by_user_id=job.requested_by_user_id,
        correlation_id=job.correlation_id,
        format=job.format,
        expected_object_key=expected_object_key(job),
        snapshot_id=job.snapshot_id,
    )


class ExportRecoveryResponse(ExportClaimResponse):
    status: Literal["claimed", "completed"]
    object_key: str | None
    object_sha256: str | None
    size_bytes: int | None


@worker_router.get("/{job_id}", response_model=ExportRecoveryResponse)
def recover(
    job_id: UUID,
    _path_only: None = Depends(_require_path_only),
    session: Session = Depends(get_session),
) -> ExportRecoveryResponse:
    try:
        job = get_worker_export(session, job_id=job_id)
    except ExportNotFoundError as error:
        raise HTTPException(status_code=404, detail="Export not found") from error
    except ExportStateConflictError as error:
        raise HTTPException(status_code=409, detail="Export state conflict") from error
    return ExportRecoveryResponse(
        export_job_id=job.id,
        organization_id=job.organization_id,
        requested_by_user_id=job.requested_by_user_id,
        correlation_id=job.correlation_id,
        format=job.format,
        expected_object_key=expected_object_key(job),
        snapshot_id=job.snapshot_id,
        status=job.status,
        object_key=job.object_key,
        object_sha256=job.object_sha256,
        size_bytes=job.size_bytes,
    )


class ExportComplete(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    object_key: str = Field(min_length=1, max_length=1024)
    object_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=1_073_741_824)
    snapshot_id: UUID = Field(strict=False)


class ExportCompleteResponse(BaseModel):
    export_job_id: UUID
    status: Literal["completed"]


@worker_router.post("/{job_id}/complete", response_model=ExportCompleteResponse)
def complete(
    job_id: UUID,
    body: ExportComplete,
    session: Session = Depends(get_session),
) -> ExportCompleteResponse:
    try:
        job = complete_export(
            session,
            job_id=job_id,
            object_key=body.object_key,
            object_sha256=body.object_sha256,
            size_bytes=body.size_bytes,
            snapshot_id=body.snapshot_id,
        )
    except ExportNotFoundError as error:
        raise HTTPException(status_code=404, detail="Export not found") from error
    except ExportStateConflictError as error:
        raise HTTPException(status_code=409, detail="Export state conflict") from error
    except ExportObjectKeyError as error:
        raise HTTPException(
            status_code=422, detail="Invalid export object key"
        ) from error
    except ExportSnapshotConflictError as error:
        raise HTTPException(
            status_code=409, detail="Export snapshot conflict"
        ) from error
    return ExportCompleteResponse(export_job_id=job.id, status="completed")


class ExportDiagnosticItem(BaseModel):
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


class ExportDiagnosticPage(BaseModel):
    items: list[ExportDiagnosticItem]
    limit: int
    offset: int
    snapshot_id: UUID


@worker_router.get("/{job_id}/diagnostics", response_model=ExportDiagnosticPage)
def diagnostics_page(
    job_id: UUID,
    request: Request,
    limit: int = 50,
    offset: int = 0,
    snapshot_id: UUID | None = None,
    session: Session = Depends(get_session),
) -> ExportDiagnosticPage:
    if set(request.query_params) - {"limit", "offset", "snapshot_id"}:
        raise HTTPException(status_code=422, detail="Invalid pagination")
    if not 1 <= limit <= 100 or offset < 0 or snapshot_id is None:
        raise HTTPException(status_code=422, detail="Invalid pagination")
    try:
        items = list_export_diagnostics(
            session,
            job_id=job_id,
            snapshot_id=snapshot_id,
            limit=limit,
            offset=offset,
        )
    except ExportNotFoundError as error:
        raise HTTPException(status_code=404, detail="Export not found") from error
    except ExportStateConflictError as error:
        raise HTTPException(status_code=409, detail="Export state conflict") from error
    except ExportSnapshotConflictError as error:
        raise HTTPException(
            status_code=409, detail="Export snapshot conflict"
        ) from error
    return ExportDiagnosticPage(
        items=[ExportDiagnosticItem.model_validate(item) for item in items],
        limit=limit,
        offset=offset,
        snapshot_id=snapshot_id,
    )


@router.post("", response_model=ExportResponse, status_code=status.HTTP_201_CREATED)
def create(
    body: ExportCreate,
    context: TenantContext = Depends(get_tenant_context),
    session: Session = Depends(get_session),
) -> ExportJob:
    return create_export(session, context=context, export_format=body.format)


@router.get("/{job_id}/download", response_model=ExportDownloadResponse)
def download_metadata(
    job_id: UUID,
    context: TenantContext = Depends(get_tenant_context),
    session: Session = Depends(get_session),
) -> ExportDownloadResponse:
    job = get_export(session, organization_id=context.organization_id, job_id=job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Export not found")
    return ExportDownloadResponse(
        export_job_id=job.id,
        organization_id=job.organization_id,
        format=job.format,
        status=job.status,
        object_key=job.object_key,
        object_sha256=job.object_sha256,
        size_bytes=job.size_bytes,
    )


@router.get("/{job_id}", response_model=ExportResponse)
def get(
    job_id: UUID,
    context: TenantContext = Depends(get_tenant_context),
    session: Session = Depends(get_session),
) -> ExportJob:
    job = get_export(session, organization_id=context.organization_id, job_id=job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Export not found")
    return job
