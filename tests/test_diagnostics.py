from concurrent.futures import ThreadPoolExecutor
import inspect
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from conftest import (
    EMAIL_WORKER_SERVICE_CREDENTIAL,
    EXPORT_WORKER_SERVICE_CREDENTIAL,
    IdentityHarness,
    SERVICE_CREDENTIAL,
    WORKER_SERVICE_CREDENTIAL,
)
from signaldesk_control_api.models import (
    Base,
    DiagnosticJob,
    Organization,
    OutboxEvent,
)
from signaldesk_control_api.routes.diagnostics import get_worker_scope
from signaldesk_control_api.services.diagnostics import (
    DiagnosticStateConflictError,
    claim_diagnostic,
    complete_diagnostic,
)


def diagnostic_headers(harness: IdentityHarness) -> dict[str, str]:
    return {
        "X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL,
        "X-SignalDesk-User-ID": str(harness.user_id),
        "X-SignalDesk-Organization-ID": str(harness.organization_id),
    }


def worker_headers(credential: str = WORKER_SERVICE_CREDENTIAL) -> dict[str, str]:
    return {"X-SignalDesk-Service-Credential": credential}


def test_worker_scope_endpoint_keeps_sync_session_work_off_event_loop() -> None:
    assert not inspect.iscoroutinefunction(get_worker_scope)


def test_diagnostic_job_metadata_has_planned_columns() -> None:
    table = Base.metadata.tables["diagnostic_jobs"]

    assert set(table.columns.keys()) == {
        "id",
        "organization_id",
        "requested_by_user_id",
        "target",
        "status",
        "result_json",
        "correlation_id",
        "created_at",
        "updated_at",
    }


def test_diagnostic_metadata_indexes_tenant_history_and_pending_work() -> None:
    table = Base.metadata.tables["diagnostic_jobs"]

    assert {index.name for index in table.indexes} == {
        "ix_diagnostic_jobs_organization_created_at",
        "ix_diagnostic_jobs_status",
    }


def test_database_rejects_unknown_diagnostic_status(
    identity_harness: IdentityHarness,
) -> None:
    with identity_harness.session_factory() as session:
        session.add(
            DiagnosticJob(
                organization_id=identity_harness.organization_id,
                requested_by_user_id=identity_harness.user_id,
                target="synthetic.example.test",
                status="unknown",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_database_rejects_blank_diagnostic_target(
    identity_harness: IdentityHarness,
) -> None:
    with identity_harness.session_factory() as session:
        session.add(
            DiagnosticJob(
                organization_id=identity_harness.organization_id,
                requested_by_user_id=identity_harness.user_id,
                target="   ",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_create_diagnostic_derives_authoritative_tenant_and_user(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "  synthetic.example.test  "},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["organization_id"] == str(identity_harness.organization_id)
    assert body["requested_by_user_id"] == str(identity_harness.user_id)
    assert body["target"] == "synthetic.example.test"
    assert body["status"] == "pending"
    assert body["result_json"] is None
    with identity_harness.session_factory() as session:
        job = session.scalar(select(DiagnosticJob))
        assert job is not None
        assert str(job.id) == body["id"]
        assert job.organization_id == identity_harness.organization_id
        assert job.requested_by_user_id == identity_harness.user_id


@pytest.mark.parametrize(
    "extra_field",
    ["organization_id", "requested_by_user_id", "user_id", "correlation_id"],
)
def test_create_diagnostic_rejects_authority_fields_in_body(
    identity_harness: IdentityHarness,
    extra_field: str,
) -> None:
    response = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "synthetic.example.test", extra_field: str(uuid4())},
    )

    assert response.status_code == 422
    with identity_harness.session_factory() as session:
        assert session.scalar(select(DiagnosticJob)) is None


@pytest.mark.parametrize("target", ["", "   ", "x" * 2049])
def test_create_diagnostic_rejects_blank_or_oversized_target(
    identity_harness: IdentityHarness,
    target: str,
) -> None:
    response = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": target},
    )

    assert response.status_code == 422


def test_tenant_cannot_read_another_organizations_diagnostic(
    identity_harness: IdentityHarness,
) -> None:
    other_organization = Organization(name="Other Synthetic Tenant")
    with identity_harness.session_factory() as session:
        session.add(other_organization)
        session.flush()
        other_job = DiagnosticJob(
            organization_id=other_organization.id,
            requested_by_user_id=identity_harness.user_id,
            target="other-tenant.example.test",
        )
        session.add(other_job)
        session.commit()
        other_job_id = other_job.id

    response = identity_harness.client.get(
        f"/diagnostics/{other_job_id}",
        headers=diagnostic_headers(identity_harness),
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Diagnostic not found"}


def test_worker_claim_transitions_pending_job_and_returns_authoritative_work(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "authoritative-worker-target.example.test"},
    )
    assert created.status_code == 201
    created_body = created.json()

    response = identity_harness.client.post(
        f"/internal/diagnostics/{created_body['id']}/claim",
        headers={
            "X-SignalDesk-Service-Credential": WORKER_SERVICE_CREDENTIAL,
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "diagnostic_job_id": created_body["id"],
        "organization_id": str(identity_harness.organization_id),
        "correlation_id": created_body["correlation_id"],
        "target": "authoritative-worker-target.example.test",
    }
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(created_body["id"]))
        assert job is not None
        assert job.status == "claimed"


def test_worker_scope_refetches_claimed_and_completed_authority(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "tcp://authoritative.example.test:443"},
    ).json()
    job_id = created["id"]
    assert identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    ).status_code == 200

    claimed = identity_harness.client.get(
        f"/internal/diagnostics/{job_id}", headers=worker_headers()
    )

    expected = {
        "diagnostic_job_id": job_id,
        "organization_id": str(identity_harness.organization_id),
        "correlation_id": created["correlation_id"],
        "target": "tcp://authoritative.example.test:443",
        "status": "claimed",
    }
    assert claimed.status_code == 200
    assert claimed.json() == expected

    assert identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers(),
        json={"result": {"outcome": "reachable"}},
    ).status_code == 200
    completed = identity_harness.client.get(
        f"/internal/diagnostics/{job_id}", headers=worker_headers()
    )
    expected["status"] = "completed"
    assert completed.status_code == 200
    assert completed.json() == expected


def test_worker_scope_pending_conflicts_and_missing_is_not_found(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "tcp://pending.example.test:443"},
    ).json()

    pending = identity_harness.client.get(
        f"/internal/diagnostics/{created['id']}", headers=worker_headers()
    )
    missing = identity_harness.client.get(
        f"/internal/diagnostics/{uuid4()}", headers=worker_headers()
    )

    assert pending.status_code == 409
    assert pending.json() == {"detail": "Diagnostic state conflict"}
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Diagnostic not found"}


@pytest.mark.parametrize(
    "credential",
    [
        None,
        "",
        SERVICE_CREDENTIAL,
        EXPORT_WORKER_SERVICE_CREDENTIAL,
        EMAIL_WORKER_SERVICE_CREDENTIAL,
    ],
)
def test_worker_scope_requires_diagnostic_credential_and_denies_cross_service(
    identity_harness: IdentityHarness,
    credential: str | None,
) -> None:
    headers = worker_headers(credential) if credential is not None else {}

    response = identity_harness.client.get(
        f"/internal/diagnostics/{uuid4()}", headers=headers
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}


@pytest.mark.parametrize(
    ("query", "body"),
    [
        ({"organization_id": str(uuid4())}, None),
        ({"user_id": str(uuid4())}, None),
        (None, {"organization_id": str(uuid4())}),
    ],
)
def test_worker_scope_rejects_body_and_query_authority(
    identity_harness: IdentityHarness,
    query: dict[str, str] | None,
    body: dict[str, str] | None,
) -> None:
    request_kwargs: dict[str, object] = {
        "headers": worker_headers(),
        "params": query,
    }
    if body is not None:
        request_kwargs["json"] = body

    response = identity_harness.client.request(
        "GET", f"/internal/diagnostics/{uuid4()}", **request_kwargs
    )

    assert response.status_code == 422


def test_worker_scope_rejects_body_even_when_content_length_is_forged_zero(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.request(
        "GET",
        f"/internal/diagnostics/{uuid4()}",
        headers=worker_headers() | {"Content-Length": "0"},
        content=b'{"organization_id":"forged"}',
    )

    assert response.status_code == 422


def test_worker_scope_rejects_malformed_content_length_without_server_error(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.get(
        f"/internal/diagnostics/{uuid4()}",
        headers=worker_headers() | {"Content-Length": "not-a-number"},
    )

    assert response.status_code == 422


def test_worker_complete_transitions_claimed_job_and_persists_result(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "complete.example.test"},
    )
    job_id = created.json()["id"]
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers={"X-SignalDesk-Service-Credential": WORKER_SERVICE_CREDENTIAL},
    )
    assert claimed.status_code == 200

    result = {"reachable": True, "synthetic_latency_ms": 7}
    response = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers={"X-SignalDesk-Service-Credential": WORKER_SERVICE_CREDENTIAL},
        json={"result": result},
    )

    assert response.status_code == 200
    assert response.json() == {
        "diagnostic_job_id": job_id,
        "status": "completed",
    }
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(job_id))
        assert job is not None
        assert job.status == "completed"
        assert job.result_json == result


@pytest.mark.parametrize(
    "credential",
    [None, "", "wrong-worker-credential", SERVICE_CREDENTIAL],
)
def test_worker_endpoint_rejects_missing_blank_wrong_or_bff_credential(
    identity_harness: IdentityHarness,
    credential: str | None,
) -> None:
    headers = worker_headers(credential) if credential is not None else {}

    response = identity_harness.client.post(
        f"/internal/diagnostics/{uuid4()}/claim",
        headers=headers,
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}


def test_bff_diagnostic_route_rejects_worker_credential(
    identity_harness: IdentityHarness,
) -> None:
    headers = diagnostic_headers(identity_harness)
    headers["X-SignalDesk-Service-Credential"] = WORKER_SERVICE_CREDENTIAL

    response = identity_harness.client.post(
        "/diagnostics",
        headers=headers,
        json={"target": "must-not-create.example.test"},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}
    with identity_harness.session_factory() as session:
        assert session.scalar(select(DiagnosticJob)) is None


def test_worker_endpoint_rejects_non_ascii_raw_header(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.post(
        f"/internal/diagnostics/{uuid4()}/claim",
        headers=[
            (b"X-SignalDesk-Service-Credential", b"\xff" * 32),
        ],
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}


def test_duplicate_claim_returns_conflict_without_writing_an_event(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "duplicate-claim.example.test"},
    )
    job_id = created.json()["id"]
    first = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    )

    duplicate = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    )

    assert first.status_code == 200
    assert duplicate.status_code == 409
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_complete_before_claim_returns_conflict_without_writing_an_event(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "out-of-order.example.test"},
    )
    job_id = created.json()["id"]

    response = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers(),
        json={"result": {"reachable": False}},
    )

    assert response.status_code == 409
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(job_id))
        assert job is not None
        assert job.status == "pending"
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_duplicate_complete_returns_conflict_and_only_one_completed_event(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "duplicate-complete.example.test"},
    )
    job_id = created.json()["id"]
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    )
    assert claimed.status_code == 200
    first = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers(),
        json={"result": {"reachable": True}},
    )

    duplicate = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers(),
        json={"result": {"reachable": False}},
    )

    assert first.status_code == 200
    assert duplicate.status_code == 409
    with identity_harness.session_factory() as session:
        completed_events = session.scalar(
            select(func.count()).select_from(OutboxEvent).where(
                OutboxEvent.event_type == "diagnostic.completed.v1"
            )
        )
        assert completed_events == 1


def test_complete_rejects_oversized_result_without_changing_claimed_job(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "bounded-result.example.test"},
    )
    job_id = created.json()["id"]
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    )
    assert claimed.status_code == 200

    response = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers(),
        json={"result": {"output": "x" * 17_000}},
    )

    assert response.status_code == 422
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(job_id))
        assert job is not None
        assert job.status == "claimed"
        assert job.result_json is None


@pytest.mark.parametrize("extra_field", ["organization_id", "correlation_id"])
def test_complete_rejects_worker_authority_assertions(
    identity_harness: IdentityHarness,
    extra_field: str,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "worker-assertion.example.test"},
    )
    job_id = created.json()["id"]
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    )
    assert claimed.status_code == 200

    response = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers(),
        json={"result": {"reachable": True}, extra_field: str(uuid4())},
    )

    assert response.status_code == 422


def test_concurrent_claims_allow_exactly_one_pending_to_claimed_transition(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "concurrent-claim.example.test"},
    )
    job_id = UUID(created.json()["id"])
    barrier = Barrier(2)

    def race_claim() -> str:
        with identity_harness.session_factory() as session:
            barrier.wait(timeout=5)
            try:
                claim_diagnostic(session, job_id=job_id)
            except DiagnosticStateConflictError:
                return "conflict"
            return "claimed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _index: race_claim(), range(2)))

    assert sorted(outcomes) == ["claimed", "conflict"]
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, job_id)
        assert job is not None
        assert job.status == "claimed"


def test_concurrent_completions_allow_exactly_one_claimed_to_completed_transition(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "concurrent-complete.example.test"},
    )
    job_id = UUID(created.json()["id"])
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers(),
    )
    assert claimed.status_code == 200
    barrier = Barrier(2)

    def race_complete(index: int) -> str:
        with identity_harness.session_factory() as session:
            barrier.wait(timeout=5)
            try:
                complete_diagnostic(
                    session,
                    job_id=job_id,
                    result={"worker": index},
                )
            except DiagnosticStateConflictError:
                return "conflict"
            return "completed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(race_complete, range(2)))

    assert sorted(outcomes) == ["completed", "conflict"]
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, job_id)
        assert job is not None
        assert job.status == "completed"
        assert job.result_json in ({"worker": 0}, {"worker": 1})
        completed_events = session.scalar(
            select(func.count()).select_from(OutboxEvent).where(
                OutboxEvent.event_type == "diagnostic.completed.v1"
            )
        )
        assert completed_events == 1
