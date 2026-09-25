from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

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
    ExportDiagnosticSnapshot,
    EmailDelivery,
    ExportJob,
    Organization,
    OutboxEvent,
)


def bff_headers(harness: IdentityHarness) -> dict[str, str]:
    return {
        "X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL,
        "X-SignalDesk-User-ID": str(harness.user_id),
        "X-SignalDesk-Organization-ID": str(harness.organization_id),
    }


def export_worker_headers(
    credential: str = EXPORT_WORKER_SERVICE_CREDENTIAL,
) -> dict[str, str]:
    return {"X-SignalDesk-Service-Credential": credential}


def test_export_job_metadata_has_planned_columns() -> None:
    table = Base.metadata.tables["export_jobs"]

    assert set(table.columns.keys()) == {
        "id",
        "organization_id",
        "requested_by_user_id",
        "format",
        "status",
        "object_key",
        "object_sha256",
        "size_bytes",
        "snapshot_id",
        "correlation_id",
        "created_at",
        "updated_at",
    }


def test_cross_tenant_export_read_and_download_metadata_return_404(
    identity_harness: IdentityHarness,
) -> None:
    other_organization = Organization(name="Other Export Tenant")
    with identity_harness.session_factory() as session:
        session.add(other_organization)
        session.flush()
        other_job = ExportJob(
            organization_id=other_organization.id,
            requested_by_user_id=identity_harness.user_id,
            format="json",
        )
        session.add(other_job)
        session.commit()
        job_id = other_job.id

    read = identity_harness.client.get(
        f"/exports/{job_id}", headers=bff_headers(identity_harness)
    )
    download = identity_harness.client.get(
        f"/exports/{job_id}/download", headers=bff_headers(identity_harness)
    )

    assert read.status_code == 404
    assert download.status_code == 404
    assert read.json() == download.json() == {"detail": "Export not found"}


def test_export_worker_claim_returns_authoritative_scope_and_expected_key(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    ).json()

    response = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim",
        headers=export_worker_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert UUID(body.pop("snapshot_id"))
    assert body == {
        "export_job_id": created["id"],
        "organization_id": str(identity_harness.organization_id),
        "requested_by_user_id": str(identity_harness.user_id),
        "correlation_id": created["correlation_id"],
        "format": "json",
        "expected_object_key": (
            f"exports/{identity_harness.organization_id}/{created['id']}.json"
        ),
    }


def test_export_completion_writes_artifact_events_and_authoritative_delivery(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "csv"}
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    )
    assert claim.status_code == 200
    object_key = claim.json()["expected_object_key"]

    response = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json={
            "snapshot_id": claim.json()["snapshot_id"],
            "object_key": object_key,
            "object_sha256": "a" * 64,
            "size_bytes": 123,
        },
    )

    assert response.status_code == 200
    assert response.json() == {"export_job_id": created["id"], "status": "completed"}
    with identity_harness.session_factory() as session:
        job = session.get(ExportJob, UUID(created["id"]))
        assert job is not None
        assert (job.object_key, job.object_sha256, job.size_bytes) == (
            object_key,
            "a" * 64,
            123,
        )
        delivery = session.scalar(select(EmailDelivery))
        assert delivery is not None
        assert delivery.organization_id == identity_harness.organization_id
        assert delivery.recipient_user_id == identity_harness.user_id
        assert delivery.correlation_id == job.correlation_id
        assert delivery.template_name == "export_completed"
        assert delivery.template_data_json == {
            "export_job_id": created["id"],
            "status": "completed",
            "format": "csv",
            "object_key": object_key,
            "object_sha256": "a" * 64,
            "size_bytes": 123,
        }
        events = {
            event.event_type: event for event in session.scalars(select(OutboxEvent))
        }
        assert set(events) == {
            "export.requested.v1",
            "export.completed.v1",
            "email.requested.v1",
        }
        assert set(events["export.completed.v1"].payload_json) == {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at",
            "correlation_id",
            "organization_id",
            "export_job_id",
        }
        assert set(events["email.requested.v1"].payload_json) == {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at",
            "correlation_id",
            "organization_id",
            "email_delivery_id",
        }
        assert "object_key" not in events["export.completed.v1"].payload_json
        assert "recipient_user_id" not in events["email.requested.v1"].payload_json
        assert "template_name" not in events["email.requested.v1"].payload_json


def test_export_history_pagination_uses_authoritative_job_tenant_and_is_bounded(
    identity_harness: IdentityHarness,
) -> None:
    other_organization = Organization(name="Other History Tenant")
    with identity_harness.session_factory() as session:
        session.add(other_organization)
        session.flush()
        own_jobs = [
            DiagnosticJob(
                organization_id=identity_harness.organization_id,
                requested_by_user_id=identity_harness.user_id,
                target=f"own-{index}.example.test",
                status="completed",
                result_json={"index": index},
            )
            for index in range(3)
        ]
        session.add_all(
            own_jobs
            + [
                DiagnosticJob(
                    organization_id=other_organization.id,
                    requested_by_user_id=identity_harness.user_id,
                    target="other.example.test",
                    status="completed",
                    result_json={"secret": "cross-tenant"},
                )
            ]
        )
        session.flush()
        last_own_job_id = own_jobs[-1].id
        session.commit()

    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    ).json()
    snapshot_id = claim["snapshot_id"]

    first = identity_harness.client.get(
        f"/internal/exports/{created['id']}/diagnostics",
        headers=export_worker_headers(),
        params={"limit": 2, "offset": 0, "snapshot_id": snapshot_id},
    )
    with identity_harness.session_factory() as session:
        live = session.get(DiagnosticJob, last_own_job_id)
        assert live is not None
        live.target = "mutated-between-pages.example.test"
        live.result_json = {"mutated": True}
        session.add(
            DiagnosticJob(
                organization_id=identity_harness.organization_id,
                requested_by_user_id=identity_harness.user_id,
                target="inserted-between-pages.example.test",
                status="completed",
                result_json={"inserted": True},
            )
        )
        session.commit()
    second = identity_harness.client.get(
        f"/internal/exports/{created['id']}/diagnostics",
        headers=export_worker_headers(),
        params={"limit": 2, "offset": 2, "snapshot_id": snapshot_id},
    )
    too_large = identity_harness.client.get(
        f"/internal/exports/{created['id']}/diagnostics",
        headers=export_worker_headers(),
        params={"limit": 101, "snapshot_id": snapshot_id},
    )

    assert first.status_code == second.status_code == 200
    assert len(first.json()["items"]) == 2
    assert len(second.json()["items"]) == 1
    returned = first.json()["items"] + second.json()["items"]
    assert {item["target"] for item in returned} == {
        "own-0.example.test",
        "own-1.example.test",
        "own-2.example.test",
    }
    assert all(
        item["organization_id"] == str(identity_harness.organization_id)
        for item in returned
    )
    assert too_large.status_code == 422


def test_export_history_rejects_organization_scope_assertion(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    ).json()
    identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    )
    response = identity_harness.client.get(
        f"/internal/exports/{created['id']}/diagnostics",
        headers=export_worker_headers(),
        params={"organization_id": str(uuid4())},
    )
    assert response.status_code == 422


def test_export_creation_writes_exact_minimal_requested_event(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    )
    assert response.status_code == 201
    with identity_harness.session_factory() as session:
        event = session.scalar(select(OutboxEvent))
        assert event is not None
        assert event.event_type == "export.requested.v1"
        assert set(event.payload_json) == {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at",
            "correlation_id",
            "organization_id",
            "export_job_id",
        }
        assert event.payload_json["export_job_id"] == response.json()["id"]
        assert "format" not in event.payload_json


def test_export_request_outbox_failure_rolls_back_job(
    identity_harness: IdentityHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected export outbox failure")

    monkeypatch.setattr(
        "signaldesk_control_api.services.exports.add_export_event", fail
    )
    response = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "csv"}
    )
    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(ExportJob)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.parametrize(
    "extra_field",
    ["organization_id", "requested_by_user_id", "user_id", "correlation_id"],
)
def test_export_creation_rejects_authority_fields(
    identity_harness: IdentityHarness, extra_field: str
) -> None:
    response = identity_harness.client.post(
        "/exports",
        headers=bff_headers(identity_harness),
        json={"format": "csv", extra_field: str(uuid4())},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("export_format", ["CSV", "xml", "", 1, None])
def test_export_creation_accepts_only_strict_csv_or_json(
    identity_harness: IdentityHarness, export_format: object
) -> None:
    response = identity_harness.client.post(
        "/exports",
        headers=bff_headers(identity_harness),
        json={"format": export_format},
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "credential",
    [
        None,
        "",
        "wrong-export-worker-credential",
        SERVICE_CREDENTIAL,
        WORKER_SERVICE_CREDENTIAL,
        EMAIL_WORKER_SERVICE_CREDENTIAL,
    ],
)
def test_export_worker_rejects_missing_blank_wrong_and_cross_service_credentials(
    identity_harness: IdentityHarness, credential: str | None
) -> None:
    headers = export_worker_headers(credential) if credential is not None else {}
    response = identity_harness.client.post(
        f"/internal/exports/{uuid4()}/claim", headers=headers
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}


def test_export_out_of_order_completion_conflicts_without_delivery_or_event(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    ).json()
    expected_key = f"exports/{identity_harness.organization_id}/{created['id']}.json"
    with identity_harness.session_factory() as session:
        pending = session.get(ExportJob, UUID(created["id"]))
        assert pending is not None
        snapshot_id = str(pending.snapshot_id)
    response = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json={
            "snapshot_id": snapshot_id,
            "object_key": expected_key,
            "object_sha256": "d" * 64,
            "size_bytes": 1,
        },
    )
    assert response.status_code == 409
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_duplicate_export_completion_conflicts_without_duplicate_events_or_delivery(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "csv"}
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    ).json()
    metadata = {
        "snapshot_id": claim["snapshot_id"],
        "object_key": claim["expected_object_key"],
        "object_sha256": "b" * 64,
        "size_bytes": 5,
    }
    first = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json=metadata,
    )
    duplicate = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json=metadata,
    )
    assert first.status_code == 200
    assert duplicate.status_code == 409
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 1
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 3


@pytest.mark.parametrize(
    "metadata",
    [
        {"object_key": "exports/wrong.csv", "object_sha256": "a" * 64, "size_bytes": 1},
        {"object_key": "EXPECTED", "object_sha256": "A" * 64, "size_bytes": 1},
        {"object_key": "EXPECTED", "object_sha256": "a" * 63, "size_bytes": 1},
        {"object_key": "EXPECTED", "object_sha256": "a" * 64, "size_bytes": -1},
        {
            "object_key": "EXPECTED",
            "object_sha256": "a" * 64,
            "size_bytes": 1_073_741_825,
        },
        {
            "object_key": "EXPECTED",
            "object_sha256": "a" * 64,
            "size_bytes": 1,
            "recipient_user_id": "x",
        },
    ],
)
def test_export_completion_rejects_untrusted_or_invalid_metadata(
    identity_harness: IdentityHarness, metadata: dict[str, object]
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "csv"}
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    ).json()
    body = {
        key: (claim["expected_object_key"] if value == "EXPECTED" else value)
        for key, value in metadata.items()
    }
    response = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json=body,
    )
    assert response.status_code == 422
    with identity_harness.session_factory() as session:
        job = session.get(ExportJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed"
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 0


def test_export_email_failure_rolls_back_completion_delivery_and_events(
    identity_harness: IdentityHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    ).json()

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected email failure")

    monkeypatch.setattr(
        "signaldesk_control_api.services.exports.create_email_delivery", fail
    )
    response = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json={
            "snapshot_id": claim["snapshot_id"],
            "object_key": claim["expected_object_key"],
            "object_sha256": "c" * 64,
            "size_bytes": 10,
        },
    )
    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        job = session.get(ExportJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed"
        assert job.object_key is None
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_create_export_derives_authoritative_tenant_and_user(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.post(
        "/exports",
        headers=bff_headers(identity_harness),
        json={"format": "csv"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["organization_id"] == str(identity_harness.organization_id)
    assert body["requested_by_user_id"] == str(identity_harness.user_id)
    assert body["format"] == "csv"
    assert body["status"] == "pending"
    assert body["object_key"] is None
    with identity_harness.session_factory() as session:
        job = session.get(ExportJob, UUID(body["id"]))
        assert job is not None
        assert job.organization_id == identity_harness.organization_id
        assert job.requested_by_user_id == identity_harness.user_id


def test_public_export_schemas_do_not_expose_internal_snapshot_id(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    )
    body = created.json()
    assert set(body) == {
        "id",
        "organization_id",
        "requested_by_user_id",
        "format",
        "status",
        "object_key",
        "object_sha256",
        "size_bytes",
        "correlation_id",
        "created_at",
        "updated_at",
    }
    download = identity_harness.client.get(
        f"/exports/{body['id']}/download", headers=bff_headers(identity_harness)
    )
    assert "snapshot_id" not in download.json()


def test_export_freezes_request_time_diagnostic_membership_and_full_values(
    identity_harness: IdentityHarness,
) -> None:
    original = DiagnosticJob(
        organization_id=identity_harness.organization_id,
        requested_by_user_id=identity_harness.user_id,
        target="original.example.test",
        status="completed",
        result_json={"value": "original"},
    )
    with identity_harness.session_factory() as session:
        session.add(original)
        session.commit()
        original_id = original.id

    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "json"}
    ).json()
    with identity_harness.session_factory() as session:
        live = session.get(DiagnosticJob, original_id)
        assert live is not None
        live.target = "mutated.example.test"
        live.result_json = {"value": "mutated"}
        session.add(
            DiagnosticJob(
                organization_id=identity_harness.organization_id,
                requested_by_user_id=identity_harness.user_id,
                target="inserted-later.example.test",
                status="completed",
                result_json={"value": "later"},
            )
        )
        session.commit()

    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    )
    assert claim.status_code == 200
    snapshot_id = claim.json()["snapshot_id"]
    page = identity_harness.client.get(
        f"/internal/exports/{created['id']}/diagnostics",
        headers=export_worker_headers(),
        params={"limit": 100, "offset": 0, "snapshot_id": snapshot_id},
    )
    assert page.status_code == 200
    assert page.json()["snapshot_id"] == snapshot_id
    assert [
        (item["id"], item["target"], item["result_json"])
        for item in page.json()["items"]
    ] == [(str(original_id), "original.example.test", {"value": "original"})]
    with identity_harness.session_factory() as session:
        assert len(list(session.scalars(select(ExportDiagnosticSnapshot)))) == 1


def test_claim_recovery_and_pages_bind_one_snapshot_and_reject_wrong_snapshot(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports", headers=bff_headers(identity_harness), json={"format": "csv"}
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim", headers=export_worker_headers()
    )
    assert claim.status_code == 200
    snapshot_id = claim.json()["snapshot_id"]
    recovered = identity_harness.client.get(
        f"/internal/exports/{created['id']}", headers=export_worker_headers()
    )
    assert recovered.status_code == 200
    assert recovered.json()["snapshot_id"] == snapshot_id
    assert recovered.json()["status"] == "claimed"
    assert recovered.json()["object_key"] is None

    for invalid_recovery in (
        identity_harness.client.get(
            f"/internal/exports/{created['id']}?organization_id={uuid4()}",
            headers=export_worker_headers(),
        ),
        identity_harness.client.request(
            "GET",
            f"/internal/exports/{created['id']}",
            headers=export_worker_headers(),
            content=b'{"snapshot_id":"attacker"}',
        ),
        identity_harness.client.request(
            "GET",
            f"/internal/exports/{created['id']}",
            headers=export_worker_headers() | {"Transfer-Encoding": "chunked"},
        ),
    ):
        assert invalid_recovery.status_code == 422

    with identity_harness.session_factory() as session:
        job = session.get(ExportJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed"

    wrong = str(uuid4())
    page = identity_harness.client.get(
        f"/internal/exports/{created['id']}/diagnostics",
        headers=export_worker_headers(),
        params={"limit": 100, "offset": 0, "snapshot_id": wrong},
    )
    assert page.status_code == 409
    completion = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers=export_worker_headers(),
        json={
            "snapshot_id": wrong,
            "object_key": claim.json()["expected_object_key"],
            "object_sha256": "a" * 64,
            "size_bytes": 1,
        },
    )
    assert completion.status_code == 409
    with identity_harness.session_factory() as session:
        job = session.get(ExportJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed" and job.object_key is None
