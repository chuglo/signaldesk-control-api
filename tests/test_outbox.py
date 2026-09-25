from datetime import datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from conftest import (
    IdentityHarness,
    SERVICE_CREDENTIAL,
    WORKER_SERVICE_CREDENTIAL,
)
from signaldesk_control_api.models import Base, DiagnosticJob, OutboxEvent


def diagnostic_headers(harness: IdentityHarness) -> dict[str, str]:
    return {
        "X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL,
        "X-SignalDesk-User-ID": str(harness.user_id),
        "X-SignalDesk-Organization-ID": str(harness.organization_id),
    }


def test_outbox_event_metadata_has_planned_columns() -> None:
    table = Base.metadata.tables["outbox_events"]

    assert set(table.columns.keys()) == {
        "id",
        "event_type",
        "aggregate_id",
        "payload_json",
        "published_at",
        "attempt_count",
        "next_attempt_at",
        "created_at",
    }
    assert table.c.attempt_count.nullable is False
    assert table.c.attempt_count.server_default is not None
    assert str(table.c.attempt_count.server_default.arg) == "0"
    assert table.c.next_attempt_at.type.timezone is True
    assert {constraint.name for constraint in table.constraints} >= {
        "ck_outbox_events_attempt_count"
    }


def test_outbox_metadata_indexes_publishable_events() -> None:
    table = Base.metadata.tables["outbox_events"]

    assert {index.name for index in table.indexes} == {
        "ix_outbox_events_publication_eligibility"
    }
    index = next(iter(table.indexes))
    assert [column.name for column in index.columns] == [
        "published_at",
        "next_attempt_at",
        "created_at",
        "id",
    ]


def test_database_rejects_duplicate_event_type_for_aggregate(
    identity_harness: IdentityHarness,
) -> None:
    aggregate_id = uuid4()
    payload = {"schema_version": 1}
    with identity_harness.session_factory() as session:
        session.add_all(
            [
                OutboxEvent(
                    event_type="diagnostic.completed.v1",
                    aggregate_id=aggregate_id,
                    payload_json=payload,
                ),
                OutboxEvent(
                    event_type="diagnostic.completed.v1",
                    aggregate_id=aggregate_id,
                    payload_json=payload,
                ),
            ]
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_create_diagnostic_commits_job_and_requested_event(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "synthetic.example.test"},
    )

    assert response.status_code == 201
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(DiagnosticJob)) == 1
        events = session.scalars(select(OutboxEvent)).all()
        assert len(events) == 1
        assert events[0].event_type == "diagnostic.requested.v1"
        assert str(events[0].aggregate_id) == response.json()["id"]


def test_requested_event_payload_matches_frozen_minimal_contract(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "must-not-enter-event.example.test"},
    )

    assert response.status_code == 201
    with identity_harness.session_factory() as session:
        event = session.scalar(select(OutboxEvent))
        assert event is not None
        payload = event.payload_json
        assert set(payload) == {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at",
            "correlation_id",
            "organization_id",
            "diagnostic_job_id",
        }
        assert payload["schema_version"] == 1
        assert UUID(payload["event_id"]) == event.id
        assert payload["event_type"] == "diagnostic.requested.v1"
        assert datetime.fromisoformat(payload["occurred_at"]).utcoffset() is not None
        assert payload["correlation_id"] == response.json()["correlation_id"]
        assert payload["organization_id"] == str(identity_harness.organization_id)
        assert payload["diagnostic_job_id"] == response.json()["id"]
        assert "target" not in payload
        assert "result" not in payload


def test_outbox_failure_rolls_back_job_and_event(
    identity_harness: IdentityHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_outbox(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected outbox failure")

    monkeypatch.setattr(
        "signaldesk_control_api.services.diagnostics.add_diagnostic_event",
        fail_outbox,
    )

    response = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "synthetic.example.test"},
    )

    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(DiagnosticJob)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


def test_completed_event_payload_matches_frozen_minimal_contract(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "completion-payload.example.test"},
    )
    created_body = created.json()
    worker_headers = {
        "X-SignalDesk-Service-Credential": WORKER_SERVICE_CREDENTIAL,
    }
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{created_body['id']}/claim",
        headers=worker_headers,
    )
    assert claimed.status_code == 200
    completed = identity_harness.client.post(
        f"/internal/diagnostics/{created_body['id']}/complete",
        headers=worker_headers,
        json={"result": {"secret_result": "must-not-enter-event"}},
    )
    assert completed.status_code == 200

    with identity_harness.session_factory() as session:
        event = session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.event_type == "diagnostic.completed.v1"
            )
        )
        assert event is not None
        payload = event.payload_json
        assert set(payload) == {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at",
            "correlation_id",
            "organization_id",
            "diagnostic_job_id",
        }
        assert payload["schema_version"] == 1
        assert UUID(payload["event_id"]) == event.id
        assert payload["event_type"] == "diagnostic.completed.v1"
        assert datetime.fromisoformat(payload["occurred_at"]).utcoffset() is not None
        assert payload["correlation_id"] == created_body["correlation_id"]
        assert payload["organization_id"] == str(identity_harness.organization_id)
        assert payload["diagnostic_job_id"] == created_body["id"]
        assert "target" not in payload
        assert "result" not in payload
        assert "secret_result" not in payload


def test_completion_outbox_failure_rolls_back_state_and_result(
    identity_harness: IdentityHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=diagnostic_headers(identity_harness),
        json={"target": "completion-rollback.example.test"},
    )
    job_id = created.json()["id"]
    worker_headers = {
        "X-SignalDesk-Service-Credential": WORKER_SERVICE_CREDENTIAL,
    }
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/claim",
        headers=worker_headers,
    )
    assert claimed.status_code == 200

    def fail_outbox(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected completion outbox failure")

    monkeypatch.setattr(
        "signaldesk_control_api.services.diagnostics.add_diagnostic_event",
        fail_outbox,
    )
    response = identity_harness.client.post(
        f"/internal/diagnostics/{job_id}/complete",
        headers=worker_headers,
        json={"result": {"must_roll_back": True}},
    )

    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(job_id))
        assert job is not None
        assert job.status == "claimed"
        assert job.result_json is None
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
