import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from threading import Event
import time
from uuid import UUID

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
    EmailDelivery,
    Membership,
    OutboxEvent,
    User,
)
from signaldesk_control_api.services import email_deliveries as email_delivery_service


def bff_headers(harness: IdentityHarness) -> dict[str, str]:
    return {
        "X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL,
        "X-SignalDesk-User-ID": str(harness.user_id),
        "X-SignalDesk-Organization-ID": str(harness.organization_id),
    }


def diagnostic_worker_headers() -> dict[str, str]:
    return {"X-SignalDesk-Service-Credential": WORKER_SERVICE_CREDENTIAL}


def email_worker_headers(
    credential: str = EMAIL_WORKER_SERVICE_CREDENTIAL,
) -> dict[str, str]:
    return {"X-SignalDesk-Service-Credential": credential}


def test_email_delivery_metadata_has_planned_columns() -> None:
    table = Base.metadata.tables["email_deliveries"]

    assert set(table.columns.keys()) == {
        "id",
        "organization_id",
        "recipient_user_id",
        "template_name",
        "template_data_json",
        "status",
        "correlation_id",
        "attempted_at",
        "recipient_email_snapshot",
        "sent_at",
    }
    assert {constraint.name for constraint in table.constraints} >= {
        "uq_email_deliveries_correlation_template",
        "ck_email_deliveries_delivery_state",
    }


@pytest.mark.parametrize(
    ("status_value", "snapshot", "attempted", "sent", "valid"),
    [
        ("pending", None, None, None, True),
        ("pending", None, "now", None, False),
        ("sending", "frozen@example.test", "now", None, True),
        ("sending", None, "now", None, False),
        ("sent", "frozen@example.test", "now", "now", True),
        ("sent", "frozen@example.test", None, "now", False),
        ("failed", "frozen@example.test", "now", None, True),
        ("failed", None, "now", None, False),
    ],
)
def test_database_enforces_exact_delivery_state_shape(
    identity_harness: IdentityHarness,
    status_value: str,
    snapshot: str | None,
    attempted: str | None,
    sent: str | None,
    valid: bool,
) -> None:
    now = datetime.now(timezone.utc)
    delivery = EmailDelivery(
        organization_id=identity_harness.organization_id,
        recipient_user_id=identity_harness.user_id,
        template_name=f"state-{status_value}-{valid}",
        template_data_json={},
        status=status_value,
        correlation_id=UUID(int=sum(map(ord, status_value)) + int(valid)),
        recipient_email_snapshot=snapshot,
        attempted_at=now if attempted else None,
        sent_at=now if sent else None,
    )
    with identity_harness.session_factory() as session:
        session.add(delivery)
        if valid:
            session.commit()
        else:
            with pytest.raises(IntegrityError):
                session.commit()
            session.rollback()


def test_email_worker_fetch_resolves_authoritative_recipient_and_template(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "email-fetch.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )
    with identity_harness.session_factory() as session:
        delivery = session.scalar(select(EmailDelivery))
        assert delivery is not None
        delivery_id = delivery.id

    response = identity_harness.client.get(
        f"/internal/email-deliveries/{delivery_id}", headers=email_worker_headers()
    )

    assert response.status_code == 200
    assert response.json() == {
        "email_delivery_id": str(delivery_id),
        "organization_id": str(identity_harness.organization_id),
        "recipient_email": "user@example.test",
        "template_name": "diagnostic_completed",
        "template_data": {
            "diagnostic_job_id": created["id"],
            "status": "completed",
        },
        "correlation_id": created["correlation_id"],
        "status": "pending",
        "message_id": f"<{delivery_id}@signaldesk.local>",
        "attempted_at": None,
        "sent_at": None,
        "observed_at": response.json()["observed_at"],
    }
    assert datetime.fromisoformat(response.json()["observed_at"]).tzinfo is not None


def _create_pending_delivery(
    identity_harness: IdentityHarness,
) -> tuple[UUID, dict[str, object]]:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "email-state.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    completed = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )
    assert completed.status_code == 200
    with identity_harness.session_factory() as session:
        delivery = session.scalar(select(EmailDelivery))
        assert delivery is not None
        return delivery.id, created


def test_claim_freezes_recipient_and_sent_is_idempotent(
    identity_harness: IdentityHarness,
) -> None:
    delivery_id, created = _create_pending_delivery(identity_harness)

    claim = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/claim",
        headers=email_worker_headers(),
    )

    assert claim.status_code == 200
    assert claim.json() == {
        "email_delivery_id": str(delivery_id),
        "organization_id": str(identity_harness.organization_id),
        "recipient_email": "user@example.test",
        "template_name": "diagnostic_completed",
        "template_data": {
            "diagnostic_job_id": created["id"],
            "status": "completed",
        },
        "correlation_id": created["correlation_id"],
        "status": "sending",
        "message_id": f"<{delivery_id}@signaldesk.local>",
        "attempted_at": claim.json()["attempted_at"],
        "sent_at": None,
        "observed_at": claim.json()["observed_at"],
    }
    assert claim.json()["attempted_at"] is not None

    with identity_harness.session_factory() as session:
        user = session.get(User, identity_harness.user_id)
        assert user is not None
        user.email = "changed@example.test"
        membership = session.get(
            Membership,
            (identity_harness.user_id, identity_harness.organization_id),
        )
        assert membership is not None
        session.delete(membership)
        session.commit()

    recovered = identity_harness.client.get(
        f"/internal/email-deliveries/{delivery_id}", headers=email_worker_headers()
    )
    assert recovered.status_code == 200
    assert recovered.json()["recipient_email"] == "user@example.test"
    assert recovered.json()["status"] == "sending"

    first = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/sent",
        headers=email_worker_headers(),
    )
    duplicate = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/sent",
        headers=email_worker_headers(),
    )
    assert first.status_code == duplicate.status_code == 200
    assert first.json()["status"] == duplicate.json()["status"] == "sent"
    assert first.json()["sent_at"] is not None
    assert duplicate.json()["sent_at"] == first.json()["sent_at"]


def test_failed_transition_is_durable_idempotent_and_preserves_frozen_authority(
    identity_harness: IdentityHarness,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    claim = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/claim",
        headers=email_worker_headers(),
    ).json()

    first = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/failed",
        headers=email_worker_headers(),
    )
    duplicate = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/failed",
        headers=email_worker_headers(),
    )
    fetched = identity_harness.client.get(
        f"/internal/email-deliveries/{delivery_id}",
        headers=email_worker_headers(),
    )

    assert first.status_code == duplicate.status_code == fetched.status_code == 200
    for response in (first, duplicate, fetched):
        payload = response.json()
        assert payload["status"] == "failed"
        assert payload["recipient_email"] == claim["recipient_email"]
        assert payload["attempted_at"] == claim["attempted_at"]
        assert payload["sent_at"] is None
        assert datetime.fromisoformat(payload["observed_at"]).tzinfo is not None
    assert (
        identity_harness.client.post(
            f"/internal/email-deliveries/{delivery_id}/sent",
            headers=email_worker_headers(),
        ).status_code
        == 409
    )


def test_pending_and_invalid_delivery_state_transitions_conflict(
    identity_harness: IdentityHarness,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    premature = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/sent",
        headers=email_worker_headers(),
    )
    assert premature.status_code == 409
    first = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/claim",
        headers=email_worker_headers(),
    )
    second = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/claim",
        headers=email_worker_headers(),
    )
    assert first.status_code == 200
    assert second.status_code == 409


def test_concurrent_claim_has_exactly_one_winner(
    identity_harness: IdentityHarness,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)

    def claim_once():
        return identity_harness.client.post(
            f"/internal/email-deliveries/{delivery_id}/claim",
            headers=email_worker_headers(),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = [
            future.result(timeout=5)
            for future in [executor.submit(claim_once), executor.submit(claim_once)]
        ]

    assert sorted(response.status_code for response in responses) == [200, 409]
    with identity_harness.session_factory() as session:
        delivery = session.get(EmailDelivery, delivery_id)
        assert delivery is not None
        assert delivery.status == "sending"
        assert delivery.recipient_email_snapshot == "user@example.test"
        assert delivery.attempted_at is not None


@pytest.mark.parametrize("revocation", ["disabled", "membership"])
def test_claim_revalidates_current_recipient_authority(
    identity_harness: IdentityHarness,
    revocation: str,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    with identity_harness.session_factory() as session:
        if revocation == "disabled":
            user = session.get(User, identity_harness.user_id)
            assert user is not None
            user.active = False
        else:
            membership = session.get(
                Membership,
                (identity_harness.user_id, identity_harness.organization_id),
            )
            assert membership is not None
            session.delete(membership)
        session.commit()

    response = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/claim",
        headers=email_worker_headers(),
    )
    assert response.status_code == 404
    with identity_harness.session_factory() as session:
        delivery = session.get(EmailDelivery, delivery_id)
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.recipient_email_snapshot is None


def test_claim_commit_failure_rolls_back_snapshot_and_state(
    identity_harness: IdentityHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    original_commit = email_delivery_service.Session.commit

    def fail_claim_commit(session):
        if any(
            isinstance(value, EmailDelivery) and value.status == "sending"
            for value in session.dirty
        ):
            raise RuntimeError("injected claim commit failure")
        return original_commit(session)

    monkeypatch.setattr(email_delivery_service.Session, "commit", fail_claim_commit)
    response = identity_harness.client.post(
        f"/internal/email-deliveries/{delivery_id}/claim",
        headers=email_worker_headers(),
    )
    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        delivery = session.get(EmailDelivery, delivery_id)
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.recipient_email_snapshot is None
        assert delivery.attempted_at is None


@pytest.mark.parametrize(
    "suffix", ["?organization_id=00000000-0000-0000-0000-000000000001", ""]
)
@pytest.mark.parametrize("operation", ["claim", "sent", "failed"])
def test_email_state_routes_reject_query_and_body_authority(
    identity_harness: IdentityHarness,
    operation: str,
    suffix: str,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    response = identity_harness.client.request(
        "POST",
        f"/internal/email-deliveries/{delivery_id}/{operation}{suffix}",
        headers=email_worker_headers(),
        json={"recipient_email": "attacker@example.test"} if not suffix else None,
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("method", "suffix"),
    [("GET", ""), ("POST", "/claim"), ("POST", "/sent"), ("POST", "/failed")],
)
def test_every_email_worker_state_route_rejects_cross_service_credentials(
    identity_harness: IdentityHarness,
    method: str,
    suffix: str,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    for credential in (
        SERVICE_CREDENTIAL,
        WORKER_SERVICE_CREDENTIAL,
        EXPORT_WORKER_SERVICE_CREDENTIAL,
    ):
        response = identity_harness.client.request(
            method,
            f"/internal/email-deliveries/{delivery_id}{suffix}",
            headers=email_worker_headers(credential),
        )
        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid service credential"}


def test_email_get_rejects_body_authority(
    identity_harness: IdentityHarness,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)
    response = identity_harness.client.request(
        "GET",
        f"/internal/email-deliveries/{delivery_id}",
        headers=email_worker_headers(),
        content=b'{"recipient_email":"attacker@example.test"}',
    )
    assert response.status_code == 422


def test_path_only_dependency_bounds_slow_unframed_and_disconnected_bodies_without_db_mutation(
    identity_harness: IdentityHarness,
) -> None:
    delivery_id, _ = _create_pending_delivery(identity_harness)

    async def invoke(receive):
        route = f"/internal/email-deliveries/{delivery_id}/claim"
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "2",
            "method": "POST",
            "scheme": "https",
            "path": route,
            "raw_path": route.encode(),
            "query_string": b"",
            "headers": [
                (b"host", b"testserver"),
                (
                    b"x-signaldesk-service-credential",
                    EMAIL_WORKER_SERVICE_CREDENTIAL.encode(),
                ),
            ],
            "client": ("127.0.0.1", 1),
            "server": ("testserver", 443),
        }
        sent: list[dict[str, object]] = []

        async def send(message):
            sent.append(message)

        started = time.monotonic()
        await identity_harness.client.app(scope, receive, send)
        return time.monotonic() - started, sent

    async def slow_receive():
        await asyncio.sleep(1)
        return {"type": "http.request", "body": b"x", "more_body": False}

    async def disconnected_receive():
        return {"type": "http.disconnect"}

    for receive in (slow_receive, disconnected_receive):
        elapsed, sent = asyncio.run(invoke(receive))
        assert elapsed < 0.5
        starts = [
            message for message in sent if message["type"] == "http.response.start"
        ]
        assert starts[0]["status"] == 422

    with identity_harness.session_factory() as session:
        delivery = session.get(EmailDelivery, delivery_id)
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.attempted_at is None


@pytest.mark.parametrize(
    "credential",
    [
        None,
        "",
        "wrong-email-worker-credential",
        SERVICE_CREDENTIAL,
        WORKER_SERVICE_CREDENTIAL,
        EXPORT_WORKER_SERVICE_CREDENTIAL,
    ],
)
def test_email_worker_rejects_missing_blank_wrong_and_cross_service_credentials(
    identity_harness: IdentityHarness, credential: str | None
) -> None:
    headers = email_worker_headers(credential) if credential is not None else {}
    response = identity_harness.client.get(
        "/internal/email-deliveries/00000000-0000-0000-0000-000000000001",
        headers=headers,
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}


def test_duplicate_diagnostic_completion_creates_one_delivery_and_email_event(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "duplicate-email.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    first = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )
    duplicate = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": False}},
    )
    assert first.status_code == 200
    assert duplicate.status_code == 409
    with identity_harness.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 1
        assert (
            session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.event_type == "email.requested.v1")
            )
            == 1
        )


def test_diagnostic_email_failure_rolls_back_completion_and_delivery(
    identity_harness: IdentityHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "rollback-email.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected email creation failure")

    monkeypatch.setattr(
        "signaldesk_control_api.services.diagnostics.create_email_delivery", fail
    )
    response = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )
    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed"
        assert job.result_json is None
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_email_outbox_failure_after_delivery_flush_rolls_back_everything(
    identity_harness: IdentityHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "rollback-email-outbox.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected email outbox failure")

    monkeypatch.setattr(
        "signaldesk_control_api.services.email_deliveries.add_email_event",
        fail,
    )
    response = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )

    assert response.status_code == 500
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed"
        assert job.result_json is None
        assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.parametrize(
    "extra_field",
    [
        "recipient_user_id",
        "recipient_email",
        "template_name",
        "template_data",
        "organization_id",
    ],
)
def test_diagnostic_completion_rejects_email_authority_overrides(
    identity_harness: IdentityHarness, extra_field: str
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "override-email.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    response = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}, extra_field: "attacker-controlled"},
    )
    assert response.status_code == 422
    with identity_harness.session_factory() as session:
        assert session.scalar(select(EmailDelivery)) is None


def test_diagnostic_completion_creates_authoritative_delivery_and_email_event(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "email-delivery.example.test"},
    ).json()
    claimed = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    assert claimed.status_code == 200

    completed = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )

    assert completed.status_code == 200
    with identity_harness.session_factory() as session:
        delivery = session.scalar(select(EmailDelivery))
        assert delivery is not None
        assert delivery.organization_id == identity_harness.organization_id
        assert delivery.recipient_user_id == identity_harness.user_id
        assert delivery.correlation_id == UUID(created["correlation_id"])
        assert delivery.template_name == "diagnostic_completed"
        assert delivery.template_data_json == {
            "diagnostic_job_id": created["id"],
            "status": "completed",
        }
        email_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.event_type == "email.requested.v1")
        )
        assert email_event is not None
        assert set(email_event.payload_json) == {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at",
            "correlation_id",
            "organization_id",
            "email_delivery_id",
        }
        assert email_event.payload_json["email_delivery_id"] == str(delivery.id)


def test_disabled_recipient_blocks_diagnostic_completion_delivery(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "disabled-recipient.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    with identity_harness.session_factory() as session:
        user = session.get(User, identity_harness.user_id)
        assert user is not None
        user.active = False
        session.commit()

    response = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )

    assert response.status_code == 409
    with identity_harness.session_factory() as session:
        job = session.get(DiagnosticJob, UUID(created["id"]))
        assert job is not None and job.status == "claimed"
        assert job.result_json is None
        assert session.scalar(select(EmailDelivery)) is None
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_removed_membership_blocks_export_completion_delivery(
    identity_harness: IdentityHarness,
) -> None:
    created = identity_harness.client.post(
        "/exports",
        headers=bff_headers(identity_harness),
        json={"format": "json"},
    ).json()
    claim = identity_harness.client.post(
        f"/internal/exports/{created['id']}/claim",
        headers={"X-SignalDesk-Service-Credential": EXPORT_WORKER_SERVICE_CREDENTIAL},
    ).json()
    with identity_harness.session_factory() as session:
        membership = session.get(
            Membership,
            (identity_harness.user_id, identity_harness.organization_id),
        )
        assert membership is not None
        session.delete(membership)
        session.commit()

    response = identity_harness.client.post(
        f"/internal/exports/{created['id']}/complete",
        headers={"X-SignalDesk-Service-Credential": EXPORT_WORKER_SERVICE_CREDENTIAL},
        json={
            "snapshot_id": claim["snapshot_id"],
            "object_key": claim["expected_object_key"],
            "object_sha256": "e" * 64,
            "size_bytes": 1,
        },
    )

    assert response.status_code == 409
    with identity_harness.session_factory() as session:
        assert session.scalar(select(EmailDelivery)) is None
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.parametrize("revocation", ["disabled", "membership"])
def test_email_worker_fetch_fails_closed_after_recipient_authorization_revoked(
    identity_harness: IdentityHarness,
    revocation: str,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "revoked-before-fetch.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    completed = identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/complete",
        headers=diagnostic_worker_headers(),
        json={"result": {"reachable": True}},
    )
    assert completed.status_code == 200
    with identity_harness.session_factory() as session:
        delivery = session.scalar(select(EmailDelivery))
        assert delivery is not None
        delivery_id = delivery.id
        if revocation == "disabled":
            user = session.get(User, identity_harness.user_id)
            assert user is not None
            user.active = False
        else:
            membership = session.get(
                Membership,
                (identity_harness.user_id, identity_harness.organization_id),
            )
            assert membership is not None
            session.delete(membership)
        session.commit()

    response = identity_harness.client.get(
        f"/internal/email-deliveries/{delivery_id}",
        headers=email_worker_headers(),
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Email delivery not found"}


@pytest.mark.parametrize("revocation", ["disabled", "membership"])
def test_completion_authorization_lock_serializes_concurrent_revocation(
    identity_harness: IdentityHarness,
    monkeypatch: pytest.MonkeyPatch,
    revocation: str,
) -> None:
    created = identity_harness.client.post(
        "/diagnostics",
        headers=bff_headers(identity_harness),
        json={"target": "concurrent-recipient-revocation.example.test"},
    ).json()
    identity_harness.client.post(
        f"/internal/diagnostics/{created['id']}/claim",
        headers=diagnostic_worker_headers(),
    )
    authorization_locked = Event()
    release_completion = Event()
    revocation_started = Event()
    original_add_email_event = email_delivery_service.add_email_event

    def block_after_authorization_lock(*args: object, **kwargs: object):
        authorization_locked.set()
        assert release_completion.wait(timeout=5)
        return original_add_email_event(*args, **kwargs)

    monkeypatch.setattr(
        email_delivery_service,
        "add_email_event",
        block_after_authorization_lock,
    )

    def complete():
        return identity_harness.client.post(
            f"/internal/diagnostics/{created['id']}/complete",
            headers=diagnostic_worker_headers(),
            json={"result": {"reachable": True}},
        )

    def revoke() -> None:
        with identity_harness.session_factory() as session:
            if revocation == "disabled":
                user = session.get(User, identity_harness.user_id)
                assert user is not None
                user.active = False
            else:
                membership = session.get(
                    Membership,
                    (identity_harness.user_id, identity_harness.organization_id),
                )
                assert membership is not None
                session.delete(membership)
            revocation_started.set()
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as executor:
        completion_future = executor.submit(complete)
        assert authorization_locked.wait(timeout=5)
        revocation_future = executor.submit(revoke)
        assert revocation_started.wait(timeout=5)
        try:
            with pytest.raises(FutureTimeoutError):
                revocation_future.result(timeout=0.25)
        finally:
            release_completion.set()
        completion_response = completion_future.result(timeout=5)
        revocation_future.result(timeout=5)

    assert completion_response.status_code == 200
    with identity_harness.session_factory() as session:
        delivery = session.scalar(select(EmailDelivery))
        assert delivery is not None
        delivery_id = delivery.id
    fetch = identity_harness.client.get(
        f"/internal/email-deliveries/{delivery_id}",
        headers=email_worker_headers(),
    )
    assert fetch.status_code == 404
