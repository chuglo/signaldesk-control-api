from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import signal
from threading import Event
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from signaldesk_contracts import parse_event_json
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from conftest import (
    EMAIL_WORKER_SERVICE_CREDENTIAL,
    EXPORT_WORKER_SERVICE_CREDENTIAL,
    IdentityHarness,
    SERVICE_CREDENTIAL,
    WORKER_SERVICE_CREDENTIAL,
)
from signaldesk_control_api import outbox_publisher as publisher
from signaldesk_control_api.models import OutboxEvent
from signaldesk_control_api.outbox_publisher import main as publisher_main
from signaldesk_control_api.outbox_publisher import publish_batch, run_forever
from signaldesk_control_api.settings import OutboxPublisherSettings, Settings


def add_outbox_event(
    session_factory: sessionmaker[Session],
    *,
    event_type: str,
    aggregate_field: str,
) -> OutboxEvent:
    event_id = uuid4()
    aggregate_id = uuid4()
    event = OutboxEvent(
        id=event_id,
        event_type=event_type,
        aggregate_id=aggregate_id,
        payload_json={
            "schema_version": 1,
            "event_id": str(event_id),
            "event_type": event_type,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "correlation_id": str(uuid4()),
            "organization_id": str(uuid4()),
            aggregate_field: str(aggregate_id),
        },
    )
    with session_factory() as session:
        session.add(event)
        session.commit()
    return event


def test_redis_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIGNALDESK_REDIS_URL", raising=False)

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+psycopg://user:pass@db/signaldesk",
            web_bff_service_credential=SERVICE_CREDENTIAL,
            diagnostic_worker_service_credential=WORKER_SERVICE_CREDENTIAL,
            export_worker_service_credential=EXPORT_WORKER_SERVICE_CREDENTIAL,
            email_worker_service_credential=EMAIL_WORKER_SERVICE_CREDENTIAL,
        )


def test_publisher_settings_do_not_require_or_expose_service_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "SIGNALDESK_WEB_BFF_SERVICE_CREDENTIAL",
        "SIGNALDESK_DIAGNOSTIC_WORKER_SERVICE_CREDENTIAL",
        "SIGNALDESK_EXPORT_WORKER_SERVICE_CREDENTIAL",
        "SIGNALDESK_EMAIL_WORKER_SERVICE_CREDENTIAL",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = OutboxPublisherSettings(
        database_url="postgresql+psycopg://user:pass@db/signaldesk",
        redis_url="redis://redis:6379/0",
    )

    assert settings.service_name == "signaldesk-outbox-publisher"
    assert not any("credential" in name for name in type(settings).model_fields)


@pytest.mark.parametrize(
    ("event_type", "aggregate_field", "stream"),
    [
        ("diagnostic.requested.v1", "diagnostic_job_id", "signaldesk:diagnostics"),
        (
            "diagnostic.completed.v1",
            "diagnostic_job_id",
            "signaldesk:diagnostic-completions",
        ),
        ("email.requested.v1", "email_delivery_id", "signaldesk:emails"),
        ("export.requested.v1", "export_job_id", "signaldesk:exports"),
        (
            "export.completed.v1",
            "export_job_id",
            "signaldesk:export-completions",
        ),
    ],
)
def test_supported_event_publishes_frozen_contract_to_explicit_stream(
    identity_harness: IdentityHarness,
    redis_client: Redis,
    event_type: str,
    aggregate_field: str,
    stream: str,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type=event_type,
        aggregate_field=aggregate_field,
    )

    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client, batch_size=10) == 1

    entries = redis_client.xrange(stream)
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert set(fields) == {"event", "event_id"}
    assert UUID(fields["event_id"]) == event.id
    parsed = parse_event_json(fields["event"])
    assert parsed.event_type == event_type
    assert parsed.event_id == event.id
    assert set(parsed.model_dump(mode="json")) == {
        "schema_version",
        "event_id",
        "event_type",
        "occurred_at",
        "correlation_id",
        "organization_id",
        aggregate_field,
    }
    for sensitive_field in (
        "target",
        "result",
        "recipient",
        "template",
        "object_key",
        "credentials",
    ):
        assert sensitive_field not in fields["event"]


def test_success_sets_timezone_aware_published_at(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    with identity_harness.session_factory() as session:
        publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        published = session.get(OutboxEvent, event.id)
        assert published is not None
        assert published.published_at is not None
        assert published.published_at.utcoffset() is not None


def test_unknown_event_type_is_contract_rejected_before_redis(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.unknown.v1",
        aggregate_field="diagnostic_job_id",
    )

    with identity_harness.session_factory() as session:
        with pytest.raises(ValidationError):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        unpublished = session.get(OutboxEvent, event.id)
        assert unpublished is not None
        assert unpublished.published_at is None
    assert list(redis_client.scan_iter(match="signaldesk:*")) == []


def test_malformed_sensitive_payload_remains_unpublished_and_stream_empty(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )
    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        stored.payload_json = {**stored.payload_json, "target": "must-not-publish.test"}
        session.commit()

    with identity_harness.session_factory() as session:
        with pytest.raises(ValidationError):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        unpublished = session.get(OutboxEvent, event.id)
        assert unpublished is not None
        assert unpublished.published_at is None
    assert redis_client.xlen("signaldesk:diagnostics") == 0
    assert list(redis_client.scan_iter(match="signaldesk:outbox:published:*")) == []


def test_redis_failure_rolls_back_and_leaves_event_retryable(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class FailingRedis:
        def info(self, section: str) -> dict[str, object]:
            return redis_client.info(section)

        def eval(self, *_args: object) -> object:
            raise RedisConnectionError("injected Redis outage")

    with identity_harness.session_factory() as session:
        with pytest.raises(RedisConnectionError, match="injected Redis outage"):
            publish_batch(session=session, redis_client=FailingRedis())  # type: ignore[arg-type]

    with identity_harness.session_factory() as session:
        unpublished = session.get(OutboxEvent, event.id)
        assert unpublished is not None
        assert unpublished.published_at is None
    assert redis_client.xlen("signaldesk:diagnostics") == 0


def test_cluster_enabled_redis_is_rejected_before_eval_or_database_mark(
    identity_harness: IdentityHarness,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class ClusterEnabledRedis:
        def __init__(self) -> None:
            self.eval_called = False

        def info(self, section: str) -> dict[str, object]:
            assert section == "cluster"
            return {"cluster_enabled": 1}

        def eval(self, *_args: object) -> object:
            self.eval_called = True
            return [1, "1-0"]

    cluster_redis = ClusterEnabledRedis()
    with identity_harness.session_factory() as session:
        with pytest.raises(RuntimeError, match="standalone Redis"):
            publish_batch(
                session=session,
                redis_client=cluster_redis,  # type: ignore[arg-type]
            )

    assert cluster_redis.eval_called is False
    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is None


def test_repeated_run_after_success_does_not_duplicate(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client) == 1
    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client) == 0

    assert redis_client.xlen("signaldesk:diagnostics") == 1


def test_retry_after_db_commit_failure_is_scheduled_and_uses_marker_without_xadd(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class CommitFailingOnceSession(Session):
        commit_calls = 0

        def commit(self) -> None:
            self.commit_calls += 1
            if self.commit_calls == 1:
                raise RuntimeError("injected PostgreSQL commit failure")
            super().commit()

    with CommitFailingOnceSession(bind=identity_harness.engine) as session:
        with pytest.raises(RuntimeError, match="injected PostgreSQL commit failure"):
            publish_batch(session=session, redis_client=redis_client)

    assert redis_client.xlen("signaldesk:diagnostics") == 1
    marker_key = f"signaldesk:outbox:published:{event.id}"
    first_stream_id = redis_client.get(marker_key)
    assert first_stream_id is not None
    with identity_harness.session_factory() as session:
        unpublished = session.get(OutboxEvent, event.id)
        assert unpublished is not None
        assert unpublished.published_at is None
        assert unpublished.attempt_count == 1
        assert unpublished.next_attempt_at is not None

    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client) == 0
        unpublished = session.get(OutboxEvent, event.id)
        assert unpublished is not None
        unpublished.next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()
    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client) == 1

    assert redis_client.xlen("signaldesk:diagnostics") == 1
    assert redis_client.get(marker_key) == first_stream_id
    with identity_harness.session_factory() as session:
        published = session.scalar(select(OutboxEvent).where(OutboxEvent.id == event.id))
        assert published is not None
        assert published.published_at is not None
        assert published.next_attempt_at is None


def test_retry_scheduling_failure_surfaces_original_without_persisting_secret(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class FailingRedis:
        def info(self, section: str) -> dict[str, object]:
            return redis_client.info(section)

        def eval(self, *_args: object) -> object:
            raise RedisConnectionError("original publication failure")

    class SchedulingCommitFailingSession(Session):
        def commit(self) -> None:
            raise RuntimeError("postgresql://user:scheduling-secret@db/signaldesk")

    with SchedulingCommitFailingSession(bind=identity_harness.engine) as session:
        with pytest.raises(
            RedisConnectionError, match="original publication failure"
        ):
            publish_batch(
                session=session,
                redis_client=FailingRedis(),  # type: ignore[arg-type]
                batch_size=1,
            )

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is None
        assert stored.attempt_count == 0
        assert stored.next_attempt_at is None
        assert "scheduling-secret" not in json.dumps(stored.payload_json)


def test_forged_marker_without_stream_entry_fails_closed(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )
    redis_client.set(
        f"signaldesk:outbox:published:{event.id}",
        "signaldesk:diagnostics|9999999999999-0",
    )

    with identity_harness.session_factory() as session:
        with pytest.raises(RuntimeError, match="missing_stream_entry"):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is None
    assert redis_client.xlen("signaldesk:diagnostics") == 0


def test_marker_pointing_to_mismatched_entry_fails_closed(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )
    stream_id = redis_client.xadd(
        "signaldesk:diagnostics",
        {"event": '{"forged":true}', "event_id": str(event.id)},
    )
    redis_client.set(
        f"signaldesk:outbox:published:{event.id}",
        f"signaldesk:diagnostics|{stream_id}",
    )

    with identity_harness.session_factory() as session:
        with pytest.raises(RuntimeError, match="payload_mismatch"):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is None
    assert redis_client.xlen("signaldesk:diagnostics") == 1


def test_deleted_stream_entry_after_redis_success_db_failure_fails_closed(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class CommitFailingSession(Session):
        def commit(self) -> None:
            raise RuntimeError("injected PostgreSQL commit failure")

    with CommitFailingSession(bind=identity_harness.engine) as session:
        with pytest.raises(RuntimeError, match="injected PostgreSQL commit failure"):
            publish_batch(session=session, redis_client=redis_client)

    stream_entries = redis_client.xrange("signaldesk:diagnostics")
    assert len(stream_entries) == 1
    redis_client.xdel("signaldesk:diagnostics", stream_entries[0][0])

    with identity_harness.session_factory() as session:
        with pytest.raises(RuntimeError, match="missing_stream_entry"):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is None
    assert redis_client.xlen("signaldesk:diagnostics") == 0


def test_valid_payload_mutation_after_crash_before_retry_fails_closed(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class CommitFailingSession(Session):
        def commit(self) -> None:
            raise RuntimeError("injected PostgreSQL commit failure")

    with CommitFailingSession(bind=identity_harness.engine) as session:
        with pytest.raises(RuntimeError, match="injected PostgreSQL commit failure"):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        stored.payload_json = {
            **stored.payload_json,
            "correlation_id": str(uuid4()),
        }
        session.commit()

    with identity_harness.session_factory() as session:
        with pytest.raises(RuntimeError, match="payload_mismatch"):
            publish_batch(session=session, redis_client=redis_client)

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is None
    assert redis_client.xlen("signaldesk:diagnostics") == 1


@pytest.mark.parametrize("batch_size", [0, -1, 101])
def test_batch_size_must_keep_claim_bounded(
    identity_harness: IdentityHarness,
    redis_client: Redis,
    batch_size: int,
) -> None:
    with identity_harness.session_factory() as session:
        with pytest.raises(ValueError, match="between 1 and 100"):
            publish_batch(
                session=session,
                redis_client=redis_client,
                batch_size=batch_size,
            )


def test_batch_limit_publishes_oldest_rows_in_deterministic_order(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    events = [
        add_outbox_event(
            identity_harness.session_factory,
            event_type="diagnostic.requested.v1",
            aggregate_field="diagnostic_job_id",
        )
        for _ in range(3)
    ]
    created_times = [
        datetime(2026, 1, 1, hour, tzinfo=timezone.utc) for hour in (1, 2, 3)
    ]
    with identity_harness.session_factory() as session:
        for event, created_at in zip(events, created_times, strict=True):
            stored = session.get(OutboxEvent, event.id)
            assert stored is not None
            stored.created_at = created_at
        session.commit()

    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client, batch_size=2) == 2

    stream_event_ids = [
        UUID(fields["event_id"])
        for _stream_id, fields in redis_client.xrange("signaldesk:diagnostics")
    ]
    assert stream_event_ids == [events[0].id, events[1].id]
    with identity_harness.session_factory() as session:
        stored_events = session.scalars(
            select(OutboxEvent).order_by(OutboxEvent.created_at, OutboxEvent.id)
        ).all()
        assert [event.published_at is not None for event in stored_events] == [
            True,
            True,
            False,
        ]


def test_batch_size_one_persists_poison_retry_and_next_session_publishes_valid(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    poison = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.unknown.v1",
        aggregate_field="diagnostic_job_id",
    )
    valid = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )
    ordered_events = [poison, valid]
    with identity_harness.session_factory() as session:
        for hour, event in enumerate(ordered_events, start=1):
            stored = session.get(OutboxEvent, event.id)
            assert stored is not None
            stored.created_at = datetime(2026, 1, 1, hour, tzinfo=timezone.utc)
        session.commit()

    before_failure = datetime.now(timezone.utc)
    with identity_harness.session_factory() as first_session:
        with pytest.raises(ValidationError):
            publish_batch(
                session=first_session,
                redis_client=redis_client,
                batch_size=1,
            )

    with identity_harness.session_factory() as session:
        stored_poison = session.get(OutboxEvent, poison.id)
        assert stored_poison is not None
        assert stored_poison.published_at is None
        assert stored_poison.attempt_count == 1
        assert stored_poison.next_attempt_at is not None
        assert stored_poison.next_attempt_at.utcoffset() is not None
        assert stored_poison.next_attempt_at > before_failure

    # A brand-new Session proves fairness is durable rather than process-local.
    with identity_harness.session_factory() as second_session:
        assert (
            publish_batch(
                session=second_session,
                redis_client=redis_client,
                batch_size=1,
            )
            == 1
        )

    with identity_harness.session_factory() as session:
        stored_poison = session.get(OutboxEvent, poison.id)
        stored_valid = session.get(OutboxEvent, valid.id)
        assert stored_poison is not None
        assert stored_valid is not None
        assert stored_poison.published_at is None
        assert stored_valid.published_at is not None
    assert redis_client.xlen("signaldesk:diagnostics") == 1


def test_more_poison_rows_than_batch_size_are_progressively_deferred(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    poison_events = [
        add_outbox_event(
            identity_harness.session_factory,
            event_type="diagnostic.unknown.v1",
            aggregate_field="diagnostic_job_id",
        )
        for _ in range(3)
    ]
    valid = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )
    with identity_harness.session_factory() as session:
        for hour, event in enumerate([*poison_events, valid], start=1):
            stored = session.get(OutboxEvent, event.id)
            assert stored is not None
            stored.created_at = datetime(2026, 1, 3, hour, tzinfo=timezone.utc)
        session.commit()

    with identity_harness.session_factory() as first_session:
        with pytest.raises(publisher.BatchPublishError) as first_error:
            publish_batch(
                session=first_session,
                redis_client=redis_client,
                batch_size=2,
            )
    assert first_error.value.failed_event_ids == tuple(
        event.id for event in poison_events[:2]
    )

    with identity_harness.session_factory() as second_session:
        with pytest.raises(publisher.BatchPublishError) as second_error:
            publish_batch(
                session=second_session,
                redis_client=redis_client,
                batch_size=2,
            )
    assert second_error.value.failed_event_ids == (poison_events[2].id,)

    with identity_harness.session_factory() as session:
        stored_poison = [session.get(OutboxEvent, event.id) for event in poison_events]
        stored_valid = session.get(OutboxEvent, valid.id)
        assert [event.attempt_count for event in stored_poison] == [1, 1, 1]
        assert all(event.next_attempt_at is not None for event in stored_poison)
        assert stored_valid is not None
        assert stored_valid.published_at is not None
    assert redis_client.xlen("signaldesk:diagnostics") == 1


def test_expired_retry_schedule_retries_poison_with_exponential_backoff(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    poison = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.unknown.v1",
        aggregate_field="diagnostic_job_id",
    )

    with identity_harness.session_factory() as session:
        with pytest.raises(ValidationError):
            publish_batch(session=session, redis_client=redis_client, batch_size=1)

    expired_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, poison.id)
        assert stored is not None
        stored.next_attempt_at = expired_at
        session.commit()

    before_retry = datetime.now(timezone.utc)
    with identity_harness.session_factory() as new_session:
        with pytest.raises(ValidationError):
            publish_batch(session=new_session, redis_client=redis_client, batch_size=1)

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, poison.id)
        assert stored is not None
        assert stored.attempt_count == 2
        assert stored.next_attempt_at is not None
        assert stored.next_attempt_at > before_retry + timedelta(seconds=1)


def test_successful_retry_clears_next_attempt_at(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    event = add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )

    class FailingRedis:
        def info(self, section: str) -> dict[str, object]:
            return redis_client.info(section)

        def eval(self, *_args: object) -> object:
            raise RedisConnectionError("redis://user:secret@example.test/0")

    with identity_harness.session_factory() as session:
        with pytest.raises(RedisConnectionError):
            publish_batch(
                session=session,
                redis_client=FailingRedis(),  # type: ignore[arg-type]
                batch_size=1,
            )

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.attempt_count == 1
        assert stored.next_attempt_at is not None
        stored.next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()

    with identity_harness.session_factory() as recovery_session:
        assert publish_batch(
            session=recovery_session,
            redis_client=redis_client,
            batch_size=1,
        ) == 1

    with identity_harness.session_factory() as session:
        stored = session.get(OutboxEvent, event.id)
        assert stored is not None
        assert stored.published_at is not None
        assert stored.attempt_count == 1
        assert stored.next_attempt_at is None
        assert set(stored.__table__.columns.keys()) == {
            "id",
            "event_type",
            "aggregate_id",
            "payload_json",
            "published_at",
            "attempt_count",
            "next_attempt_at",
            "created_at",
        }


def test_redis_failure_does_not_rollback_prior_mark_or_starve_later_rows(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    events = [
        add_outbox_event(
            identity_harness.session_factory,
            event_type="diagnostic.requested.v1",
            aggregate_field="diagnostic_job_id",
        )
        for _ in range(3)
    ]
    with identity_harness.session_factory() as session:
        for hour, event in enumerate(events, start=1):
            stored = session.get(OutboxEvent, event.id)
            assert stored is not None
            stored.created_at = datetime(2026, 1, 2, hour, tzinfo=timezone.utc)
        session.commit()

    class FailSecondPublicationRedis:
        def __init__(self) -> None:
            self.eval_calls = 0

        def info(self, section: str) -> dict[str, object]:
            return redis_client.info(section)

        def eval(self, *args: object) -> object:
            self.eval_calls += 1
            if self.eval_calls == 2:
                raise RedisConnectionError("injected second-event Redis outage")
            return redis_client.eval(*args)

    injected_redis = FailSecondPublicationRedis()
    with identity_harness.session_factory() as session:
        with pytest.raises(RuntimeError, match="failed.*1") as captured:
            publish_batch(
                session=session,
                redis_client=injected_redis,  # type: ignore[arg-type]
                batch_size=3,
            )
    assert isinstance(captured.value.__cause__, RedisConnectionError)
    assert injected_redis.eval_calls == 3

    with identity_harness.session_factory() as session:
        stored_events = [session.get(OutboxEvent, event.id) for event in events]
        assert [event.published_at is not None for event in stored_events] == [
            True,
            False,
            True,
        ]
    stream_event_ids = [
        UUID(fields["event_id"])
        for _stream_id, fields in redis_client.xrange("signaldesk:diagnostics")
    ]
    assert stream_event_ids == [events[0].id, events[2].id]

    with identity_harness.session_factory() as session:
        deferred = session.get(OutboxEvent, events[1].id)
        assert deferred is not None
        assert deferred.attempt_count == 1
        assert deferred.next_attempt_at is not None
        assert publish_batch(session=session, redis_client=redis_client, batch_size=3) == 0
        deferred.next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()
    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client, batch_size=3) == 1
    with identity_harness.session_factory() as session:
        assert publish_batch(session=session, redis_client=redis_client, batch_size=3) == 0
    assert redis_client.xlen("signaldesk:diagnostics") == 3


def test_concurrent_sessions_skip_locked_claim_without_double_publish(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    add_outbox_event(
        identity_harness.session_factory,
        event_type="diagnostic.requested.v1",
        aggregate_field="diagnostic_job_id",
    )
    first_claim_reached_redis = Event()
    release_first_publisher = Event()

    class BlockingRedis:
        def info(self, section: str) -> dict[str, object]:
            return redis_client.info(section)

        def eval(self, *args: object) -> object:
            first_claim_reached_redis.set()
            if not release_first_publisher.wait(timeout=10):
                raise TimeoutError("test did not release first publisher")
            return redis_client.eval(*args)

    def first_publisher() -> int:
        with identity_harness.session_factory() as session:
            return publish_batch(
                session=session,
                redis_client=BlockingRedis(),  # type: ignore[arg-type]
            )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(first_publisher)
        assert first_claim_reached_redis.wait(timeout=10)
        with identity_harness.session_factory() as session:
            second_count = publish_batch(session=session, redis_client=redis_client)
        release_first_publisher.set()
        first_count = future.result(timeout=10)

    assert (first_count, second_count) == (1, 0)
    assert redis_client.xlen("signaldesk:diagnostics") == 1


def test_cli_once_runs_one_batch_and_returns_without_polling(
    identity_harness: IdentityHarness,
    redis_client: Redis,
) -> None:
    add_outbox_event(
        identity_harness.session_factory,
        event_type="email.requested.v1",
        aggregate_field="email_delivery_id",
    )

    exit_code = publisher_main(
        ["--once"],
        session_factory=identity_harness.session_factory,
        redis_client=redis_client,
    )

    assert exit_code == 0
    assert redis_client.xlen("signaldesk:emails") == 1


def test_run_forever_exits_without_polling_when_stop_event_is_already_set() -> None:
    stop_event = Event()
    stop_event.set()

    run_forever(
        session_factory=object(),  # type: ignore[arg-type]
        redis_client=object(),  # type: ignore[arg-type]
        stop_event=stop_event,
    )


def test_run_forever_logs_failure_and_continues_with_event_aware_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingStopEvent:
        def __init__(self) -> None:
            self.stopped = False
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return self.stopped

        def wait(self, timeout: float) -> bool:
            self.waits.append(timeout)
            return self.stopped

    stop_event = RecordingStopEvent()
    calls = 0

    def fake_run_once(**_kwargs: object) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected bounded batch failure")
        stop_event.stopped = True
        return 0

    logged_messages: list[str] = []
    monkeypatch.setattr(publisher, "run_once", fake_run_once)
    monkeypatch.setattr(
        publisher.logger,
        "exception",
        lambda message: logged_messages.append(message),
    )
    run_forever(
        session_factory=object(),  # type: ignore[arg-type]
        redis_client=object(),  # type: ignore[arg-type]
        poll_interval=0.1,
        stop_event=stop_event,  # type: ignore[arg-type]
    )

    assert calls == 2
    assert stop_event.waits == [0.1, 0.1]
    assert logged_messages == ["Outbox publication batch failed"]


def test_default_cli_sigterm_handler_restores_and_closes_owned_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = OutboxPublisherSettings(
        database_url="postgresql+psycopg://user:pass@db/signaldesk",
        redis_url="redis://redis:6379/0",
    )

    class FakeEngine:
        def __init__(self) -> None:
            self.disposed = False

        def dispose(self) -> None:
            self.disposed = True

    class FakeRedisClient:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    engine = FakeEngine()
    redis_client = FakeRedisClient()
    fake_session_factory = object()

    class FakeRedisFactory:
        @staticmethod
        def from_url(*_args: object, **_kwargs: object) -> FakeRedisClient:
            return redis_client

    monkeypatch.setattr(publisher, "Redis", FakeRedisFactory)
    monkeypatch.setattr(publisher, "create_engine", lambda _settings: engine)
    monkeypatch.setattr(
        publisher,
        "create_session_factory",
        lambda supplied_engine: (
            fake_session_factory
            if supplied_engine is engine
            else pytest.fail("unexpected engine")
        ),
    )

    original_handler = signal.getsignal(signal.SIGTERM)
    handler_observations: list[bool] = []

    def fake_run_forever(*, stop_event: Event, **_kwargs: object) -> None:
        installed_handler = signal.getsignal(signal.SIGTERM)
        assert callable(installed_handler)
        installed_handler(signal.SIGTERM, None)
        handler_observations.append(stop_event.is_set())

    monkeypatch.setattr(publisher, "run_forever", fake_run_forever)

    assert publisher_main([], settings=settings) == 0
    assert handler_observations == [True]
    assert signal.getsignal(signal.SIGTERM) is original_handler
    assert redis_client.closed is True
    assert engine.disposed is True
