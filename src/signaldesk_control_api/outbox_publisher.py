"""Publish validated transaction-outbox rows to Redis Streams."""

import argparse
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
import json
import logging
import signal
from threading import Event
from uuid import UUID

from redis import Redis
from signaldesk_contracts import REDIS_STREAM_BY_EVENT_TYPE, parse_event_json
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from signaldesk_control_api.database import (
    SessionFactory,
    create_engine,
    create_session_factory,
)
from signaldesk_control_api.models import OutboxEvent
from signaldesk_control_api.settings import OutboxPublisherSettings


logger = logging.getLogger(__name__)

_RETRY_BASE_SECONDS = 30
_RETRY_MAX_SECONDS = 300

_AGGREGATE_FIELD_BY_EVENT_TYPE = {
    "diagnostic.requested.v1": "diagnostic_job_id",
    "diagnostic.completed.v1": "diagnostic_job_id",
    "email.requested.v1": "email_delivery_id",
    "export.requested.v1": "export_job_id",
    "export.completed.v1": "export_job_id",
}

_PUBLISH_ONCE_LUA = """
local prior_marker = redis.call('GET', KEYS[1])
if prior_marker then
    local separator = string.find(prior_marker, '|', 1, true)
    if not separator or string.find(prior_marker, '|', separator + 1, true) then
        return {-1, 'malformed_marker'}
    end
    local marker_stream = string.sub(prior_marker, 1, separator - 1)
    local prior_id = string.sub(prior_marker, separator + 1)
    if marker_stream == '' or not string.match(prior_id, '^%d+%-%d+$') then
        return {-1, 'malformed_marker'}
    end
    if marker_stream ~= KEYS[2] then
        return {-2, 'wrong_stream'}
    end

    local entries = redis.call('XRANGE', KEYS[2], prior_id, prior_id)
    if #entries ~= 1 or entries[1][1] ~= prior_id then
        return {-3, 'missing_stream_entry'}
    end
    local fields = entries[1][2]
    local stored_event = nil
    local stored_event_id = nil
    local event_fields = 0
    local event_id_fields = 0
    for index = 1, #fields, 2 do
        if fields[index] == 'event' then
            stored_event = fields[index + 1]
            event_fields = event_fields + 1
        elseif fields[index] == 'event_id' then
            stored_event_id = fields[index + 1]
            event_id_fields = event_id_fields + 1
        end
    end
    if event_fields ~= 1 or event_id_fields ~= 1 then
        return {-4, 'malformed_stream_entry'}
    end
    if stored_event_id ~= ARGV[2] then
        return {-5, 'event_id_mismatch'}
    end
    if stored_event ~= ARGV[1] then
        return {-6, 'payload_mismatch'}
    end
    return {0, prior_id}
end
local stream_id = redis.call(
    'XADD', KEYS[2], '*',
    'event', ARGV[1],
    'event_id', ARGV[2]
)
redis.call('SET', KEYS[1], KEYS[2] .. '|' .. stream_id)
return {1, stream_id}
"""


class BatchPublishError(RuntimeError):
    """Report failed rows after all bounded publication attempts complete."""

    def __init__(self, failed_event_ids: Sequence[UUID]) -> None:
        self.failed_event_ids = tuple(failed_event_ids)
        self.failure_count = len(self.failed_event_ids)
        joined_ids = ", ".join(str(event_id) for event_id in self.failed_event_ids)
        super().__init__(
            f"outbox batch failed for {self.failure_count} event(s): {joined_ids}"
        )


def _require_standalone_redis(redis_client: Redis) -> None:
    cluster_info = redis_client.info("cluster")
    if not isinstance(cluster_info, dict):
        raise RuntimeError("could not verify standalone Redis topology")
    cluster_enabled = cluster_info.get("cluster_enabled")
    if cluster_enabled in (0, "0", False):
        return
    if cluster_enabled in (1, "1", True):
        raise RuntimeError("outbox publication requires standalone Redis")
    raise RuntimeError("could not verify standalone Redis topology")


def _canonical_event(event: OutboxEvent) -> str:
    raw_json = json.dumps(event.payload_json, separators=(",", ":"), sort_keys=True)
    parsed = parse_event_json(raw_json)
    if parsed.event_id != event.id or parsed.event_type != event.event_type:
        raise ValueError("outbox metadata does not match the event contract")
    aggregate_id = getattr(parsed, _AGGREGATE_FIELD_BY_EVENT_TYPE[parsed.event_type])
    if aggregate_id != event.aggregate_id:
        raise ValueError("outbox aggregate does not match the event contract")
    return json.dumps(
        parsed.model_dump(mode="json"),
        separators=(",", ":"),
        sort_keys=True,
    )


def _retry_delay(attempt_count: int) -> timedelta:
    delay_seconds = min(
        _RETRY_MAX_SECONDS,
        _RETRY_BASE_SECONDS * (2 ** min(attempt_count - 1, 30)),
    )
    return timedelta(seconds=delay_seconds)


def _schedule_retry(*, session: Session, event_id: UUID) -> bool:
    """Persist retry eligibility in a new transaction after publication rollback."""

    try:
        event = session.scalar(
            select(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.published_at.is_(None),
            )
            .with_for_update()
        )
        if event is None:
            session.rollback()
            return True
        event.attempt_count += 1
        database_now = session.scalar(select(func.now()))
        if database_now is None:
            raise RuntimeError("could not read database time for outbox retry")
        event.next_attempt_at = database_now + _retry_delay(event.attempt_count)
        session.commit()
        return True
    except Exception:
        logger.exception(
            "Failed to schedule outbox publication retry",
            extra={"event_id": str(event_id)},
        )
        try:
            session.rollback()
        except Exception:
            logger.exception(
                "Failed to roll back outbox retry scheduling transaction",
                extra={"event_id": str(event_id)},
            )
        return False


def publish_batch(*, session: Session, redis_client: Redis, batch_size: int = 100) -> int:
    """Attempt one bounded batch with an independent transaction per row."""

    if not 1 <= batch_size <= 100:
        raise ValueError("batch_size must be between 1 and 100")
    _require_standalone_redis(redis_client)

    failures: list[tuple[UUID, Exception]] = []
    published_count = 0
    attempted_count = 0
    attempted_ids: set[UUID] = set()

    for _attempt in range(batch_size):
        try:
            statement = (
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),
                    or_(
                        OutboxEvent.next_attempt_at.is_(None),
                        OutboxEvent.next_attempt_at <= func.now(),
                    ),
                )
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if attempted_ids:
                statement = statement.where(OutboxEvent.id.not_in(attempted_ids))
            event = session.scalar(statement)
        except Exception:
            session.rollback()
            raise

        if event is None:
            session.rollback()
            break

        event_id = event.id
        attempted_count += 1
        attempted_ids.add(event_id)
        try:
            canonical = _canonical_event(event)
            stream = REDIS_STREAM_BY_EVENT_TYPE[event.event_type]
            marker_key = f"signaldesk:outbox:published:{event.id}"
            result = redis_client.eval(
                _PUBLISH_ONCE_LUA,
                2,
                marker_key,
                stream,
                canonical,
                str(event.id),
            )
            if not isinstance(result, (list, tuple)) or len(result) != 2:
                raise RuntimeError("Redis did not confirm outbox publication")
            try:
                publication_status = int(result[0])
            except (TypeError, ValueError) as error:
                raise RuntimeError(
                    "Redis returned an invalid outbox publication status"
                ) from error
            if publication_status < 0:
                reason = result[1]
                if isinstance(reason, bytes):
                    reason = reason.decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Redis publication marker verification failed: {reason}"
                )
            if publication_status not in (0, 1) or not result[1]:
                raise RuntimeError("Redis did not confirm outbox publication")
            event.published_at = datetime.now(timezone.utc)
            event.next_attempt_at = None
            session.commit()
            published_count += 1
        except Exception as error:
            failures.append((event_id, error))
            try:
                session.rollback()
            except Exception:
                logger.exception(
                    "Failed to roll back outbox publication transaction",
                    extra={"event_id": str(event_id)},
                )
                break
            if not _schedule_retry(session=session, event_id=event_id):
                break

    if failures:
        first_error = failures[0][1]
        if attempted_count == 1 and published_count == 0:
            raise first_error
        raise BatchPublishError([event_id for event_id, _error in failures]) from first_error
    return published_count


def run_once(
    *,
    session_factory: SessionFactory,
    redis_client: Redis,
    batch_size: int = 100,
) -> int:
    """Open one database session and publish at most one batch."""

    with session_factory() as session:
        return publish_batch(
            session=session,
            redis_client=redis_client,
            batch_size=batch_size,
        )


def run_forever(
    *,
    session_factory: SessionFactory,
    redis_client: Redis,
    batch_size: int = 100,
    poll_interval: float = 1.0,
    stop_event: Event | None = None,
) -> None:
    """Poll until interrupted, logging bounded failures before retrying."""

    if not 0.1 <= poll_interval <= 60:
        raise ValueError("poll_interval must be between 0.1 and 60 seconds")
    stop_event = stop_event or Event()
    while not stop_event.is_set():
        try:
            published = run_once(
                session_factory=session_factory,
                redis_client=redis_client,
                batch_size=batch_size,
            )
        except Exception:
            logger.exception("Outbox publication batch failed")
            stop_event.wait(poll_interval)
            continue
        if published < batch_size:
            stop_event.wait(poll_interval)


def main(
    argv: Sequence[str] | None = None,
    *,
    settings: OutboxPublisherSettings | None = None,
    session_factory: SessionFactory | None = None,
    redis_client: Redis | None = None,
) -> int:
    """Run one batch with ``--once`` or poll continuously by default."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="publish one batch and exit")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    args = parser.parse_args(argv)

    owned_engine = None
    owned_redis = None
    if session_factory is None or redis_client is None:
        settings = settings or OutboxPublisherSettings()  # type: ignore[call-arg]
    if session_factory is None:
        assert settings is not None
        owned_engine = create_engine(settings)
        session_factory = create_session_factory(owned_engine)
    if redis_client is None:
        assert settings is not None
        owned_redis = Redis.from_url(
            settings.redis_url.unicode_string(),
            decode_responses=True,
        )
        redis_client = owned_redis

    stop_event = Event()
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)
    sigterm_handler_installed = False
    try:
        if args.once:
            run_once(
                session_factory=session_factory,
                redis_client=redis_client,
                batch_size=args.batch_size,
            )
        else:
            def handle_sigterm(_signum: int, _frame: object) -> None:
                stop_event.set()

            signal.signal(signal.SIGTERM, handle_sigterm)
            sigterm_handler_installed = True
            run_forever(
                session_factory=session_factory,
                redis_client=redis_client,
                batch_size=args.batch_size,
                poll_interval=args.poll_interval,
                stop_event=stop_event,
            )
    except KeyboardInterrupt:
        return 0
    finally:
        try:
            if sigterm_handler_installed:
                signal.signal(signal.SIGTERM, previous_sigterm_handler)
        finally:
            try:
                if owned_redis is not None:
                    owned_redis.close()
            finally:
                if owned_engine is not None:
                    owned_engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
