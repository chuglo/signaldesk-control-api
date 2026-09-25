import pytest
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import InvalidRequestError

from conftest import (
    EMAIL_WORKER_SERVICE_CREDENTIAL,
    EXPORT_WORKER_SERVICE_CREDENTIAL,
    REDIS_URL,
    SERVICE_CREDENTIAL,
    WORKER_SERVICE_CREDENTIAL,
)
from signaldesk_control_api.database import create_engine, create_session_factory, session_scope
from signaldesk_control_api.settings import Settings


def test_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIGNALDESK_DATABASE_URL", raising=False)

    with pytest.raises(ValidationError):
        Settings()


def test_session_executes_postgresql_query_and_closes(postgres_url: str) -> None:
    engine = create_engine(
        Settings(
            database_url=postgres_url,
            redis_url=REDIS_URL,
            web_bff_service_credential=SERVICE_CREDENTIAL,
            diagnostic_worker_service_credential=WORKER_SERVICE_CREDENTIAL,
            export_worker_service_credential=EXPORT_WORKER_SERVICE_CREDENTIAL,
            email_worker_service_credential=EMAIL_WORKER_SERVICE_CREDENTIAL,
        )
    )
    session_factory = create_session_factory(engine)

    with session_scope(session_factory) as session:
        assert session.scalar(text("SELECT 1")) == 1

    with pytest.raises(InvalidRequestError, match="permanently closed"):
        session.scalar(text("SELECT 1"))

    engine.dispose()


def test_session_scope_rolls_back_when_work_raises(postgres_url: str) -> None:
    engine = create_engine(Settings(
        database_url=postgres_url,
        redis_url=REDIS_URL,
        web_bff_service_credential=SERVICE_CREDENTIAL,
        diagnostic_worker_service_credential=WORKER_SERVICE_CREDENTIAL,
        export_worker_service_credential=EXPORT_WORKER_SERVICE_CREDENTIAL,
        email_worker_service_credential=EMAIL_WORKER_SERVICE_CREDENTIAL,
    ))
    session_factory = create_session_factory(engine)

    try:
        with engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE session_scope_probe (value INTEGER NOT NULL)")
            )

        with pytest.raises(RuntimeError, match="force rollback"):
            with session_scope(session_factory) as session:
                session.execute(
                    text("INSERT INTO session_scope_probe (value) VALUES (1)")
                )
                raise RuntimeError("force rollback")

        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM session_scope_probe")) == 0
    finally:
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS session_scope_probe"))
        engine.dispose()
