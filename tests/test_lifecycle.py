from unittest.mock import Mock

from fastapi.testclient import TestClient

from conftest import (
    EMAIL_WORKER_SERVICE_CREDENTIAL,
    EXPORT_WORKER_SERVICE_CREDENTIAL,
    REDIS_URL,
    SERVICE_CREDENTIAL,
    WORKER_SERVICE_CREDENTIAL,
)
from signaldesk_control_api import main
from signaldesk_control_api.settings import Settings


def test_app_owns_one_engine_and_disposes_it_at_shutdown(
    monkeypatch,
    postgres_url: str,
) -> None:
    engine = Mock()
    session_factory = Mock()
    create_engine = Mock(return_value=engine)
    create_session_factory = Mock(return_value=session_factory)
    monkeypatch.setattr(main, "create_engine", create_engine)
    monkeypatch.setattr(main, "create_session_factory", create_session_factory)

    app = main.create_app(
        settings=Settings(
            database_url=postgres_url,
            redis_url=REDIS_URL,
            web_bff_service_credential=SERVICE_CREDENTIAL,
            diagnostic_worker_service_credential=WORKER_SERVICE_CREDENTIAL,
            export_worker_service_credential=EXPORT_WORKER_SERVICE_CREDENTIAL,
            email_worker_service_credential=EMAIL_WORKER_SERVICE_CREDENTIAL,
        )
    )

    create_engine.assert_called_once()
    create_session_factory.assert_called_once_with(engine)
    assert app.state.engine is engine
    assert app.state.session_factory is session_factory

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    engine.dispose.assert_called_once_with()


def test_unconfigured_app_keeps_health_open_but_internal_database_closed() -> None:
    client = TestClient(main.create_app(), raise_server_exceptions=False)

    assert client.get("/healthz").status_code == 200
    response = client.get(
        "/internal/tenant-context",
        headers={
            "X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL,
            "X-SignalDesk-User-ID": "00000000-0000-4000-8000-000000000001",
            "X-SignalDesk-Organization-ID": "00000000-0000-4000-8000-000000000002",
        },
    )

    assert response.status_code == 503
