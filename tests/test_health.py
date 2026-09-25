from fastapi.testclient import TestClient

from signaldesk_control_api.main import app


def test_healthz_reports_service_is_ok() -> None:
    response = TestClient(app).get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
