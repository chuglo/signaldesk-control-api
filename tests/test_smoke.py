from signaldesk_control_api.settings import Settings


def test_package_imports() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://user:pass@db/signaldesk",
        redis_url="redis://redis.test:6379/0",
        web_bff_service_credential="synthetic-web-bff-credential-for-tests-only",
        diagnostic_worker_service_credential=(
            "synthetic-diagnostic-worker-credential-tests-only"
        ),
        export_worker_service_credential=(
            "synthetic-export-worker-credential-for-tests-only"
        ),
        email_worker_service_credential=(
            "synthetic-email-worker-credential-for-tests-only"
        ),
    )

    assert settings.service_name == "signaldesk-control-api"
