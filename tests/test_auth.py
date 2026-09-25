from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from conftest import (
    EMAIL_WORKER_SERVICE_CREDENTIAL,
    EXPORT_WORKER_SERVICE_CREDENTIAL,
    IdentityHarness,
    REDIS_URL,
    SERVICE_CREDENTIAL,
    USER_PASSWORD,
    WORKER_SERVICE_CREDENTIAL,
)
from signaldesk_control_api.auth import (
    require_diagnostic_worker_service,
    require_export_worker_service,
    require_web_bff_service,
)
from signaldesk_control_api.models import Organization, User
from signaldesk_control_api.settings import Settings


def authentication_request(
    harness: IdentityHarness,
    *,
    email: str = "user@example.test",
    password: str = USER_PASSWORD,
    organization_id: UUID | None = None,
    service_credential: str | None = SERVICE_CREDENTIAL,
):
    headers = {}
    if service_credential is not None:
        headers["X-SignalDesk-Service-Credential"] = service_credential
    return harness.client.post(
        "/internal/authenticate",
        headers=headers,
        json={
            "email": email,
            "password": password,
            "organization_id": str(organization_id or harness.organization_id),
        },
    )


def all_service_credentials(**overrides: str) -> dict[str, str]:
    credentials = {
        "redis_url": REDIS_URL,
        "web_bff_service_credential": SERVICE_CREDENTIAL,
        "diagnostic_worker_service_credential": WORKER_SERVICE_CREDENTIAL,
        "export_worker_service_credential": EXPORT_WORKER_SERVICE_CREDENTIAL,
        "email_worker_service_credential": EMAIL_WORKER_SERVICE_CREDENTIAL,
    }
    credentials.update(overrides)
    return credentials


def test_export_worker_service_credential_is_required(postgres_url: str) -> None:
    credentials = all_service_credentials()
    del credentials["export_worker_service_credential"]

    with pytest.raises(ValidationError):
        Settings(database_url=postgres_url, **credentials)


def test_all_service_credentials_must_be_pairwise_distinct(postgres_url: str) -> None:
    with pytest.raises(ValidationError, match="distinct"):
        Settings(
            database_url=postgres_url,
            **all_service_credentials(
                email_worker_service_credential=EXPORT_WORKER_SERVICE_CREDENTIAL
            ),
        )


@pytest.mark.parametrize("field", ["export_worker_service_credential", "email_worker_service_credential"])
@pytest.mark.parametrize("credential", ["", " " * 40, "too-short", "é" * 32])
def test_new_worker_credentials_use_strong_ascii_invariant(
    postgres_url: str, field: str, credential: str
) -> None:
    with pytest.raises(ValidationError):
        Settings(
            database_url=postgres_url,
            **all_service_credentials(**{field: credential}),
        )


def test_web_bff_service_credential_is_required(postgres_url: str) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=postgres_url)


def test_diagnostic_worker_service_credential_is_required(postgres_url: str) -> None:
    credentials = all_service_credentials()
    del credentials["diagnostic_worker_service_credential"]
    with pytest.raises(ValidationError):
        Settings(database_url=postgres_url, **credentials)


def test_bff_and_worker_service_credentials_must_be_distinct(
    postgres_url: str,
) -> None:
    with pytest.raises(ValidationError, match="distinct"):
        Settings(
            database_url=postgres_url,
            **all_service_credentials(
                diagnostic_worker_service_credential=SERVICE_CREDENTIAL
            ),
        )


@pytest.mark.parametrize("credential", ["", " " * 40, "too-short", "é" * 32])
def test_diagnostic_worker_credential_uses_strong_ascii_invariant(
    postgres_url: str,
    credential: str,
) -> None:
    with pytest.raises(ValidationError):
        Settings(
            database_url=postgres_url,
            **all_service_credentials(
                diagnostic_worker_service_credential=credential
            ),
        )


@pytest.mark.parametrize("credential", ["", " " * 40, "too-short"])
def test_web_bff_service_credential_must_be_nonblank_and_long(
    postgres_url: str,
    credential: str,
) -> None:
    with pytest.raises(ValidationError, match="at least 32"):
        Settings(
            database_url=postgres_url,
            **all_service_credentials(web_bff_service_credential=credential),
        )


def test_web_bff_service_credential_must_be_ascii(postgres_url: str) -> None:
    with pytest.raises(ValidationError, match="ASCII"):
        Settings(
            database_url=postgres_url,
            **all_service_credentials(web_bff_service_credential="é" * 32),
        )


def test_non_ascii_presented_service_credential_fails_closed(
    postgres_url: str,
) -> None:
    settings = Settings(
        database_url=postgres_url,
        **all_service_credentials(),
    )

    with pytest.raises(HTTPException) as exc_info:
        require_web_bff_service(settings=settings, credential="é" * 32)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid service credential"


def test_non_ascii_presented_worker_credential_fails_closed(
    postgres_url: str,
) -> None:
    settings = Settings(
        database_url=postgres_url,
        **all_service_credentials(),
    )

    with pytest.raises(HTTPException) as exc_info:
        require_diagnostic_worker_service(settings=settings, credential="é" * 32)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid service credential"


def test_malformed_worker_credential_is_rejected_before_constant_time_compare(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(database_url=postgres_url, **all_service_credentials())

    def fail_compare(*_args: object) -> bool:
        raise AssertionError("comparator must not receive malformed credentials")

    monkeypatch.setattr("signaldesk_control_api.auth.secrets.compare_digest", fail_compare)
    with pytest.raises(HTTPException) as exc_info:
        require_export_worker_service(settings=settings, credential="too-short")

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid service credential"


def test_user_email_is_normalized() -> None:
    user = User(
        email="  USER@EXAMPLE.TEST ",
        password_hash="synthetic-password-hash",
        active=True,
    )

    assert user.email == "user@example.test"


def test_normalized_user_email_is_unique(
    identity_harness: IdentityHarness,
) -> None:
    duplicate = User(
        email="  USER@EXAMPLE.TEST ",
        password_hash="synthetic-password-hash",
        active=True,
    )

    with identity_harness.session_factory() as session:
        session.add(duplicate)
        with pytest.raises(IntegrityError):
            session.commit()


def test_seeded_user_authenticates(identity_harness: IdentityHarness) -> None:
    response = authentication_request(
        identity_harness,
        email="  USER@EXAMPLE.TEST ",
    )

    assert response.status_code == 200
    assert response.json() == {
        "user_id": str(identity_harness.user_id),
        "organization_id": str(identity_harness.organization_id),
        "role": "owner",
    }


def test_wrong_email_and_password_have_same_failure(
    identity_harness: IdentityHarness,
) -> None:
    wrong_email = authentication_request(
        identity_harness,
        email="missing@example.test",
    )
    wrong_password = authentication_request(
        identity_harness,
        password="not-the-password",
    )

    assert wrong_email.status_code == wrong_password.status_code == 401
    assert wrong_email.json() == wrong_password.json() == {"detail": "Invalid credentials"}


def test_disabled_user_has_same_invalid_credentials_failure(
    identity_harness: IdentityHarness,
) -> None:
    with identity_harness.session_factory() as session:
        user = session.get(User, identity_harness.user_id)
        assert user is not None
        user.active = False
        session.commit()

    response = authentication_request(identity_harness)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid credentials"}


def test_missing_and_wrong_service_credential_are_rejected(
    identity_harness: IdentityHarness,
) -> None:
    missing = authentication_request(identity_harness, service_credential=None)
    blank = authentication_request(identity_harness, service_credential="")
    wrong = authentication_request(
        identity_harness,
        service_credential="wrong-synthetic-service-credential",
    )

    assert missing.status_code == blank.status_code == wrong.status_code == 401
    assert missing.json() == blank.json() == wrong.json() == {
        "detail": "Invalid service credential"
    }


def test_user_cannot_select_non_member_organization(
    identity_harness: IdentityHarness,
) -> None:
    other_organization = Organization(id=uuid4(), name="Other Organization")
    with identity_harness.session_factory() as session:
        session.add(other_organization)
        session.commit()

    response = authentication_request(
        identity_harness,
        organization_id=other_organization.id,
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Organization membership required"}
