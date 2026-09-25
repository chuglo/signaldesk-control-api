from uuid import uuid4

import pytest

from conftest import IdentityHarness, SERVICE_CREDENTIAL
from signaldesk_control_api.models import Membership, Organization, User


def tenant_context_request(
    harness: IdentityHarness,
    *,
    user_id: str | None = None,
    organization_id: str | None = None,
):
    return harness.client.get(
        "/internal/tenant-context",
        headers={
            "X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL,
            "X-SignalDesk-User-ID": user_id or str(harness.user_id),
            "X-SignalDesk-Organization-ID": (
                organization_id or str(harness.organization_id)
            ),
        },
    )


def test_valid_membership_assertions_return_authoritative_context(
    identity_harness: IdentityHarness,
) -> None:
    response = tenant_context_request(identity_harness)

    assert response.status_code == 200
    assert response.json() == {
        "user_id": str(identity_harness.user_id),
        "organization_id": str(identity_harness.organization_id),
        "role": "owner",
    }


@pytest.mark.parametrize("service_credential", [None, "", "wrong-service-credential"])
def test_tenant_context_rejects_missing_blank_or_wrong_service_credential(
    identity_harness: IdentityHarness,
    service_credential: str | None,
) -> None:
    headers = {
        "X-SignalDesk-User-ID": str(identity_harness.user_id),
        "X-SignalDesk-Organization-ID": str(identity_harness.organization_id),
    }
    if service_credential is not None:
        headers["X-SignalDesk-Service-Credential"] = service_credential

    response = identity_harness.client.get(
        "/internal/tenant-context",
        headers=headers,
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid service credential"}


def test_asserted_organization_cannot_override_database_membership(
    identity_harness: IdentityHarness,
) -> None:
    other_organization = Organization(id=uuid4(), name="Asserted Organization")
    with identity_harness.session_factory() as session:
        session.add(other_organization)
        session.commit()

    response = tenant_context_request(
        identity_harness,
        organization_id=str(other_organization.id),
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Authoritative tenant context required"}


def test_disabled_asserted_user_is_rejected(
    identity_harness: IdentityHarness,
) -> None:
    with identity_harness.session_factory() as session:
        user = session.get(User, identity_harness.user_id)
        assert user is not None
        user.active = False
        session.commit()

    response = tenant_context_request(identity_harness)

    assert response.status_code == 403
    assert response.json() == {"detail": "Authoritative tenant context required"}


def test_nonexistent_asserted_user_is_rejected(
    identity_harness: IdentityHarness,
) -> None:
    response = tenant_context_request(
        identity_harness,
        user_id=str(uuid4()),
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Authoritative tenant context required"}


def test_missing_identity_assertion_headers_are_rejected(
    identity_harness: IdentityHarness,
) -> None:
    response = identity_harness.client.get(
        "/internal/tenant-context",
        headers={"X-SignalDesk-Service-Credential": SERVICE_CREDENTIAL},
    )

    assert response.status_code == 422


def test_cross_user_cross_organization_assertion_is_rejected(
    identity_harness: IdentityHarness,
) -> None:
    other_user = User(
        id=uuid4(),
        email="other@example.test",
        password_hash="synthetic-password-hash",
        active=True,
    )
    other_organization = Organization(id=uuid4(), name="Other Tenant")
    with identity_harness.session_factory() as session:
        session.add_all([other_user, other_organization])
        session.flush()
        session.add(
            Membership(
                user_id=other_user.id,
                organization_id=other_organization.id,
                role="member",
            )
        )
        session.commit()

    response = tenant_context_request(
        identity_harness,
        user_id=str(other_user.id),
        organization_id=str(identity_harness.organization_id),
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Authoritative tenant context required"}
