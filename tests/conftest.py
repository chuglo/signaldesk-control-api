from collections.abc import Iterator
from dataclasses import dataclass
import os
from uuid import UUID, uuid4

import pytest

# The fixture's context manager owns cleanup, so a separate Ryuk container is
# unnecessary and would introduce a second public image dependency.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

from fastapi.testclient import TestClient  # noqa: E402
from redis import Redis  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402
from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.wait_strategies import LogMessageWaitStrategy  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402
from werkzeug.security import generate_password_hash  # noqa: E402

from signaldesk_control_api.main import create_app  # noqa: E402
from signaldesk_control_api.models import (  # noqa: E402
    Base,
    Membership,
    Organization,
    User,
)
from signaldesk_control_api.settings import Settings  # noqa: E402

POSTGRES_IMAGE = (
    "postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
)
REDIS_IMAGE = (
    "redis@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"
)
REDIS_URL = "redis://redis.test:6379/0"
SERVICE_CREDENTIAL = "synthetic-web-bff-credential-for-tests-only"
WORKER_SERVICE_CREDENTIAL = "synthetic-diagnostic-worker-credential-tests-only"
EXPORT_WORKER_SERVICE_CREDENTIAL = "synthetic-export-worker-credential-for-tests-only"
EMAIL_WORKER_SERVICE_CREDENTIAL = "synthetic-email-worker-credential-for-tests-only"
USER_PASSWORD = "synthetic-user-password"


@dataclass(frozen=True)
class IdentityHarness:
    client: TestClient
    engine: Engine
    session_factory: sessionmaker[Session]
    user_id: UUID
    organization_id: UUID


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture(scope="session")
def redis_container() -> Iterator[DockerContainer]:
    container = (
        DockerContainer(REDIS_IMAGE)
        .with_exposed_ports(6379)
        .waiting_for(LogMessageWaitStrategy("Ready to accept connections"))
    )
    with container:
        yield container


@pytest.fixture
def redis_client(redis_container: DockerContainer) -> Iterator[Redis]:
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    client.flushdb()
    try:
        yield client
    finally:
        client.flushdb()
        client.close()


@pytest.fixture
def identity_harness(postgres_url: str) -> Iterator[IdentityHarness]:
    settings = Settings(
        database_url=postgres_url,
        redis_url=REDIS_URL,
        web_bff_service_credential=SERVICE_CREDENTIAL,
        diagnostic_worker_service_credential=WORKER_SERVICE_CREDENTIAL,
        export_worker_service_credential=EXPORT_WORKER_SERVICE_CREDENTIAL,
        email_worker_service_credential=EMAIL_WORKER_SERVICE_CREDENTIAL,
    )
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)
    Base.metadata.create_all(engine)

    user = User(
        id=uuid4(),
        email="user@example.test",
        password_hash=generate_password_hash(USER_PASSWORD),
        active=True,
    )
    organization = Organization(id=uuid4(), name="Synthetic Organization")
    with session_factory() as session:
        session.add_all([user, organization])
        session.flush()
        session.add(
            Membership(
                user_id=user.id,
                organization_id=organization.id,
                role="owner",
            )
        )
        session.commit()

    try:
        yield IdentityHarness(
            client=TestClient(
                create_app(settings=settings, session_factory=session_factory),
                raise_server_exceptions=False,
            ),
            engine=engine,
            session_factory=session_factory,
            user_id=user.id,
            organization_id=organization.id,
        )
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
