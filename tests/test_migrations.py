from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from signaldesk_control_api.models import Base


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_upgrade_head_records_expected_revision_on_empty_postgresql(
    postgres_url: str,
) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)

    command.upgrade(config, "head")

    expected_head = ScriptDirectory.from_config(config).get_current_head()
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            applied_head = connection.scalar(
                text("SELECT version_num FROM alembic_version")
            )
    finally:
        engine.dispose()

    assert expected_head is not None
    assert applied_head == expected_head


def test_upgrade_head_creates_identity_tables_on_empty_postgresql(
    postgres_url: str,
) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.downgrade(config, "base")

    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    try:
        inspector = inspect(engine)
        assert {"users", "organizations", "memberships"}.issubset(
            inspector.get_table_names()
        )
        assert {column["name"] for column in inspector.get_columns("users")} == {
            "id",
            "email",
            "password_hash",
            "active",
        }
        assert {column["name"] for column in inspector.get_columns("memberships")} == {
            "user_id",
            "organization_id",
            "role",
        }
        assert inspector.get_pk_constraint("memberships")["constrained_columns"] == [
            "user_id",
            "organization_id",
        ]
        assert [
            constraint["column_names"]
            for constraint in inspector.get_unique_constraints("users")
        ] == [["email"]]
        assert [
            constraint["name"]
            for constraint in inspector.get_check_constraints("users")
        ] == ["ck_users_email_normalized"]
    finally:
        engine.dispose()


def test_upgrade_head_creates_diagnostic_and_outbox_tables_on_empty_postgresql(
    postgres_url: str,
) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.downgrade(config, "base")

    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    try:
        inspector = inspect(engine)
        assert {"diagnostic_jobs", "outbox_events"}.issubset(
            inspector.get_table_names()
        )
        assert {
            column["name"] for column in inspector.get_columns("diagnostic_jobs")
        } == {
            "id",
            "organization_id",
            "requested_by_user_id",
            "target",
            "status",
            "result_json",
            "correlation_id",
            "created_at",
            "updated_at",
        }
        assert {
            column["name"] for column in inspector.get_columns("outbox_events")
        } == {
            "id",
            "event_type",
            "aggregate_id",
            "payload_json",
            "published_at",
            "attempt_count",
            "next_attempt_at",
            "created_at",
        }
        columns = {
            column["name"]: column for column in inspector.get_columns("outbox_events")
        }
        assert columns["attempt_count"]["nullable"] is False
        assert columns["attempt_count"]["default"] == "0"
        assert columns["next_attempt_at"]["nullable"] is True
        assert columns["next_attempt_at"]["type"].timezone is True
        assert {
            constraint["name"]: constraint["sqltext"]
            for constraint in inspector.get_check_constraints("outbox_events")
        } == {"ck_outbox_events_attempt_count": "attempt_count >= 0"}
        indexes = {
            index["name"]: index for index in inspector.get_indexes("outbox_events")
        }
        assert indexes["ix_outbox_events_publication_eligibility"]["column_names"] == [
            "published_at",
            "next_attempt_at",
            "created_at",
            "id",
        ]
        assert "ix_outbox_events_published_created_at" not in indexes
    finally:
        engine.dispose()


def test_upgrade_head_creates_export_and_email_tables_on_empty_postgresql(
    postgres_url: str,
) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.downgrade(config, "base")
    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    try:
        inspector = inspect(engine)
        assert {"export_jobs", "email_deliveries"}.issubset(inspector.get_table_names())
        assert {column["name"] for column in inspector.get_columns("export_jobs")} == {
            "id",
            "organization_id",
            "requested_by_user_id",
            "format",
            "status",
            "object_key",
            "object_sha256",
            "size_bytes",
            "snapshot_id",
            "correlation_id",
            "created_at",
            "updated_at",
        }
        assert {
            column["name"] for column in inspector.get_columns("email_deliveries")
        } == {
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
        email_columns = {
            column["name"]: column
            for column in inspector.get_columns("email_deliveries")
        }
        assert email_columns["recipient_email_snapshot"]["nullable"] is True
        assert email_columns["sent_at"]["nullable"] is True
        assert email_columns["sent_at"]["type"].timezone is True
        checks = {
            constraint["name"]: constraint["sqltext"]
            for constraint in inspector.get_check_constraints("email_deliveries")
        }
        assert "'sending'" in checks["ck_email_deliveries_status"]
        assert (
            "recipient_email_snapshot" in checks["ck_email_deliveries_delivery_state"]
        )
    finally:
        engine.dispose()


def test_upgrade_0005_to_0006_normalizes_populated_delivery_states_and_downgrades(
    postgres_url: str,
) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.downgrade(config, "base")
    command.upgrade(config, "20260723_0005")
    engine = create_engine(postgres_url)
    organization_id = uuid4()
    user_id = uuid4()
    delivery_ids = {status: uuid4() for status in ("pending", "sent", "failed")}
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO organizations (id, name) VALUES (:id, 'migration org')"
                ),
                {"id": organization_id},
            )
            connection.execute(
                text(
                    "INSERT INTO users (id, email, password_hash, active) "
                    "VALUES (:id, 'migration@example.test', 'hash', true)"
                ),
                {"id": user_id},
            )
            for status_value, delivery_id in delivery_ids.items():
                connection.execute(
                    text(
                        "INSERT INTO email_deliveries "
                        "(id, organization_id, recipient_user_id, template_name, "
                        "template_data_json, status, correlation_id, attempted_at) "
                        "VALUES (:id, :organization_id, :user_id, :template_name, "
                        "CAST('{}' AS jsonb), :status, :correlation_id, :attempted_at)"
                    ),
                    {
                        "id": delivery_id,
                        "organization_id": organization_id,
                        "user_id": user_id,
                        "template_name": f"migration-{status_value}",
                        "status": status_value,
                        "correlation_id": uuid4(),
                        "attempted_at": (
                            datetime.now(timezone.utc)
                            if status_value == "pending"
                            else None
                        ),
                    },
                )

        command.upgrade(config, "20260723_0006")
        with engine.connect() as connection:
            rows = {
                row.status: row
                for row in connection.execute(
                    text(
                        "SELECT status, recipient_email_snapshot, attempted_at, sent_at "
                        "FROM email_deliveries"
                    )
                )
            }
        assert rows["pending"].recipient_email_snapshot is None
        assert rows["pending"].attempted_at is None
        assert rows["pending"].sent_at is None
        for status_value in ("sent", "failed"):
            assert (
                rows[status_value].recipient_email_snapshot == "migration@example.test"
            )
            assert rows[status_value].attempted_at is not None
        assert rows["sent"].sent_at is not None
        assert rows["failed"].sent_at is None

        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE email_deliveries SET status = 'sending', "
                    "recipient_email_snapshot = 'migration@example.test', "
                    "attempted_at = CURRENT_TIMESTAMP WHERE id = :id"
                ),
                {"id": delivery_ids["pending"]},
            )
        command.downgrade(config, "20260723_0005")
        assert {
            column["name"] for column in inspect(engine).get_columns("email_deliveries")
        }.isdisjoint({"recipient_email_snapshot", "sent_at"})
        with engine.connect() as connection:
            downgraded = connection.execute(
                text(
                    "SELECT status, attempted_at FROM email_deliveries WHERE id = :id"
                ),
                {"id": delivery_ids["pending"]},
            ).one()
        assert downgraded.status == "pending"
        assert downgraded.attempted_at is None
    finally:
        engine.dispose()


def test_alembic_head_exactly_matches_orm_metadata(postgres_url: str) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.downgrade(config, "base")
    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            migration_context = MigrationContext.configure(connection)
            differences = compare_metadata(migration_context, Base.metadata)
    finally:
        engine.dispose()

    assert differences == []


def test_upgrade_0006_to_0007_backfills_populated_exports_and_downgrades(
    postgres_url: str,
) -> None:
    config = Config(REPO_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.downgrade(config, "base")
    command.upgrade(config, "20260723_0006")
    engine = create_engine(postgres_url)
    organization_id, user_id = uuid4(), uuid4()
    export_ids = {status: uuid4() for status in ("pending", "claimed", "completed")}
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO organizations (id, name) VALUES (:id, 'snapshot org')"
                ),
                {"id": organization_id},
            )
            connection.execute(
                text(
                    "INSERT INTO users (id, email, password_hash, active) "
                    "VALUES (:id, 'snapshot@example.test', 'hash', true)"
                ),
                {"id": user_id},
            )
            for index in range(2):
                connection.execute(
                    text(
                        "INSERT INTO diagnostic_jobs "
                        "(id, organization_id, requested_by_user_id, target, status, "
                        "result_json, correlation_id, created_at) VALUES "
                        "(:id, :org, :user, :target, 'completed', "
                        "CAST(:result AS jsonb), :correlation, :created_at)"
                    ),
                    {
                        "id": uuid4(),
                        "org": organization_id,
                        "user": user_id,
                        "target": f"frozen-{index}.example.test",
                        "result": '{"frozen": true}',
                        "correlation": uuid4(),
                        "created_at": datetime(
                            2026, 7, 23, 10, 0, index, tzinfo=timezone.utc
                        ),
                    },
                )
            for status_value, export_id in export_ids.items():
                connection.execute(
                    text(
                        "INSERT INTO export_jobs "
                        "(id, organization_id, requested_by_user_id, format, status, "
                        "correlation_id) VALUES "
                        "(:id, :org, :user, 'json', :status, :correlation)"
                    ),
                    {
                        "id": export_id,
                        "org": organization_id,
                        "user": user_id,
                        "status": status_value,
                        "correlation": uuid4(),
                    },
                )

        command.upgrade(config, "20260723_0007")
        with engine.connect() as connection:
            jobs = connection.execute(
                text("SELECT id, snapshot_id FROM export_jobs ORDER BY id")
            ).all()
            snapshots = connection.execute(
                text(
                    "SELECT export_job_id, snapshot_id, position, target, result_json "
                    "FROM export_diagnostic_snapshots ORDER BY export_job_id, position"
                )
            ).all()
        assert len(jobs) == 3
        assert len({row.snapshot_id for row in jobs}) == 3
        assert all(row.snapshot_id is not None for row in jobs)
        assert len(snapshots) == 6
        for job in jobs:
            own = [row for row in snapshots if row.export_job_id == job.id]
            assert [row.position for row in own] == [1, 2]
            assert all(row.snapshot_id == job.snapshot_id for row in own)
            assert [row.target for row in own] == [
                "frozen-0.example.test",
                "frozen-1.example.test",
            ]
            assert all(row.result_json == {"frozen": True} for row in own)

        command.downgrade(config, "20260723_0006")
        inspector = inspect(engine)
        assert "export_diagnostic_snapshots" not in inspector.get_table_names()
        assert "snapshot_id" not in {
            column["name"] for column in inspector.get_columns("export_jobs")
        }
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM export_jobs")) == 3
    finally:
        command.upgrade(config, "head")
        engine.dispose()
