from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol, TypeAlias

from fastapi import HTTPException, Request, status
from pydantic import PostgresDsn
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

SessionFactory: TypeAlias = sessionmaker[Session]


class DatabaseSettings(Protocol):
    database_url: PostgresDsn


def create_engine(settings: DatabaseSettings) -> Engine:
    return sqlalchemy_create_engine(settings.database_url.unicode_string())


def create_session_factory(engine: Engine) -> SessionFactory:
    return sessionmaker(
        bind=engine,
        class_=Session,
        expire_on_commit=False,
        close_resets_only=False,
    )


def get_session(request: Request) -> Iterator[Session]:
    session_factory: SessionFactory | None = request.app.state.session_factory
    if session_factory is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not configured",
        )

    with session_factory() as session:
        yield session


@contextmanager
def session_scope(session_factory: SessionFactory) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
