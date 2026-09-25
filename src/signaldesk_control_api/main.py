from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from signaldesk_control_api.database import (
    SessionFactory,
    create_engine,
    create_session_factory,
)
from signaldesk_control_api.routes import (
    auth,
    diagnostics,
    email_deliveries,
    exports,
    internal,
)
from signaldesk_control_api.settings import Settings


def create_app(
    *,
    settings: Settings | None = None,
    session_factory: SessionFactory | None = None,
) -> FastAPI:
    owned_engine = None
    if settings is not None and session_factory is None:
        owned_engine = create_engine(settings)
        session_factory = create_session_factory(owned_engine)

    @asynccontextmanager
    async def lifespan(_application: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owned_engine is not None:
                owned_engine.dispose()

    application = FastAPI(lifespan=lifespan)
    application.state.settings = settings
    application.state.session_factory = session_factory
    application.state.engine = owned_engine
    application.include_router(auth.router)
    application.include_router(internal.router)
    application.include_router(diagnostics.router)
    application.include_router(diagnostics.worker_router)
    application.include_router(exports.router)
    application.include_router(exports.worker_router)
    application.include_router(email_deliveries.router)

    @application.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application


def create_configured_app() -> FastAPI:
    """Create the deployable application from validated environment settings."""

    return create_app(settings=Settings())  # type: ignore[call-arg]


app = create_app()
