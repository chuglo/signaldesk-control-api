FROM python:3.11.15-slim-bookworm@sha256:b18992999dbe963a45a8a4da40ac2b1975be1a776d939d098c647482bcad5cba

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/tmp \
    PATH=/app/.venv/bin:$PATH
WORKDIR /app
COPY --from=contracts pyproject.toml README.md /build/signaldesk-contracts/
COPY --from=contracts src /build/signaldesk-contracts/src
COPY pyproject.toml README.md uv.lock /build/signaldesk-control-api/
COPY src /build/signaldesk-control-api/src
RUN python -m pip install uv==0.11.31 \
    && cd /build/signaldesk-control-api \
    && UV_PROJECT_ENVIRONMENT=/app/.venv uv sync --locked --no-dev --no-editable \
    && rm -rf /build
COPY alembic.ini /app/alembic.ini
COPY alembic /app/alembic
COPY --from=deploy scripts/wait-for-health.py /opt/signaldesk/wait-for-health.py
COPY --from=deploy scripts/migrate.py /opt/signaldesk/migrate.py
COPY --from=deploy scripts/seed.py /opt/signaldesk/seed.py
RUN chmod 0555 /opt/signaldesk/*.py \
    && chmod -R a=rX /app/alembic /app/alembic.ini
USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "signaldesk_control_api.main:create_configured_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--no-server-header", "--no-proxy-headers"]
