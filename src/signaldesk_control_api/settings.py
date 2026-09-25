import secrets
from typing import Self

from pydantic import PostgresDsn, RedisDsn, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SIGNALDESK_",
        extra="forbid",
        strict=True,
    )

    database_url: PostgresDsn
    redis_url: RedisDsn
    web_bff_service_credential: SecretStr
    diagnostic_worker_service_credential: SecretStr
    export_worker_service_credential: SecretStr
    email_worker_service_credential: SecretStr
    service_name: str = "signaldesk-control-api"

    @field_validator(
        "web_bff_service_credential",
        "diagnostic_worker_service_credential",
        "export_worker_service_credential",
        "email_worker_service_credential",
    )
    @classmethod
    def require_strong_service_credential(cls, value: SecretStr) -> SecretStr:
        credential = value.get_secret_value()
        if (
            len(credential) < 32
            or credential != credential.strip()
            or not credential.isascii()
        ):
            raise ValueError(
                "service credentials must contain at least 32 ASCII "
                "characters and no surrounding whitespace"
            )
        return value

    @model_validator(mode="after")
    def require_distinct_service_credentials(self) -> Self:
        credentials = [
            self.web_bff_service_credential.get_secret_value(),
            self.diagnostic_worker_service_credential.get_secret_value(),
            self.export_worker_service_credential.get_secret_value(),
            self.email_worker_service_credential.get_secret_value(),
        ]
        for index, credential in enumerate(credentials):
            if any(
                secrets.compare_digest(credential, other)
                for other in credentials[index + 1 :]
            ):
                raise ValueError("service credentials must be distinct")
        return self


class OutboxPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SIGNALDESK_",
        extra="forbid",
        strict=True,
    )

    database_url: PostgresDsn
    redis_url: RedisDsn
    service_name: str = "signaldesk-outbox-publisher"
