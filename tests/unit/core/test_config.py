import pytest
from pydantic import ValidationError

from app.core.config import Settings


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/gateway")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    monkeypatch.setenv("API_KEY_PEPPER", "test-pepper")


def test_settings_loads_required_values_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = Settings()

    assert settings.database_url == "postgresql+asyncpg://u:p@localhost/gateway"
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.rabbitmq_url == "amqp://guest:guest@localhost/"


def test_settings_defaults_app_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = Settings()

    assert settings.app_env == "dev"
    assert settings.app_name == "ctrader-fix-gateway"
    assert settings.app_log_level == "INFO"


def test_settings_fails_fast_when_database_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    monkeypatch.setenv("API_KEY_PEPPER", "test-pepper")

    with pytest.raises(ValidationError):
        Settings()


def test_settings_repr_redacts_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = Settings()

    rendered = repr(settings)
    assert "u:p@localhost" not in rendered
    assert "***REDACTED***" in rendered
