from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://u:p@localhost/gateway",
        redis_url="redis://localhost:6379/0",
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )


def test_create_app_builds_a_fastapi_app_with_configured_title() -> None:
    settings = _settings()

    app = create_app(settings=settings)

    assert app.title == settings.app_name
    assert app.state.settings is settings


def test_create_app_exposes_openapi_docs() -> None:
    app = create_app(settings=_settings())
    client = TestClient(app)

    response = client.get("/openapi.json")

    assert response.status_code == 200


def test_create_app_builds_a_session_factory() -> None:
    app = create_app(settings=_settings())

    assert app.state.session_factory is not None


def test_create_app_echoes_correlation_id_on_every_response() -> None:
    app = create_app(settings=_settings())
    client = TestClient(app)

    response = client.get("/health/live", headers={"X-Correlation-Id": "test-id-123"})

    assert response.headers["X-Correlation-Id"] == "test-id-123"


def test_create_app_disposes_the_engine_and_closes_redis_on_shutdown() -> None:
    app = create_app(settings=_settings())

    with (
        patch.object(type(app.state.engine), "dispose", new=AsyncMock()) as dispose,
        patch.object(type(app.state.redis_client), "aclose", new=AsyncMock()) as aclose,
        TestClient(app) as client,
    ):
        client.get("/health/live")

    dispose.assert_awaited_once()
    aclose.assert_awaited_once()
