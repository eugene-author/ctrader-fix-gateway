from fastapi.testclient import TestClient

from app.api.v1 import health
from app.api.v1.health import ReadinessChecker, ReadinessResult
from app.core.config import Settings
from app.main import create_app


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://u:p@localhost/gateway",
        redis_url="redis://localhost:6379/0",
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )


class _MockReadinessChecker(ReadinessChecker):
    def __init__(self, result: ReadinessResult) -> None:
        self._result = result

    async def check(self) -> ReadinessResult:
        return self._result


def test_health_live_returns_ok() -> None:
    app = create_app(settings=_settings())
    client = TestClient(app)

    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_ready_returns_200_when_all_dependencies_up() -> None:
    app = create_app(settings=_settings())
    app.dependency_overrides[health.get_readiness_checker] = lambda: _MockReadinessChecker(
        ReadinessResult(database=True, redis=True, rabbitmq=True)
    )
    client = TestClient(app)

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"database": True, "redis": True, "rabbitmq": True},
    }


def test_health_ready_returns_503_when_a_dependency_is_down() -> None:
    app = create_app(settings=_settings())
    app.dependency_overrides[health.get_readiness_checker] = lambda: _MockReadinessChecker(
        ReadinessResult(database=False, redis=True, rabbitmq=True)
    )
    client = TestClient(app)

    response = client.get("/health/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unavailable"
    assert body["checks"] == {"database": False, "redis": True, "rabbitmq": True}
