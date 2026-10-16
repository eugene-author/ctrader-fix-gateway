from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core import metrics_router
from app.core.config import Settings
from app.core.metrics_middleware import MetricsMiddleware
from app.main import create_app


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://u:p@localhost/gateway",
        redis_url="redis://localhost:6379/0",
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )


def test_a_request_is_observed_under_its_route_template_not_the_raw_path() -> None:
    app = create_app(settings=_settings())
    client = TestClient(app)

    client.get("/health/live")

    body = client.get("/metrics").text
    assert 'http_request_duration_seconds_count{method="GET",route="/health/live",status="200"}' in body


def test_a_path_parameter_is_collapsed_to_its_template_not_the_raw_value() -> None:
    app = FastAPI()
    app.add_middleware(MetricsMiddleware)
    app.include_router(metrics_router.router)

    @app.get("/things/{thing_id}")
    def _get_thing(thing_id: str) -> dict[str, str]:
        return {"thing_id": thing_id}

    client = TestClient(app)

    client.get("/things/4471123")

    body = client.get("/metrics").text
    assert 'route="/things/{thing_id}"' in body
    assert "4471123" not in body
