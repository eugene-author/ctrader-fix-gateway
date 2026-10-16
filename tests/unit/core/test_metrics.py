from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app


def test_metrics_endpoint_exposes_prometheus_text_format() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://u:p@localhost/gateway",
        redis_url="redis://localhost:6379/0",
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )
    app = create_app(settings=settings)
    client = TestClient(app)

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
