import structlog
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.correlation import CORRELATION_ID_HEADER, CorrelationIdMiddleware


def _make_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)

    @app.get("/whoami")
    async def _whoami() -> dict[str, str]:
        bound = structlog.contextvars.get_contextvars()
        return {"correlation_id": bound.get("correlation_id", "")}

    return app


def test_generates_a_correlation_id_when_none_is_supplied() -> None:
    client = TestClient(_make_app())

    response = client.get("/whoami")

    assert response.status_code == 200
    header_value = response.headers[CORRELATION_ID_HEADER]
    assert header_value
    assert response.json()["correlation_id"] == header_value


def test_echoes_a_supplied_correlation_id() -> None:
    client = TestClient(_make_app())

    response = client.get("/whoami", headers={CORRELATION_ID_HEADER: "given-id-123"})

    assert response.headers[CORRELATION_ID_HEADER] == "given-id-123"
    assert response.json()["correlation_id"] == "given-id-123"
