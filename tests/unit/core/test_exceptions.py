from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from app.core.exceptions import register_exception_handlers
from app.domain.errors import OrderNotFoundError, ValidationError


def _make_app() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)

    @app.middleware("http")
    async def _set_correlation_id(request, call_next):
        request.state.correlation_id = "test-correlation-id"
        return await call_next(request)

    class Body(BaseModel):
        quantity: int

    @app.get("/boom/not-found")
    async def _not_found() -> None:
        raise OrderNotFoundError("0192f3c2-0000-7000-8000-000000000000")

    @app.get("/boom/validation")
    async def _validation() -> None:
        raise ValidationError("quantity must be a multiple of 100", code="QUANTITY_STEP_INVALID")

    @app.post("/boom/schema")
    async def _schema(body: Body) -> dict[str, int]:
        return {"quantity": body.quantity}

    return app


def test_app_error_renders_as_error_response_with_its_own_status_and_code() -> None:
    client = TestClient(_make_app())

    response = client.get("/boom/not-found")

    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "ORDER_NOT_FOUND"
    assert body["status"] == 404
    assert body["type"] == "urn:ctrader-gateway:error:order-not-found"
    assert body["instance"] == "/boom/not-found"
    assert body["correlation_id"] == "test-correlation-id"
    assert "0192f3c2" in body["detail"]


def test_validation_error_carries_its_overridden_code() -> None:
    client = TestClient(_make_app())

    response = client.get("/boom/validation")

    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "QUANTITY_STEP_INVALID"
    assert body["status"] == 422


def test_pydantic_schema_errors_are_rendered_as_error_response_too() -> None:
    client = TestClient(_make_app())

    response = client.post("/boom/schema", json={"quantity": "not-a-number"})

    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert body["status"] == 422
    assert "correlation_id" in body
