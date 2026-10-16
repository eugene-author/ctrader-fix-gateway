"""AppError error-response JSON."""

import math

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.domain.errors import AppError, RateLimitedError
from app.schemas.errors import ErrorResponse

logger = structlog.get_logger()

_ERROR_TYPE_PREFIX = "urn:ctrader-gateway:error"


def _error_type(code: str) -> str:
    return f"{_ERROR_TYPE_PREFIX}:{code.lower().replace('_', '-')}"


def _title_for(code: str) -> str:
    return code.replace("_", " ").title()


def _correlation_id(request: Request) -> str:
    return getattr(request.state, "correlation_id", "unknown")


async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    """Render any AppError as a problem-details response using its own code and status."""
    correlation_id = _correlation_id(request)
    error = ErrorResponse(
        type=_error_type(exc.code),
        title=_title_for(exc.code),
        status=exc.status_code,
        code=exc.code,
        detail=str(exc),
        instance=request.url.path,
        correlation_id=correlation_id,
    )
    log = logger.error if exc.status_code >= 500 else logger.info
    log("request_rejected", code=exc.code, status=exc.status_code, path=request.url.path)
    headers = {"Retry-After": str(math.ceil(exc.retry_after_seconds))} if isinstance(exc, RateLimitedError) else None
    return JSONResponse(status_code=exc.status_code, content=error.model_dump(), headers=headers)


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Render pydantic's own validation errors in the same problem-details shape."""
    correlation_id = _correlation_id(request)
    error = ErrorResponse(
        type=_error_type("VALIDATION_ERROR"),
        title=_title_for("VALIDATION_ERROR"),
        status=422,
        code="VALIDATION_ERROR",
        detail=str(exc),
        instance=request.url.path,
        correlation_id=correlation_id,
    )
    logger.info("request_rejected", code="VALIDATION_ERROR", status=422, path=request.url.path)
    return JSONResponse(status_code=422, content=error.model_dump())


def register_exception_handlers(app: FastAPI) -> None:
    """Wire the AppError and RequestValidationError handlers into the app."""
    app.exception_handler(AppError)(app_error_handler)
    app.exception_handler(RequestValidationError)(validation_error_handler)
