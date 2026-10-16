"""http_request_duration_seconds instrumentation for every request."""

import time
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.core.metrics import http_request_duration_seconds


class MetricsMiddleware(BaseHTTPMiddleware):
    """Observe http_request_duration_seconds, labelled by the route template, not the raw path."""

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Time the request and record it under the reconstructed route template, method and status."""
        start = time.perf_counter()
        response = await call_next(request)
        route_path = request.url.path
        for name, value in request.path_params.items():
            route_path = route_path.replace(str(value), f"{{{name}}}")
        http_request_duration_seconds.labels(
            route=route_path, method=request.method, status=str(response.status_code)
        ).observe(time.perf_counter() - start)
        return response
