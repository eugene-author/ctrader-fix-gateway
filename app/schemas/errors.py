"""Error response schema."""

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """Error body for every failed request."""

    type: str
    title: str
    status: int
    code: str
    detail: str
    instance: str
    correlation_id: str
