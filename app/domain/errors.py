"""Error taxonomy and its specific errors."""


class AppError(Exception):
    """Base of the error taxonomy. Every subclass has a stable `code` and `status_code`.

    Both can be overridden per-instance for the many specific error codes.
    """

    code: str = "INTERNAL"
    status_code: int = 500

    def __init__(self, message: str, *, code: str | None = None, status_code: int | None = None) -> None:
        """Set `message` as the exception text, optionally overriding `code`/`status_code`."""
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code
        super().__init__(message)


class ValidationError(AppError):
    """Client's fault. Maps to a 4xx. Never retried."""

    code = "VALIDATION_ERROR"
    status_code = 422


class TransientError(AppError):
    """Retry with backoff. The failure may not recur."""

    code = "TRANSIENT_ERROR"
    status_code = 503


class PermanentError(AppError):
    """Retrying cannot help - the request or the data itself is wrong."""

    code = "PERMANENT_ERROR"
    status_code = 500


class NotFoundError(AppError):
    """The requested resource does not exist."""

    code = "NOT_FOUND"
    status_code = 404


class ConflictError(AppError):
    """The request conflicts with the resource's current state."""

    code = "CONFLICT"
    status_code = 409


class UnauthorizedError(AppError):
    """The request is missing or has invalid credentials."""

    code = "UNAUTHORIZED"
    status_code = 401


class IllegalOrderTransitionError(PermanentError):
    """Raised by app.domain.state_machine when a status transition isn't legal."""

    code = "ILLEGAL_ORDER_TRANSITION"

    def __init__(self, *, from_status: object, to_status: object) -> None:
        """Record the rejected `from_status` -> `to_status` transition."""
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(f"Illegal order transition: {from_status} -> {to_status}")


class OrderNotFoundError(NotFoundError):
    """No order exists with the given id."""

    code = "ORDER_NOT_FOUND"

    def __init__(self, order_id: object) -> None:
        """Record the `order_id` that couldn't be found."""
        self.order_id = order_id
        super().__init__(f"Order not found: {order_id}")


class IdempotencyKeyReusedError(ConflictError):
    """A client_order_id was reused with a different request body."""

    code = "IDEMPOTENCY_KEY_REUSED"

    def __init__(self, client_order_id: str) -> None:
        """Record the reused `client_order_id`."""
        self.client_order_id = client_order_id
        super().__init__(f"client_order_id {client_order_id!r} was reused with a different request body")


class SymbolsNotReadyError(TransientError):
    """No symbols are configured yet, so order validation cannot proceed."""

    code = "SYMBOLS_NOT_READY"

    def __init__(self) -> None:
        """Build the fixed message for this error - it carries no per instance detail."""
        super().__init__("No symbols are configured for this account yet")


class PriceUnavailableError(TransientError):
    """No cached quote exists for a symbol yet."""

    code = "PRICE_UNAVAILABLE"

    def __init__(self, symbol: str) -> None:
        """Record the `symbol` with no cached price."""
        self.symbol = symbol
        super().__init__(f"No cached price available for {symbol}")


class OrderNotCancellableError(ConflictError):
    """The order is not in a working state a cancel can target."""

    code = "ORDER_NOT_CANCELLABLE"

    def __init__(self, order_id: object, status: object) -> None:
        """Record the `order_id` and its current `status`."""
        self.order_id = order_id
        self.status = status
        super().__init__(f"Order {order_id} is not cancellable in status {status}")


class PositionNotFoundError(NotFoundError):
    """No position exists with the given id."""

    code = "POSITION_NOT_FOUND"

    def __init__(self, position_id: object) -> None:
        """Record the `position_id` that couldn't be found."""
        self.position_id = position_id
        super().__init__(f"Position not found: {position_id}")


class PositionAlreadyClosingError(ConflictError):
    """A close is already in flight for this position."""

    code = "POSITION_ALREADY_CLOSING"

    def __init__(self, position_id: object) -> None:
        """Record the `position_id` already closing."""
        self.position_id = position_id
        super().__init__(f"Position {position_id} is already closing")


class RateLimitedError(TransientError):
    """A per-client rate limit was exceeded: POST /orders, Redis token bucket."""

    code = "RATE_LIMITED"
    status_code = 429

    def __init__(self, retry_after_seconds: float) -> None:
        """Record how long the client should wait before retrying."""
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"Rate limit exceeded, retry after {retry_after_seconds:.2f}s")


class DuplicateKeyError(ConflictError):
    """A unique constraint was violated.

    Raised by app.db.unit_of_work.SqlAlchemyUnitOfWork.commit
    """

    code = "DUPLICATE_KEY"


class ConcurrentModificationError(ConflictError):
    """A row changed between being read and being written back.

    Raised by app.db.unit_of_work.SqlAlchemyUnitOfWork.commit
    """

    code = "CONCURRENT_MODIFICATION"
