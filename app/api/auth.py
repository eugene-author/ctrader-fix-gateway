"""X-API-Key authentication."""

from dataclasses import dataclass
from uuid import UUID

from fastapi import Depends, Request
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.dependencies import get_session_factory, get_settings
from app.core.api_keys import key_prefix, verify_api_key
from app.db.models import ApiClientRow
from app.domain.errors import UnauthorizedError

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass(frozen=True, slots=True)
class ApiClient:
    """The authenticated caller's identity."""

    id: UUID
    account_id: UUID


class ApiKeyAuthenticator:
    """Resolves a raw X-API-Key to its ApiClient. Shared by HTTP and WebSocket auth."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], api_key_pepper: str) -> None:
        """Store the session factory used to look up candidate keys, and the HMAC pepper."""
        self._session_factory = session_factory
        self._api_key_pepper = api_key_pepper

    async def authenticate(self, raw_key: str | None) -> ApiClient:
        """Resolve `raw_key` to the ApiClient it belongs to, or raise UnauthorizedError.

        Looks up candidates by key_prefix (indexed).
        """
        if not raw_key:
            raise UnauthorizedError("Missing X-API-Key header")

        async with self._session_factory() as session:
            stmt = select(ApiClientRow).where(ApiClientRow.key_prefix == key_prefix(raw_key))
            candidates = (await session.execute(stmt)).scalars().all()

        for candidate in candidates:
            if candidate.disabled_at is not None:
                continue
            if verify_api_key(raw_key, candidate.api_key_hmac, pepper=self._api_key_pepper):
                return ApiClient(id=candidate.id, account_id=candidate.account_id)

        raise UnauthorizedError("Invalid API key")


def get_api_key_authenticator(request: Request) -> ApiKeyAuthenticator:
    """Build an ApiKeyAuthenticator sharing the app's session factory."""
    return ApiKeyAuthenticator(get_session_factory(request), get_settings(request).api_key_pepper)


async def get_authenticated_client(
    api_key: str | None = Depends(_api_key_header),
    authenticator: ApiKeyAuthenticator = Depends(get_api_key_authenticator),
) -> ApiClient:
    """Resolve X-API-Key to its ApiClient, or raise UnauthorizedError."""
    return await authenticator.authenticate(api_key)


async def get_authenticated_account_id(client: ApiClient = Depends(get_authenticated_client)) -> UUID:
    """Resolve X-API-Key to the account it belongs to, or raise UnauthorizedError."""
    return client.account_id
