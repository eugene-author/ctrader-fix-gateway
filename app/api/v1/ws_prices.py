"""WS /ws/prices: streaming quotes over the Redis pub/sub channels fix-worker publishes to."""

import asyncio
import contextlib

import redis.asyncio as redis
import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.auth import ApiKeyAuthenticator
from app.cache.redis_price_cache import quotes_channel
from app.core.config import Settings
from app.domain.errors import UnauthorizedError

logger = structlog.get_logger()

router = APIRouter()


class PriceWebSocketSession:
    """One /ws/prices connection: owns its socket, Redis PubSub, subscriptions and heartbeat timing."""

    def __init__(
        self,
        websocket: WebSocket,
        redis_client: redis.Redis,
        authenticator: ApiKeyAuthenticator,
        settings: Settings,
    ) -> None:
        """Store collaborators, run() authenticates, accepts the socket, and serves the connection."""
        self._websocket = websocket
        self._redis = redis_client
        self._authenticator = authenticator
        self._settings = settings
        self._subscribed: set[str] = set()
        self._pubsub: redis.client.PubSub = redis_client.pubsub()

    async def run(self) -> None:
        """Authenticate, accept, then subscribe/unsubscribe until the client disconnects."""
        api_key = self._websocket.query_params.get("api_key") or self._websocket.headers.get("x-api-key")
        try:
            await self._authenticator.authenticate(api_key)
        except UnauthorizedError:
            await self._websocket.close(code=4401)
            return

        await self._websocket.accept()
        await self._pubsub.subscribe(f"__ws_prices_control__:{id(self._websocket)}")

        try:
            while True:
                recv_task = asyncio.ensure_future(self._websocket.receive_json())
                pubsub_task = asyncio.ensure_future(self.next_pubsub_message())
                done, pending = await asyncio.wait({recv_task, pubsub_task}, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                for task in pending:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

                if recv_task in done:
                    try:
                        frame = recv_task.result()
                    except WebSocketDisconnect:
                        break
                    await self.handle_frame(frame)

                if pubsub_task in done:
                    message = pubsub_task.result()
                    if message is None:
                        await self._websocket.send_json({"type": "heartbeat"})
                    else:
                        await self._websocket.send_text(str(message["data"]))
        except WebSocketDisconnect:
            pass
        finally:
            await self.close()

    async def handle_frame(self, frame: dict[str, object]) -> None:
        """Apply one client subscribe/unsubscribe frame."""
        action = frame.get("action")
        symbols = frame.get("symbols")
        if action not in ("subscribe", "unsubscribe") or not isinstance(symbols, list):
            await self._websocket.send_json({"type": "error", "message": "expected {action, symbols}"})
            return

        if action == "subscribe":
            for symbol in symbols:
                if symbol in self._subscribed:
                    continue
                if len(self._subscribed) >= self._settings.ws_max_symbols:
                    await self._websocket.send_json(
                        {"type": "error", "message": f"WS_MAX_SYMBOLS ({self._settings.ws_max_symbols}) exceeded"}
                    )
                    break
                await self._pubsub.subscribe(quotes_channel(symbol))
                self._subscribed.add(symbol)
        else:
            for symbol in symbols:
                if symbol not in self._subscribed:
                    continue
                await self._pubsub.unsubscribe(quotes_channel(symbol))
                self._subscribed.discard(symbol)

    async def next_pubsub_message(self) -> dict[str, object] | None:
        """Poll for the next real message within ws_heartbeat_seconds, or None once it elapses."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._settings.ws_heartbeat_seconds
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            message = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=remaining)
            if message is not None:
                return message

    async def close(self) -> None:
        """Release the Redis PubSub connection."""
        await self._pubsub.aclose()


@router.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket) -> None:
    """Construct this connection's PriceWebSocketSession and run it."""
    settings: Settings = websocket.app.state.settings
    session_factory: async_sessionmaker[AsyncSession] = websocket.app.state.session_factory
    redis_client: redis.Redis = websocket.app.state.redis_client

    session = PriceWebSocketSession(
        websocket, redis_client, ApiKeyAuthenticator(session_factory, settings.api_key_pepper), settings
    )
    await session.run()
