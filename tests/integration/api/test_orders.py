from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.api_keys import hash_api_key, key_prefix
from app.core.config import Settings
from app.db.models import AccountRow, ApiClientRow, OutboxEventRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import OrderStatus
from app.main import create_app


def _settings(database_url: str, redis_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        redis_url=redis_url,
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )


async def _seed_account_symbol_and_api_key(
    session_factory: async_sessionmaker[AsyncSession], raw_key: str
) -> tuple[AccountRow, SymbolRow]:
    async with session_factory() as session:
        account = AccountRow(broker="ctrader", external_account_id="1", mode="hedging", currency="USD", enabled=True)
        session.add(account)
        await session.flush()

        symbol = SymbolRow(
            account_id=account.id,
            name="EURUSD",
            broker_symbol_id=1,
            digits=5,
            lot_size=Decimal(100_000),
            qty_step=Decimal(1000),
            min_qty=Decimal(1000),
            max_qty=Decimal(10_000_000),
            price_step=Decimal("0.00001"),
            enabled=True,
            subscribe=True,
            needs_review=False,
        )
        session.add(symbol)

        session.add(
            ApiClientRow(
                account_id=account.id,
                name="bot7",
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper="test-pepper"),
                scopes=["orders:write"],
            )
        )
        await session.commit()

    return account, symbol


@pytest.fixture
async def client_setup(
    migrated_postgres_url: str, redis_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> tuple[AsyncClient, str, AccountRow, SymbolRow]:
    raw_key = f"sk_live_{uuid4().hex}"
    account, symbol = await _seed_account_symbol_and_api_key(session_factory, raw_key)
    app = create_app(settings=_settings(migrated_postgres_url, redis_url))
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": raw_key})
    return client, raw_key, account, symbol


@pytest.mark.asyncio
async def test_place_order_returns_202_and_lands_pending_with_one_outbox_row(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _raw_key, _account, _symbol = client_setup
    client_order_id = f"bot7-{uuid4()}"

    async with client:
        response = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": client_order_id,
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 1000,
            },
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "PENDING"
    assert body["client_order_id"] == client_order_id
    assert body["links"]["self"] == f"/api/v1/orders/{body['order_id']}"
    assert body["links"]["status"] == f"/api/v1/orders/{body['order_id']}/status"
    assert "X-Correlation-Id" in response.headers

    order_id = body["order_id"]
    async with session_factory() as session:
        outbox_rows = (
            (await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == order_id)))
            .scalars()
            .all()
        )

    assert len(outbox_rows) == 1
    assert outbox_rows[0].event_type == "CreateOrderCommand"
    assert outbox_rows[0].payload["quantity"] == "1000"


@pytest.mark.asyncio
async def test_duplicate_post_with_same_body_replays_with_200(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup
    payload = {
        "client_order_id": f"bot7-{uuid4()}",
        "symbol": "EURUSD",
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": 1000,
    }

    async with client:
        first = await client.post("/api/v1/orders", json=payload)
        second = await client.post("/api/v1/orders", json=payload)

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.headers["Idempotency-Replayed"] == "true"
    assert second.json()["order_id"] == first.json()["order_id"]


@pytest.mark.asyncio
async def test_duplicate_client_order_id_with_different_body_is_conflict(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup
    client_order_id = f"bot7-{uuid4()}"

    async with client:
        await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": client_order_id,
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 1000,
            },
        )
        response = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": client_order_id,
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 2000,  # different body -> different fingerprint
            },
        )

    assert response.status_code == 409
    assert response.json()["code"] == "IDEMPOTENCY_KEY_REUSED"


@pytest.mark.asyncio
async def test_unknown_symbol_returns_422_symbol_unknown(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        response = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": f"bot7-{uuid4()}",
                "symbol": "DOESNOTEXIST",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 1000,
            },
        )

    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "SYMBOL_UNKNOWN"
    assert "correlation_id" in body


@pytest.mark.asyncio
async def test_place_order_without_api_key_is_unauthorized(migrated_postgres_url: str, redis_url: str) -> None:
    app = create_app(settings=_settings(migrated_postgres_url, redis_url))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": "bot7-0001",
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 1000,
            },
        )

    assert response.status_code == 401
    assert response.json()["code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_get_order_and_get_order_status_after_placement(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        placed = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": f"bot7-{uuid4()}",
                "symbol": "EURUSD",
                "side": "SELL",
                "order_type": "MARKET",
                "quantity": 2000,
            },
        )
        order_id = placed.json()["order_id"]

        full = await client.get(f"/api/v1/orders/{order_id}")
        slim = await client.get(f"/api/v1/orders/{order_id}/status")

    assert full.status_code == 200
    full_body = full.json()
    assert full_body["order_id"] == order_id
    assert full_body["status"] == "PENDING"
    assert Decimal(full_body["quantity"]) == Decimal(2000)

    assert slim.status_code == 200
    slim_body = slim.json()
    assert slim_body["order_id"] == order_id
    assert slim_body["status"] == "PENDING"


@pytest.mark.asyncio
async def test_get_orders_lists_newest_first_and_paginates(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        placed_ids = []
        for _ in range(3):
            response = await client.post(
                "/api/v1/orders",
                json={
                    "client_order_id": f"bot7-{uuid4()}",
                    "symbol": "EURUSD",
                    "side": "BUY",
                    "order_type": "MARKET",
                    "quantity": 1000,
                },
            )
            placed_ids.append(response.json()["order_id"])

        first_page = await client.get("/api/v1/orders", params={"limit": 2})
        assert first_page.status_code == 200
        first_body = first_page.json()
        assert len(first_body["orders"]) == 2
        assert first_body["next_cursor"] is not None
        # Newest first: the last order placed is the first one listed.
        assert first_body["orders"][0]["order_id"] == placed_ids[-1]

        second_page = await client.get("/api/v1/orders", params={"limit": 2, "cursor": first_body["next_cursor"]})
        assert second_page.status_code == 200
        second_body = second_page.json()
        assert len(second_body["orders"]) == 1
        assert second_body["orders"][0]["order_id"] == placed_ids[0]

        by_status = await client.get("/api/v1/orders", params={"status": "PENDING"})
        assert by_status.status_code == 200
        assert {o["order_id"] for o in by_status.json()["orders"]} >= set(placed_ids)

        by_wrong_status = await client.get("/api/v1/orders", params={"status": "FILLED"})
        assert by_wrong_status.status_code == 200
        assert by_wrong_status.json()["orders"] == []

        openapi = await client.get("/openapi.json")
    assert openapi.status_code == 200
    assert "/api/v1/orders" in openapi.json()["paths"]
    assert "get" in openapi.json()["paths"]["/api/v1/orders"]


@pytest.mark.asyncio
async def test_get_order_for_unknown_id_is_404(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        response = await client.get(f"/api/v1/orders/{uuid4()}")

    assert response.status_code == 404
    assert response.json()["code"] == "ORDER_NOT_FOUND"


@pytest.mark.asyncio
async def test_place_order_with_volume_lots(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        response = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": f"bot7-{uuid4()}",
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "volume_lots": "0.1",
            },
        )
        assert response.status_code == 202
        order_id = response.json()["order_id"]

        full = await client.get(f"/api/v1/orders/{order_id}")

    assert Decimal(full.json()["quantity"]) == Decimal(10000)


@pytest.mark.asyncio
async def test_cancel_an_accepted_order_writes_a_cancel_command_to_the_outbox(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        placed = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": f"bot7-{uuid4()}",
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 1000,
            },
        )
        order_id = placed.json()["order_id"]

        # No fix-worker/projector running in this test - simulate the
        # ACCEPTED transition a real ExecutionReport would have driven.
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            order = await uow.orders.get(UUID(order_id))
            assert order is not None
            order.status = OrderStatus.ACCEPTED
            order.fix_cl_ord_id = "acceptedfix01"
            order.version = 3
            await uow.orders.update(order)
            await uow.commit()

        response = await client.post(f"/api/v1/orders/{order_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "ACCEPTED"

    async with session_factory() as session:
        outbox_rows = (
            (await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == order_id)))
            .scalars()
            .all()
        )
    cancel_rows = [row for row in outbox_rows if row.event_type == "CancelOrderCommand"]
    assert len(cancel_rows) == 1
    assert cancel_rows[0].payload["orig_cl_ord_id"] == "acceptedfix01"


@pytest.mark.asyncio
async def test_cancel_a_pending_order_is_conflict(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        placed = await client.post(
            "/api/v1/orders",
            json={
                "client_order_id": f"bot7-{uuid4()}",
                "symbol": "EURUSD",
                "side": "BUY",
                "order_type": "MARKET",
                "quantity": 1000,
            },
        )
        order_id = placed.json()["order_id"]

        response = await client.post(f"/api/v1/orders/{order_id}/cancel")

    assert response.status_code == 409
    assert response.json()["code"] == "ORDER_NOT_CANCELLABLE"


@pytest.mark.asyncio
async def test_cancel_an_unknown_order_is_404(
    client_setup: tuple[AsyncClient, str, AccountRow, SymbolRow],
) -> None:
    client, _raw_key, _account, _symbol = client_setup

    async with client:
        response = await client.post(f"/api/v1/orders/{uuid4()}/cancel")

    assert response.status_code == 404
    assert response.json()["code"] == "ORDER_NOT_FOUND"


@pytest.mark.asyncio
async def test_post_orders_rate_limits_a_client_and_sets_retry_after(
    migrated_postgres_url: str,
    redis_url: str,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    raw_key = f"sk_live_{uuid4().hex}"
    await _seed_account_symbol_and_api_key(session_factory, raw_key)
    settings = Settings(
        database_url=migrated_postgres_url,
        redis_url=redis_url,
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
        # A very slow refill: this test's assertions must hold no matter how
        # much wall-clock time three sequential HTTP round trips take under
        # full-suite load - a rate near 1/s previously made this test flaky,
        # since enough elapsed time between requests refills a whole token.
        rate_limit_orders_per_second=0.001,
        rate_limit_orders_burst=2.0,
    )
    app = create_app(settings=settings)
    transport = ASGITransport(app=app)

    def _body() -> dict[str, object]:
        return {
            "client_order_id": f"bot7-{uuid4()}",
            "symbol": "EURUSD",
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": 1000,
        }

    async with AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": raw_key}) as client:
        first = await client.post("/api/v1/orders", json=_body())
        second = await client.post("/api/v1/orders", json=_body())
        third = await client.post("/api/v1/orders", json=_body())

    assert first.status_code == 202
    assert second.status_code == 202
    assert third.status_code == 429
    assert third.json()["code"] == "RATE_LIMITED"
    assert "Retry-After" in third.headers
