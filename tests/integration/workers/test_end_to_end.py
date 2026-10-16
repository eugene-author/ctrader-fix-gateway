"""End-to-end acceptance test: POST /orders -> FILLED.

Runs every M5/M6 piece together against the mock FIX server: PlaceOrderHandler
writes PENDING plus an outbox row, a relay poll loop drains it onto
commands.orders, fix-worker (a real subprocess - see
test_fix_worker_process.py for why it must be one) dispatches NewOrderSingle
and receives the mock server's default fill, and a projector consumer
applies the resulting ExecutionReportReceived. The relay and projector run as
plain asyncio background tasks in this process - pure asyncio, no reactor
conflict - only fix-worker needs subprocess isolation.
"""

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import threading
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import RelaySettings
from app.db.models import AccountRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import OrderStatus, OrderType, Side
from app.queue.consumer import consume
from app.queue.topology import COMMANDS_EXCHANGE, EVENTS_QUEUE
from app.services.commands.place_order import PlaceOrderCommand, PlaceOrderHandler
from app.workers.projector import ProjectorHandler
from app.workers.relay import OutboxRelay
from tests.mocks.fix_server import MockFixServer

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


async def _seed_tradable_account_and_symbol(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[AccountRow, SymbolRow]:
    # The shared seeded_account_and_symbol fixture in
    # tests/integration/conftest.py is deliberately minimal - needs_review is
    # left at its True default - just enough for FK references in
    # repository-level tests. Placing a real order needs a fully configured
    # symbol, matching tests/integration/api/test_orders.py's own fixture.
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
        await session.commit()
        await session.refresh(account)
        await session.refresh(symbol)

    return account, symbol


def _read_until(process: subprocess.Popen[str], needle: str, timeout: float) -> str:
    """Read the child's stdout until `needle` appears, then keep draining it for the rest of its life.

    A thread that stops reading once it finds `needle` leaves the pipe
    unread from then on - once the OS pipe buffer fills (default 64KB on
    Linux), the child's next write() blocks, freezing it, including its
    ability to process the order this test is about to place. The prefix
    up to `needle` is still what's returned/asserted on; everything after
    is drained and discarded, just to keep the pipe from backing up.
    """
    lines: list[str] = []
    found = threading.Event()
    prefix: list[str] = []

    def _pump() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.append(line)
            if not found.is_set() and needle in line:
                prefix.extend(lines)
                found.set()

    thread = threading.Thread(target=_pump, daemon=True)
    thread.start()
    found.wait(timeout=timeout)
    return "".join(prefix)


async def _relay_loop(relay: OutboxRelay) -> None:
    while True:
        await relay.publish_batch(max_attempts=10)
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_post_orders_reaches_filled_end_to_end(
    migrated_postgres_url: str,
    rabbitmq_url: str,
    redis_url: str,
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
) -> None:
    account, symbol = await _seed_tradable_account_and_symbol(session_factory)
    server = MockFixServer()
    port = server.start()
    process: subprocess.Popen[str] | None = None
    relay_task: asyncio.Task[None] | None = None
    projector_task: asyncio.Task[None] | None = None
    try:
        # relay: drains outbox_events onto commands.orders.
        relay_channel = await rabbitmq_connection.channel(publisher_confirms=True)
        commands_exchange = await relay_channel.get_exchange(COMMANDS_EXCHANGE)
        relay_settings = RelaySettings(database_url=migrated_postgres_url, rabbitmq_url=rabbitmq_url)
        relay = OutboxRelay(session_factory, commands_exchange, rabbitmq_connection, relay_settings)
        relay_task = asyncio.ensure_future(_relay_loop(relay))

        # projector: applies events.projector onto the database.
        def uow_factory() -> SqlAlchemyUnitOfWork:
            return SqlAlchemyUnitOfWork(session_factory)

        projector_handler = ProjectorHandler(uow_factory)
        projector_task = asyncio.ensure_future(consume(rabbitmq_connection, EVENTS_QUEUE, projector_handler.handle))

        # fix-worker: a real subprocess against the mock server (see
        # test_fix_worker_process.py's module docstring for why).
        env = {
            **os.environ,
            "APP_ENV": "dev",
            "APP_LOG_LEVEL": "INFO",
            "DATABASE_URL": migrated_postgres_url,
            "RABBITMQ_URL": rabbitmq_url,
            "REDIS_URL": redis_url,
            "ACCOUNT_ID": str(account.id),
            "FIX_TRADE_HOST": "127.0.0.1",
            "FIX_TRADE_PORT": str(port),
            "FIX_TRADE_SSL": "false",  # the mock server speaks plaintext, not TLS
            "FIX_TRADE_USERNAME": "1",
            "FIX_TRADE_PASSWORD": "pw",
            "FIX_TRADE_SENDER_COMP_ID": "test.client",
            "FIX_QUOTE_HOST": "127.0.0.1",
            "FIX_QUOTE_PORT": str(port),
            "FIX_QUOTE_SSL": "false",
            "FIX_QUOTE_USERNAME": "1",
            "FIX_QUOTE_PASSWORD": "pw",
            "FIX_QUOTE_SENDER_COMP_ID": "test.client",
            "GRACEFUL_TIMEOUT": "5",
        }
        # subprocess.Popen() itself only forks/execs (near-instant) and does not
        # wait for the child; the actual blocking I/O (reading stdout, waiting
        # for exit) already runs off the event loop, in _read_until's thread and
        # the bounded process.wait() calls below.
        process = subprocess.Popen(  # noqa: ASYNC220
            [sys.executable, "-u", "-m", "app.workers.fix_worker"],
            cwd=_PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        startup_output = _read_until(process, "security_list_received", timeout=20)
        assert "trade_session_logged_on" in startup_output, startup_output
        assert "security_list_received" in startup_output, startup_output

        # POST /orders, minus the HTTP layer: PlaceOrderHandler writes PENDING
        # plus the outbox row in one transaction, exactly as the API does.
        place_order = PlaceOrderHandler(lambda: SqlAlchemyUnitOfWork(session_factory))
        result = await place_order.handle(
            PlaceOrderCommand(
                account_id=account.id,
                client_order_id=f"e2e-{uuid.uuid4()}",
                symbol=symbol.name,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                request_fingerprint="test-fingerprint",
                quantity=Decimal(1000),
            )
        )
        assert result.status == OrderStatus.PENDING

        async def _poll_for_filled() -> OrderStatus:
            async with SqlAlchemyUnitOfWork(session_factory) as uow:
                while True:
                    order = await uow.orders.get(result.order_id)
                    assert order is not None
                    if order.status in (OrderStatus.FILLED, OrderStatus.FAILED, OrderStatus.REJECTED):
                        return order.status
                    await asyncio.sleep(0.1)

        final_status = await asyncio.wait_for(_poll_for_filled(), timeout=20)
        assert final_status == OrderStatus.FILLED

        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            filled_order = await uow.orders.get(result.order_id)
            assert filled_order is not None
            assert filled_order.position_id is not None
            position = await uow.positions.get_by_broker_position_id(account.id, filled_order.position_id)
        assert position is not None
        assert position.quantity.value == Decimal(1000)
    finally:
        if process is not None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for task in (relay_task, projector_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        server.stop()
