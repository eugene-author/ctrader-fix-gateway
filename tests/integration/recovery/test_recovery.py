"""Recovery scenarios: the gateway recovers from a connectivity loss unattended."""

import asyncio
import contextlib
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import aio_pika
import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from app.core.config import RelaySettings
from app.db.models import AccountRow, OutboxEventRow
from app.db.session import create_engine, create_session_factory
from app.queue.topology import COMMANDS_EXCHANGE, declare_topology
from app.workers.relay import OutboxRelay

_ALEMBIC_INI = Path(__file__).resolve().parents[3] / "alembic.ini"


def _migrate(database_url: str) -> None:
    original = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = database_url
    try:
        command.upgrade(Config(str(_ALEMBIC_INI)), "head")
    finally:
        if original is not None:
            os.environ["DATABASE_URL"] = original
        else:
            os.environ.pop("DATABASE_URL", None)


async def _seed_account(session_factory) -> UUID:
    account_id = uuid4()
    async with session_factory() as session:
        session.add(
            AccountRow(
                id=account_id,
                broker="ctrader",
                external_account_id="1",
                mode="hedging",
                currency="USD",
                enabled=True,
            )
        )
        await session.commit()
    return account_id


async def _seed_outbox_row(session_factory, *, account_id: UUID) -> UUID:
    aggregate_id = uuid4()
    async with session_factory() as session:
        session.add(
            OutboxEventRow(
                account_id=account_id,
                aggregate_type="order",
                aggregate_id=aggregate_id,
                event_type="CreateOrderCommand",
                payload={"order_id": str(aggregate_id)},
                routing_key="order.create",
                correlation_id=str(aggregate_id),
                causation_id=str(aggregate_id),
                available_at=datetime.now(UTC),
            )
        )
        await session.commit()
    return aggregate_id


async def _close_all_amqp_connections(management_url: str) -> None:
    """Force-close every open connection via RabbitMQ's management API (simulates an AMQP restart)."""
    async with httpx.AsyncClient(auth=("guest", "guest"), timeout=10) as client:
        response = await client.get(f"{management_url}/api/connections")
        response.raise_for_status()
        for conn in response.json():
            await client.delete(f"{management_url}/api/connections/{conn['name']}")


@pytest.mark.asyncio
async def test_the_relay_survives_its_amqp_connection_being_severed() -> None:
    with PostgresContainer("postgres:16", username="gateway", password="gateway", dbname="gateway") as postgres:
        db_url = (
            f"postgresql+asyncpg://{postgres.username}:{postgres.password}"
            f"@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
        )
        await asyncio.to_thread(_migrate, db_url)
        engine = create_engine(db_url)
        session_factory = create_session_factory(engine)

        try:
            account_id = await _seed_account(session_factory)

            rabbitmq_container = RabbitMqContainer(
                "rabbitmq:3.13-management", username="guest", password="guest"
            ).with_exposed_ports(5672, 15672)
            with rabbitmq_container as rabbitmq:
                host = rabbitmq.get_container_host_ip()
                amqp_port = rabbitmq.get_exposed_port(5672)
                management_port = rabbitmq.get_exposed_port(15672)
                rabbitmq_url = f"amqp://guest:guest@{host}:{amqp_port}/"
                management_url = f"http://{host}:{management_port}"

                connection = await aio_pika.connect_robust(rabbitmq_url)
                await declare_topology(connection)
                channel = await connection.channel(publisher_confirms=True)
                exchange = await channel.get_exchange(COMMANDS_EXCHANGE)
                relay_settings = RelaySettings(database_url=db_url, rabbitmq_url=rabbitmq_url)
                relay = OutboxRelay(session_factory, exchange, connection, relay_settings)

                # Sanity check: draining works before any fault is introduced.
                warmup_id = await _seed_outbox_row(session_factory, account_id=account_id)
                await relay.publish_batch(max_attempts=10)
                async with session_factory() as session:
                    warmup_row = (
                        await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == warmup_id))
                    ).scalar_one()
                assert warmup_row.published_at is not None

                recovery_id = await _seed_outbox_row(session_factory, account_id=account_id)

                await _close_all_amqp_connections(management_url)

                async def _wait_for_recovery() -> None:
                    while True:
                        with contextlib.suppress(Exception):
                            await relay.publish_batch(max_attempts=1000)
                        async with session_factory() as session:
                            row = (
                                await session.execute(
                                    select(OutboxEventRow).where(OutboxEventRow.aggregate_id == recovery_id)
                                )
                            ).scalar_one()
                        if row.published_at is not None:
                            return
                        await asyncio.sleep(0.5)

                # connect_robust()'s automatic reconnection is near-instant
                # against a broker that never actually went away
                await asyncio.wait_for(_wait_for_recovery(), timeout=30)

                await connection.close()
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_a_db_backed_query_recovers_after_its_connection_is_terminated() -> None:
    with PostgresContainer("postgres:16", username="gateway", password="gateway", dbname="gateway") as postgres:
        db_url = (
            f"postgresql+asyncpg://{postgres.username}:{postgres.password}"
            f"@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
        )
        await asyncio.to_thread(_migrate, db_url)
        engine = create_engine(db_url)
        session_factory = create_session_factory(engine)

        try:
            account_id = await _seed_account(session_factory)

            async with session_factory() as session:
                found = (
                    await session.execute(select(AccountRow).where(AccountRow.id == account_id))
                ).scalar_one_or_none()
            assert found is not None

            # Kill every other backend on this database - including
            # whatever connection the pool is currently holding idle
            async with engine.connect() as killer:
                await killer.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    )
                )
                await killer.commit()

            async with session_factory() as session:
                result = (
                    await session.execute(select(AccountRow).where(AccountRow.id == account_id))
                ).scalar_one_or_none()
            assert result is not None
        finally:
            await engine.dispose()
