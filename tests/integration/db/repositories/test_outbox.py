from uuid import uuid4

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, OutboxEventRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork


@pytest.mark.asyncio
async def test_outbox_repository_persists_an_event(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    correlation_id = str(uuid4())

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.outbox.add(
            account_id=account.id,
            aggregate_type="order",
            aggregate_id=aggregate_id,
            event_type="CreateOrderCommand",
            payload={"order_id": str(aggregate_id), "quantity": "1000"},
            routing_key="order.create",
            correlation_id=correlation_id,
            causation_id=correlation_id,
        )
        await uow.commit()

    async with session_factory() as session:
        row = (
            await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == aggregate_id))
        ).scalar_one()

    assert row.aggregate_type == "order"
    assert row.event_type == "CreateOrderCommand"
    assert row.payload == {"order_id": str(aggregate_id), "quantity": "1000"}
    assert row.routing_key == "order.create"
    assert row.correlation_id == correlation_id
    assert row.published_at is None
    assert row.attempts == 0


@pytest.mark.asyncio
async def test_outbox_repository_accepts_a_non_uuid_correlation_id(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    # X-Correlation-Id is an arbitrary client-supplied value, not
    # guaranteed to be a UUID.
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.outbox.add(
            account_id=account.id,
            aggregate_type="order",
            aggregate_id=aggregate_id,
            event_type="CreateOrderCommand",
            payload={},
            routing_key="order.create",
            correlation_id="client-supplied-trace-42",
            causation_id="client-supplied-trace-42",
        )
        await uow.commit()

    async with session_factory() as session:
        row = (
            await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == aggregate_id))
        ).scalar_one()

    assert row.correlation_id == "client-supplied-trace-42"


@pytest.mark.asyncio
async def test_outbox_repository_captures_the_ambient_trace_context(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    tracer = TracerProvider().get_tracer(__name__)

    with tracer.start_as_current_span("http.request"):
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.outbox.add(
                account_id=account.id,
                aggregate_type="order",
                aggregate_id=aggregate_id,
                event_type="CreateOrderCommand",
                payload={},
                routing_key="order.create",
                correlation_id="corr-1",
                causation_id="corr-1",
            )
            await uow.commit()

    async with session_factory() as session:
        row = (
            await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == aggregate_id))
        ).scalar_one()

    assert row.trace_context is not None
    assert "traceparent" in row.trace_context


@pytest.mark.asyncio
async def test_outbox_repository_leaves_trace_context_null_with_no_active_span(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    # trace.get_current_span() outside any start_as_current_span() block
    # yields an INVALID_SPAN - nothing for the W3C propagator to serialize.
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    assert trace.get_current_span().get_span_context().is_valid is False

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.outbox.add(
            account_id=account.id,
            aggregate_type="order",
            aggregate_id=aggregate_id,
            event_type="CreateOrderCommand",
            payload={},
            routing_key="order.create",
            correlation_id="corr-2",
            causation_id="corr-2",
        )
        await uow.commit()

    async with session_factory() as session:
        row = (
            await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == aggregate_id))
        ).scalar_one()

    assert row.trace_context is None
