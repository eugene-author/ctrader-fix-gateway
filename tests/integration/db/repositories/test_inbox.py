from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.unit_of_work import SqlAlchemyUnitOfWork


@pytest.mark.asyncio
async def test_claim_returns_true_the_first_time(session_factory: async_sessionmaker[AsyncSession]) -> None:
    message_id = str(uuid4())

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        was_new = await uow.inbox.claim(consumer="fix-worker", message_id=message_id, payload_hash="hash-1")
        await uow.commit()

    assert was_new is True


@pytest.mark.asyncio
async def test_claim_returns_false_on_a_repeat(session_factory: async_sessionmaker[AsyncSession]) -> None:
    message_id = str(uuid4())

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.inbox.claim(consumer="fix-worker", message_id=message_id, payload_hash="hash-1")
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        was_new = await uow.inbox.claim(consumer="fix-worker", message_id=message_id, payload_hash="hash-1")
        await uow.commit()

    assert was_new is False


@pytest.mark.asyncio
async def test_claim_is_scoped_per_consumer(session_factory: async_sessionmaker[AsyncSession]) -> None:
    message_id = str(uuid4())

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.inbox.claim(consumer="fix-worker", message_id=message_id, payload_hash="hash-1")
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        was_new = await uow.inbox.claim(consumer="projector", message_id=message_id, payload_hash="hash-1")
        await uow.commit()

    assert was_new is True  # same message_id, different consumer -> not a duplicate
