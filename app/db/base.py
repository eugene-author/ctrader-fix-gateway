"""Declarative base and shared column mixins for SQLAlchemy models."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base every ORM model in app.db.models inherits from."""


def new_uuid7() -> uuid.UUID:
    """Primary keys are UUIDv7 so they sort by creation time."""
    return uuid.uuid7()


class TimestampMixin:
    """Adds server side created_at/updated_at columns."""

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
