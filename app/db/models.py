"""SQLAlchemy ORM models for the tables."""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, new_uuid7
from app.domain.enums import (
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionStatus,
    ProtectionState,
    ProtectiveKind,
    Side,
    SymbolSource,
    TimeInForce,
)

_MONEY = Numeric(28, 10)
_QTY = Numeric(28, 8)


def _enum_column(enum_cls: type) -> SqlEnum:
    return SqlEnum(enum_cls, native_enum=False, validate_strings=True, length=20)


class AccountRow(Base, TimestampMixin):
    """A cTrader trading account."""

    __tablename__ = "accounts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    broker: Mapped[str] = mapped_column(String(50))
    external_account_id: Mapped[str] = mapped_column(String(100))
    mode: Mapped[str] = mapped_column(String(20))
    currency: Mapped[str] = mapped_column(String(10))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class ApiClientRow(Base, TimestampMixin):
    """A REST client's API key, HMAC-SHA256 hashed (app/core/api_keys.py)."""

    __tablename__ = "api_clients"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    name: Mapped[str] = mapped_column(String(100))
    key_prefix: Mapped[str] = mapped_column(String(16), index=True)
    api_key_hmac: Mapped[str] = mapped_column(String(64))
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SymbolRow(Base, TimestampMixin):
    """A tradable symbol and its trading constraints."""

    __tablename__ = "symbols"
    __table_args__ = (
        UniqueConstraint("account_id", "name", name="uq_symbols_account_name"),
        UniqueConstraint("account_id", "broker_symbol_id", name="uq_symbols_account_broker_symbol_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    name: Mapped[str] = mapped_column(String(100))
    broker_symbol_id: Mapped[int | None] = mapped_column(nullable=True)
    digits: Mapped[int | None] = mapped_column(nullable=True)
    lot_size: Mapped[Decimal | None] = mapped_column(_QTY, nullable=True)
    qty_step: Mapped[Decimal | None] = mapped_column(_QTY, nullable=True)
    min_qty: Mapped[Decimal | None] = mapped_column(_QTY, nullable=True)
    max_qty: Mapped[Decimal | None] = mapped_column(_QTY, nullable=True)
    price_step: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    subscribe: Mapped[bool] = mapped_column(Boolean, default=True)
    needs_review: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[SymbolSource | None] = mapped_column(_enum_column(SymbolSource), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OrderRow(Base, TimestampMixin):
    """The Order model."""

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("account_id", "client_order_id", name="uq_orders_account_client_order_id"),
        UniqueConstraint("fix_cl_ord_id", name="uq_orders_fix_cl_ord_id"),
        Index("ix_orders_account_status", "account_id", "status"),
        Index("ix_orders_position_id", "position_id"),
        Index("ix_orders_created_at", "created_at"),
        Index("ix_orders_oco_group_id", "oco_group_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    client_order_id: Mapped[str] = mapped_column(String(64))
    fix_cl_ord_id: Mapped[str | None] = mapped_column(String(30), nullable=True)
    orig_cl_ord_id: Mapped[str | None] = mapped_column(String(30), nullable=True)
    ctrader_order_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    position_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    symbol_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("symbols.id"))
    side: Mapped[Side] = mapped_column(_enum_column(Side))
    order_type: Mapped[OrderType] = mapped_column(_enum_column(OrderType))
    time_in_force: Mapped[TimeInForce] = mapped_column(_enum_column(TimeInForce), default=TimeInForce.GTC)
    quantity: Mapped[Decimal] = mapped_column(_QTY)
    filled_quantity: Mapped[Decimal] = mapped_column(_QTY, default=Decimal(0))
    avg_fill_price: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    limit_price: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    stop_price: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    stop_loss: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    take_profit: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    status: Mapped[OrderStatus] = mapped_column(_enum_column(OrderStatus))
    reject_reason_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    purpose: Mapped[OrderPurpose] = mapped_column(_enum_column(OrderPurpose))
    closes_position_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    protective_kind: Mapped[ProtectiveKind | None] = mapped_column(_enum_column(ProtectiveKind), nullable=True)
    protects_position_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    parent_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=True
    )
    oco_group_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_broker_transact_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(default=1)
    __mapper_args__ = {"version_id_col": version}  # noqa: RUF012
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    executions: Mapped[list[ExecutionRow]] = relationship(back_populates="order")


class ExecutionRow(Base):
    """An immutable log entry, one row per ExecutionReport."""

    __tablename__ = "executions"
    __table_args__ = (UniqueConstraint("account_id", "exec_id", name="uq_executions_account_exec_id"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    order_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=True)
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    exec_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    exec_type: Mapped[str] = mapped_column(String(5))
    ord_status: Mapped[str] = mapped_column(String(5))
    last_qty: Mapped[Decimal | None] = mapped_column(_QTY, nullable=True)
    last_price: Mapped[Decimal | None] = mapped_column(_MONEY, nullable=True)
    cum_qty: Mapped[Decimal] = mapped_column(_QTY)
    leaves_qty: Mapped[Decimal] = mapped_column(_QTY)
    position_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    transact_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_message: Mapped[str] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    order: Mapped[OrderRow | None] = relationship(back_populates="executions")


class PositionRow(Base):
    """A projection."""

    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("account_id", "position_id", name="uq_positions_account_position_id"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    position_id: Mapped[str] = mapped_column(String(50))
    symbol_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("symbols.id"))
    side: Mapped[Side] = mapped_column(_enum_column(Side))
    quantity: Mapped[Decimal] = mapped_column(_QTY)
    entry_price: Mapped[Decimal] = mapped_column(_MONEY)
    status: Mapped[PositionStatus] = mapped_column(_enum_column(PositionStatus))
    opened_by_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=True
    )
    closed_by_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=True
    )
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    protection_state: Mapped[ProtectionState] = mapped_column(
        _enum_column(ProtectionState), default=ProtectionState.NONE
    )
    stop_loss_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=True
    )
    take_profit_order_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id"), nullable=True
    )
    version: Mapped[int] = mapped_column(default=1)
    __mapper_args__ = {"version_id_col": version}  # noqa: RUF012


class OutboxEventRow(Base):
    """The transactional outbox."""

    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_events_unpublished", "available_at", postgresql_where="published_at IS NULL"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid7)
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.id"))
    aggregate_type: Mapped[str] = mapped_column(String(50))
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    routing_key: Mapped[str] = mapped_column(String(100))
    correlation_id: Mapped[str] = mapped_column(String(100))
    causation_id: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    trace_context: Mapped[dict[str, str] | None] = mapped_column(JSONB, nullable=True)


class InboxEventRow(Base):
    """At-least-once delivery deduplication.

    A handler inserts here first, inside its transaction.
    """

    __tablename__ = "inbox_events"

    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    message_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    payload_hash: Mapped[str] = mapped_column(String(64))
