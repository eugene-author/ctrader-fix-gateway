"""add protective order columns

Revision ID: 91b90a3e2c38
Revises: c5d145825678
Create Date: 2026-09-22 01:41:04.556179

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "91b90a3e2c38"
down_revision: str | Sequence[str] | None = "c5d145825678"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "orders",
        sa.Column(
            "protective_kind",
            sa.Enum("STOP_LOSS", "TAKE_PROFIT", name="protectivekind", native_enum=False, length=20),
            nullable=True,
        ),
    )
    op.add_column("orders", sa.Column("protects_position_id", sa.String(length=50), nullable=True))
    op.add_column("orders", sa.Column("parent_order_id", sa.UUID(), nullable=True))
    op.add_column("orders", sa.Column("oco_group_id", sa.UUID(), nullable=True))
    op.create_index("ix_orders_oco_group_id", "orders", ["oco_group_id"], unique=False)
    op.create_foreign_key("fk_orders_parent_order_id_orders", "orders", "orders", ["parent_order_id"], ["id"])
    op.add_column(
        "positions",
        sa.Column(
            "protection_state",
            sa.Enum(
                "NONE", "PENDING", "ACTIVE", "PARTIAL", "FAILED", name="protectionstate", native_enum=False, length=20
            ),
            nullable=False,
            server_default="NONE",
        ),
    )
    op.add_column("positions", sa.Column("stop_loss_order_id", sa.UUID(), nullable=True))
    op.add_column("positions", sa.Column("take_profit_order_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_positions_take_profit_order_id_orders", "positions", "orders", ["take_profit_order_id"], ["id"]
    )
    op.create_foreign_key(
        "fk_positions_stop_loss_order_id_orders", "positions", "orders", ["stop_loss_order_id"], ["id"]
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("fk_positions_stop_loss_order_id_orders", "positions", type_="foreignkey")
    op.drop_constraint("fk_positions_take_profit_order_id_orders", "positions", type_="foreignkey")
    op.drop_column("positions", "take_profit_order_id")
    op.drop_column("positions", "stop_loss_order_id")
    op.drop_column("positions", "protection_state")
    op.drop_constraint("fk_orders_parent_order_id_orders", "orders", type_="foreignkey")
    op.drop_index("ix_orders_oco_group_id", table_name="orders")
    op.drop_column("orders", "oco_group_id")
    op.drop_column("orders", "parent_order_id")
    op.drop_column("orders", "protects_position_id")
    op.drop_column("orders", "protective_kind")
