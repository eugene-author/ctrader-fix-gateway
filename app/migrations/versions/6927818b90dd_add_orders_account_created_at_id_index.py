"""add orders account created_at id index

Revision ID: 6927818b90dd
Revises: e7ed2ba547e6
Create Date: 2026-09-26 19:56:44.958342

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6927818b90dd"
down_revision: str | Sequence[str] | None = "e7ed2ba547e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ix_orders_account_created_at_id",
        "orders",
        ["account_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_orders_account_created_at_id", table_name="orders")
