"""add last broker transact at to orders

Revision ID: 8e4e74a84ef0
Revises: 6927818b90dd
Create Date: 2026-09-26 23:11:11.635261

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8e4e74a84ef0"
down_revision: str | Sequence[str] | None = "6927818b90dd"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "orders",
        sa.Column("last_broker_transact_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("orders", "last_broker_transact_at")
