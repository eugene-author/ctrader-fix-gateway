"""add correlation id to orders

Revision ID: e7ed2ba547e6
Revises: ce38c472b90c
Create Date: 2026-09-26 19:49:54.777868

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e7ed2ba547e6"
down_revision: str | Sequence[str] | None = "ce38c472b90c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("orders", sa.Column("correlation_id", sa.String(length=100), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("orders", "correlation_id")
