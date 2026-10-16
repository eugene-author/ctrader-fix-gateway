"""widen symbols name column

Revision ID: a2c9e6f1b3d4
Revises: f1f6d3719333
Create Date: 2026-09-22 11:05:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a2c9e6f1b3d4"
down_revision: str | Sequence[str] | None = "f1f6d3719333"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column("symbols", "name", existing_type=sa.String(length=20), type_=sa.String(length=100))


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column("symbols", "name", existing_type=sa.String(length=100), type_=sa.String(length=20))
