"""add trace context column to outbox events

Revision ID: f1f6d3719333
Revises: 91b90a3e2c38
Create Date: 2026-09-22 06:37:32.682542

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f1f6d3719333"
down_revision: str | Sequence[str] | None = "91b90a3e2c38"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("outbox_events", sa.Column("trace_context", postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("outbox_events", "trace_context")
