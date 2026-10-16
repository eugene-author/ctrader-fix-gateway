"""rename api key hash to hmac

Revision ID: ce38c472b90c
Revises: a2c9e6f1b3d4
Create Date: 2026-09-26 19:27:59.760104

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ce38c472b90c"
down_revision: str | Sequence[str] | None = "a2c9e6f1b3d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema.

    api_clients.api_key_hash (Argon2, String(200)) -> api_key_hmac
    (HMAC-SHA256 hex digest, always 64 chars - app/core/api_keys.py). No
    dual-scheme fallback: there are no deployed keys, so existing rows (if
    any) just get re-seeded, not converted.
    """
    op.alter_column("api_clients", "api_key_hash", new_column_name="api_key_hmac", type_=sa.String(length=64))


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column("api_clients", "api_key_hmac", new_column_name="api_key_hash", type_=sa.String(length=200))
