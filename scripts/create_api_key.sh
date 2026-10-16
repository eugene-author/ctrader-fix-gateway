#!/usr/bin/env bash
# Creates a fresh API key against a running `docker compose up` stack and print
# it to the console. Requires the `api` service running with `ACCOUNT_ID` set
# in `.env` and a matching row already in the `accounts` table
#
set -euo pipefail

NAME="${NAME:-create_api_key.sh}"
SCOPES="${SCOPES:-orders:write}"

IFS=',' read -r -a scopes_array <<<"$SCOPES"
scopes_python=$(printf "'%s', " "${scopes_array[@]}")

echo "Creating a fresh API key for \$ACCOUNT_ID (name=${NAME}, scopes=${SCOPES}) ..." >&2

create_output=$(docker compose exec -T -e NAME="$NAME" api python -c "
import asyncio, os
from app.core.api_keys import generate_api_key, hash_api_key, key_prefix
from app.core.config import Settings
from app.db.models import ApiClientRow
from app.db.session import create_engine, create_session_factory


async def main() -> None:
    settings = Settings()
    session_factory = create_session_factory(create_engine(settings.database_url))
    raw_key = generate_api_key()
    async with session_factory() as session:
        session.add(
            ApiClientRow(
                account_id=os.environ['ACCOUNT_ID'],
                name=os.environ['NAME'],
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper=settings.api_key_pepper),
                scopes=[${scopes_python}],
            )
        )
        await session.commit()
    print(f'API_KEY={raw_key}')


asyncio.run(main())
")

api_key=$(echo "$create_output" | sed -n 's/^API_KEY=//p')
if [ -z "$api_key" ]; then
    echo "Failed to create an API key - is 'docker compose up' running with a seeded account?" >&2
    echo "$create_output" >&2
    exit 1
fi

echo "API_KEY=${api_key}"
