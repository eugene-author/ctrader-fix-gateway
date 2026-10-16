#!/usr/bin/env bash
# Manual smoke test against a running `docker compose up` stack: subscribe to
# WS /api/v1/ws/prices for one symbol and print whatever quote frames arrive within WAIT_SECONDS.
#
# Uses the project's own `websockets` dependency (already locked via
# `uvicorn[standard]`) through `uv run`
#
# Requires fix-worker to be running and logged on (it needs real cTrader FIX
# credentials in .env before `docker compose up` will bring it up cleanly
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SYMBOL="${SYMBOL:-EURUSD}"
WAIT_SECONDS="${WAIT_SECONDS:-15}"

# An API key's raw value is never recoverable once created (only its HMAC is
# stored - app/core/api_keys.py), so there is no fixed default that stays
# valid across a fresh/reset database. Pass API_KEY=... yourself to reuse an
# existing key; otherwise this creates a fresh one for docker-compose.yml's
# `api` service's own $ACCOUNT_ID every run, exactly like order_commands.sh.
if [ -z "${API_KEY:-}" ]; then
    echo "No API_KEY set - creating a fresh one for \$ACCOUNT_ID ..." >&2
    create_output=$(docker compose exec -T api python -c "
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
                name='test_ws_prices.sh',
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper=settings.api_key_pepper),
                scopes=['orders:write'],
            )
        )
        await session.commit()
    print(f'API_KEY={raw_key}')


asyncio.run(main())
")
    echo "$create_output" >&2
    API_KEY=$(echo "$create_output" | sed -n 's/^API_KEY=//p')
    if [ -z "$API_KEY" ]; then
        echo "Failed to create an API key - is 'docker compose up' running with a seeded account?" >&2
        exit 1
    fi
fi

WS_URL="${BASE_URL/http/ws}/api/v1/ws/prices?api_key=${API_KEY}"

echo "--- 1. GET /prices/${SYMBOL} (sanity check) ---"
curl -sS -H "X-API-Key: ${API_KEY}" "${BASE_URL}/api/v1/prices/${SYMBOL}" | python3 -m json.tool

echo
echo "--- 2. Subscribing to ${SYMBOL} over WS for ${WAIT_SECONDS}s ---"
echo "    (check every ~15s means the connection is alive but no quote"
echo "     has arrived yet; a {\"type\": \"quote\", ...} frame is the real thing)"
echo

uv run python3 - "$WS_URL" "$SYMBOL" "$WAIT_SECONDS" <<'PYEOF'
import asyncio
import json
import sys

import websockets


async def main(url: str, symbol: str, wait_seconds: float) -> None:
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"action": "subscribe", "symbols": [symbol]}))
        try:
            async with asyncio.timeout(wait_seconds):
                while True:
                    frame = json.loads(await ws.recv())
                    print(frame)
        except TimeoutError:
            print(f"-- {wait_seconds}s elapsed, closing --")


asyncio.run(main(sys.argv[1], sys.argv[2], float(sys.argv[3])))
PYEOF
