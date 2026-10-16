#!/usr/bin/env bash
# Manual smoke test against a running `docker compose up` stack: get a price,
# place a market order, poll it to FILLED, then close the resulting position.
# Requires fix-worker to be running and logged on (it needs real cTrader FIX
# credentials in .env before `docker compose up` will bring it up cleanly -
# see README's "Running the FIX worker" section) and at least one symbol
# discovered and reviewed
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SYMBOL="${SYMBOL:-EURUSD}"
QUANTITY="${QUANTITY:-1000}"
SIDE="${SIDE:-SELL}"
POLL_ATTEMPTS="${POLL_ATTEMPTS:-10}"
POLL_DELAY_SECONDS="${POLL_DELAY_SECONDS:-5}"
STEP_DELAY_SECONDS="${STEP_DELAY_SECONDS:-2}"

# An API key's raw value is never recoverable once created (only its HMAC is
# stored - app/core/api_keys.py), so there is no fixed default that stays
# valid across a fresh/reset database. Pass API_KEY=... yourself to reuse an
# existing key; otherwise this creates a fresh one for docker-compose.yml's
# `api` service's own $ACCOUNT_ID every run.
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
                name='test_commands.sh',
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

curl_json() {
    curl -sS -H "X-API-Key: ${API_KEY}" -H "Content-Type: application/json" "$@"
}

poll_status_until_terminal() {
    local status_url="$1"
    local label="$2"
    local response status
    for ((i = 1; i <= POLL_ATTEMPTS; i++)); do
        response=$(curl_json "${BASE_URL}${status_url}")
        status=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))")
        echo "[$i] ${label}: ${response}" >&2
        case "$status" in
            FILLED | REJECTED | FAILED | CANCELLED)
                echo "$response"
                return 0
                ;;
        esac
        sleep "$POLL_DELAY_SECONDS"
    done
    echo "${label} did not reach a terminal state after ${POLL_ATTEMPTS} attempts" >&2
    return 1
}

echo "--- 1. GET /prices/${SYMBOL} ---"
curl_json "${BASE_URL}/api/v1/prices/${SYMBOL}" | python3 -m json.tool
sleep "$STEP_DELAY_SECONDS"

echo
echo "--- 2. POST /orders (market ${SIDE}, ${QUANTITY} units of ${SYMBOL}) ---"
client_order_id="demo-order-$(date +%s)"
order_response=$(curl_json -X POST "${BASE_URL}/api/v1/orders" -d "{
    \"client_order_id\": \"${client_order_id}\",
    \"symbol\": \"${SYMBOL}\",
    \"side\": \"${SIDE}\",
    \"order_type\": \"MARKET\",
    \"quantity\": ${QUANTITY},
    \"time_in_force\": \"IOC\"
}")
echo "$order_response" | python3 -m json.tool
order_id=$(echo "$order_response" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
sleep "$STEP_DELAY_SECONDS"

echo
echo "--- 3. Poll GET /orders/${order_id}/status until FILLED ---"
order_final=$(poll_status_until_terminal "/api/v1/orders/${order_id}/status" "order")
position_id=$(echo "$order_final" | python3 -c "import sys,json; print(json.load(sys.stdin).get('position_id') or '')")

if [ -z "$position_id" ]; then
    echo "No position_id on the order (order was not filled) - nothing to close." >&2
    exit 1
fi
sleep "$STEP_DELAY_SECONDS"

echo
echo "--- 4. POST /positions/${position_id}/close ---"
close_response=$(curl_json -X POST "${BASE_URL}/api/v1/positions/${position_id}/close" -d '{}')
echo "$close_response" | python3 -m json.tool
close_order_id=$(echo "$close_response" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
sleep "$STEP_DELAY_SECONDS"

echo
echo "--- 5. Poll GET /orders/${close_order_id}/status until FILLED ---"
poll_status_until_terminal "/api/v1/orders/${close_order_id}/status" "close order" >/dev/null
sleep "$STEP_DELAY_SECONDS"

echo
echo "--- 6. GET /positions/${position_id} (should be CLOSED) ---"
curl_json "${BASE_URL}/api/v1/positions/${position_id}" | python3 -m json.tool
