"""fix-worker's price path: quotes reach Redis and the quotes:{symbol} pub/sub channel.

Same subprocess-against-the-mock-server approach as test_fix_worker_process.py,
scripted with a bid and an offer for the discovered symbol.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
import redis.asyncio as redis

from tests.mocks.fix_server import MockFixServer

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _read_until(process: subprocess.Popen[str], needle: str, timeout: float) -> str:
    """Read until `needle` appears, then keep draining stdout for the rest of the process's life.

    A thread that stops reading once it finds `needle` leaves the pipe
    unread from then on - once the OS pipe buffer fills (default 64KB on
    Linux), the child's next write() blocks, freezing it.
    """
    lines: list[str] = []
    found = threading.Event()
    prefix: list[str] = []

    def _pump() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.append(line)
            if not found.is_set() and needle in line:
                prefix.extend(lines)
                found.set()

    thread = threading.Thread(target=_pump, daemon=True)
    thread.start()
    found.wait(timeout=timeout)
    return "".join(prefix)


@pytest.mark.asyncio
async def test_fix_worker_writes_quotes_to_redis_and_publishes_them(
    migrated_postgres_url: str, rabbitmq_url: str, redis_url: str, redis_client: redis.Redis
) -> None:
    server = MockFixServer()
    # Keyed by the wire symbol id (tag 55) the mock server's default security
    # list reply uses ("1" -> EURUSD, see MockFixServerScript.security_list).
    server.script.quotes["1"] = [
        [(269, "0"), (270, "1.08231")],  # bid
        [(269, "1"), (270, "1.08245")],  # offer
    ]
    port = server.start()
    process: subprocess.Popen[str] | None = None
    account_id = str(uuid.uuid4())
    pubsub = redis_client.pubsub()
    try:
        # Subscribe before fix-worker starts, so we can't miss the publish.
        await pubsub.subscribe("quotes:EURUSD")

        env = {
            **os.environ,
            "APP_ENV": "dev",
            "APP_LOG_LEVEL": "INFO",
            "DATABASE_URL": migrated_postgres_url,
            "RABBITMQ_URL": rabbitmq_url,
            "REDIS_URL": redis_url,
            "ACCOUNT_ID": account_id,
            "FIX_TRADE_HOST": "127.0.0.1",
            "FIX_TRADE_PORT": str(port),
            "FIX_TRADE_SSL": "false",  # the mock server speaks plaintext, not TLS
            "FIX_TRADE_USERNAME": "1",
            "FIX_TRADE_PASSWORD": "pw",
            "FIX_TRADE_SENDER_COMP_ID": "test.client",
            "FIX_QUOTE_HOST": "127.0.0.1",
            "FIX_QUOTE_PORT": str(port),
            "FIX_QUOTE_SSL": "false",
            "FIX_QUOTE_USERNAME": "1",
            "FIX_QUOTE_PASSWORD": "pw",
            "FIX_QUOTE_SENDER_COMP_ID": "test.client",
            "GRACEFUL_TIMEOUT": "5",
        }
        process = subprocess.Popen(  # noqa: ASYNC220
            [sys.executable, "-u", "-m", "app.workers.fix_worker"],
            cwd=_PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        startup_output = _read_until(process, "security_list_received", timeout=20)
        assert "security_list_received" in startup_output, startup_output

        cached: dict[str, str] | None = None
        async with asyncio.timeout(15):
            while not cached:
                cached = await redis_client.hgetall(f"price:{account_id}:EURUSD")
                if not cached:
                    await asyncio.sleep(0.1)
        assert Decimal(cached["bid"]) == Decimal("1.08231")
        assert Decimal(cached["ask"]) == Decimal("1.08245")

        published = None
        async with asyncio.timeout(15):
            while published is None:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is not None:
                    published = json.loads(message["data"])
        assert published["symbol"] == "EURUSD"
        assert Decimal(published["bid"]) == Decimal("1.08231")
        assert Decimal(published["ask"]) == Decimal("1.08245")
    finally:
        await pubsub.aclose()
        if process is not None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        server.stop()
