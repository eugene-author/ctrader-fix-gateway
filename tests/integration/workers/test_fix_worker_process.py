"""Runs fix_worker.py as a real subprocess against the mock FIX server.

fix_worker.py installs asyncioreactor at import time (see its module
docstring), so it can never be imported into this shared pytest process -
ctrader_fix already installed Twisted's default reactor earlier via other
test modules, and a process only gets one reactor install. A real subprocess
is the only way to exercise its own wiring (settings, DB engine, AMQP
connect, TRADE session connect/logon, graceful SIGTERM shutdown) end to end.
"""

import os
import signal
import subprocess
import sys
import threading
import uuid
from pathlib import Path

from tests.mocks.fix_server import MockFixServer

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _read_until(process: subprocess.Popen[str], needle: str, timeout: float) -> str:
    """Collect stdout lines until `needle` appears or `timeout` elapses, then keep draining the pipe.

    A thread that stops reading once it finds `needle` leaves the pipe
    unread from then on - once the OS pipe buffer fills (default 64KB on
    Linux), the child's next write() blocks, freezing it. The prefix up to
    `needle` is still what's returned/asserted on; everything after is
    drained and discarded, just to keep the pipe from backing up.
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


def test_fix_worker_connects_logs_on_and_shuts_down_cleanly_on_sigterm(
    migrated_postgres_url: str, rabbitmq_url: str, redis_url: str
) -> None:
    server = MockFixServer()
    port = server.start()
    process: subprocess.Popen[str] | None = None
    try:
        env = {
            **os.environ,
            "APP_ENV": "dev",
            "APP_LOG_LEVEL": "INFO",
            "DATABASE_URL": migrated_postgres_url,
            "RABBITMQ_URL": rabbitmq_url,
            "REDIS_URL": redis_url,
            "ACCOUNT_ID": str(uuid.uuid4()),
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
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", "app.workers.fix_worker"],
            cwd=_PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        startup_output = _read_until(process, "security_list_received", timeout=20)
        assert "trade_session_logged_on" in startup_output, startup_output
        assert "quote_session_logged_on" in startup_output, startup_output
        assert "security_list_received" in startup_output, startup_output

        process.send_signal(signal.SIGTERM)
        returncode = process.wait(timeout=20)

        assert returncode == 0
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        server.stop()


def test_fix_worker_exits_non_zero_when_rabbitmq_is_unreachable_at_startup(
    migrated_postgres_url: str, redis_url: str
) -> None:
    """A startup failure must crash the process, not hang around "healthy" but dead.

    Regression test: _run() is scheduled via a bare asyncio.ensure_future()
    under reactor.callWhenRunning(), unlike relay/projector's plain
    asyncio.run(). An exception there used to just vanish into asyncio's
    "exception was never retrieved" warning while reactor.run() spun forever
    with nothing actually running - the container stayed "Up"/"healthy" (the
    metrics server starts independently) with a permanently dead FIX worker.
    Observed live: fix-worker lost the startup race against a RabbitMQ that
    wasn't reachable yet on `docker compose up`. The process must now exit
    non-zero so Docker's `restart: on-failure` retries it, like relay/projector.
    """
    process: subprocess.Popen[str] | None = None
    try:
        env = {
            **os.environ,
            "APP_ENV": "dev",
            "APP_LOG_LEVEL": "INFO",
            "DATABASE_URL": migrated_postgres_url,
            "RABBITMQ_URL": "amqp://guest:guest@127.0.0.1:1/",  # nothing listens here
            "REDIS_URL": redis_url,
            "ACCOUNT_ID": str(uuid.uuid4()),
            "FIX_TRADE_HOST": "127.0.0.1",
            "FIX_TRADE_PORT": "1",
            "FIX_TRADE_SSL": "false",
            "FIX_TRADE_USERNAME": "1",
            "FIX_TRADE_PASSWORD": "pw",
            "FIX_TRADE_SENDER_COMP_ID": "test.client",
            "FIX_QUOTE_HOST": "127.0.0.1",
            "FIX_QUOTE_PORT": "1",
            "FIX_QUOTE_SSL": "false",
            "FIX_QUOTE_USERNAME": "1",
            "FIX_QUOTE_PASSWORD": "pw",
            "FIX_QUOTE_SENDER_COMP_ID": "test.client",
            "GRACEFUL_TIMEOUT": "5",
        }
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", "app.workers.fix_worker"],
            cwd=_PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        returncode = process.wait(timeout=20)

        assert returncode != 0
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
