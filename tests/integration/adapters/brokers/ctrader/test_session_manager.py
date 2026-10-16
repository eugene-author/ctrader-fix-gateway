import threading

import crochet
from twisted.internet import reactor

from app.adapters.brokers.ctrader.client import create_client
from app.adapters.brokers.ctrader.session_manager import SessionManager, SessionState
from tests.mocks.fix_server import MockFixServer

_CONFIG = {
    "SSL": False,
    "BeginString": "FIX.4.4",
    "SenderCompID": "test.client",
    "TargetCompID": "SERVER",
    "TargetSubID": "TRADE",
    "SenderSubID": "TRADE",
    "Username": "1",
    "Password": "pw",
    "HeartBeat": "30",
}


@crochet.wait_for(timeout=5.0)
def _start_manager(manager: SessionManager) -> None:
    manager.start()


@crochet.wait_for(timeout=5.0)
def _stop_manager(manager: SessionManager) -> None:
    manager.stop()


def test_session_manager_reaches_logged_on_against_the_mock_server() -> None:
    server = MockFixServer()
    port = server.start()
    config = {**_CONFIG, "Host": "127.0.0.1", "Port": port}
    client = create_client(config)
    logged_on = threading.Event()

    def _on_state_change(state: SessionState) -> None:
        if state is SessionState.LOGGED_ON:
            logged_on.set()

    manager = SessionManager(
        session_type="TRADE", client=client, config=config, reactor=reactor, on_state_change=_on_state_change
    )

    try:
        _start_manager(manager)
        assert logged_on.wait(timeout=5)
    finally:
        _stop_manager(manager)
        server.stop()


def test_session_manager_reconnects_after_the_server_drops_the_connection() -> None:
    server = MockFixServer()
    server.script.drop_after_logon = True
    port = server.start()
    config = {**_CONFIG, "Host": "127.0.0.1", "Port": port}
    client = create_client(config, reconnect_max_delay=1.0)

    logon_count = 0
    reconnected = threading.Event()

    def _on_state_change(state: SessionState) -> None:
        nonlocal logon_count
        if state is SessionState.LOGGED_ON:
            logon_count += 1
            if logon_count >= 2:
                reconnected.set()

    manager = SessionManager(
        session_type="TRADE", client=client, config=config, reactor=reactor, on_state_change=_on_state_change
    )

    try:
        _start_manager(manager)
        assert reconnected.wait(timeout=10)
    finally:
        _stop_manager(manager)
        server.stop()
