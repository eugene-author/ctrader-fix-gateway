from twisted.internet.task import Clock

from app.adapters.brokers.ctrader.session_manager import SessionManager, SessionState

_CONFIG = {
    "HeartBeat": "30",
    "SenderCompID": "a",
    "TargetCompID": "b",
    "TargetSubID": "TRADE",
    "SenderSubID": "TRADE",
    "BeginString": "FIX.4.4",
    "Username": "1",
    "Password": "pw",
}


class MockMessage:
    def __init__(self, fields: dict[int, str]) -> None:
        self._fields = fields

    def getFieldValue(self, tag: int) -> str | None:
        return self._fields.get(tag)


class MockClient:
    """A ClientProtocol double: scriptable via simulate_*(), records send()/service calls."""

    def __init__(self) -> None:
        self.sent: list[object] = []
        self.start_count = 0
        self.stop_count = 0
        self._connected_cb = None
        self._disconnected_cb = None
        self._message_cb = None

    def setConnectedCallback(self, callback) -> None:
        self._connected_cb = callback

    def setDisconnectedCallback(self, callback) -> None:
        self._disconnected_cb = callback

    def setMessageReceivedCallback(self, callback) -> None:
        self._message_cb = callback

    def send(self, message: object) -> None:
        self.sent.append(message)

    def startService(self) -> None:
        self.start_count += 1

    def stopService(self) -> None:
        self.stop_count += 1

    def simulate_connect(self) -> None:
        self._connected_cb(self)

    def simulate_disconnect(self, reason: object = None) -> None:
        self._disconnected_cb(self, reason)

    def simulate_message(self, fields: dict[int, str]) -> None:
        self._message_cb(self, MockMessage(fields))


def _manager(client: MockClient, clock: Clock, **kwargs: object) -> SessionManager:
    return SessionManager(session_type="TRADE", client=client, config=_CONFIG, reactor=clock, **kwargs)


def test_connect_sends_logon_and_transitions_to_logging_on() -> None:
    client = MockClient()
    manager = _manager(client, Clock())

    manager.start()
    client.simulate_connect()

    assert manager.state is SessionState.LOGGING_ON
    assert len(client.sent) == 1


def test_logon_always_sets_reset_seq_num_flag() -> None:
    # This process never persists FIX sequence numbers across restarts - a
    # fresh ctrader_fix.Client every time - so every logon is effectively
    # a new session and must set tag 141. A broker expecting a
    # continued sequence when this is missing may not send an explicit
    # reject at all, just silently close the connection right after
    # accepting it.
    client = MockClient()
    manager = _manager(client, Clock())

    manager.start()
    client.simulate_connect()

    logon = client.sent[0]
    logon.delimiter = "|"
    assert "141=Y" in logon.getMessage(1)


def test_logon_ack_transitions_to_logged_on() -> None:
    client = MockClient()
    manager = _manager(client, Clock())

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})

    assert manager.state is SessionState.LOGGED_ON
    assert manager.is_logged_on() is True


def test_disconnect_transitions_to_disconnected() -> None:
    client = MockClient()
    manager = _manager(client, Clock())

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})
    client.simulate_disconnect()

    assert manager.state is SessionState.DISCONNECTED


def test_state_change_callback_sees_every_transition_in_order() -> None:
    client = MockClient()
    seen: list[SessionState] = []
    manager = _manager(client, Clock(), on_state_change=seen.append)

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})

    assert seen == [
        SessionState.CONNECTING,
        SessionState.CONNECTED,
        SessionState.LOGGING_ON,
        SessionState.LOGGED_ON,
    ]


def test_test_request_is_answered_with_a_heartbeat_echoing_test_req_id() -> None:
    client = MockClient()
    manager = _manager(client, Clock())

    manager.start()
    client.simulate_connect()
    client.sent.clear()
    client.simulate_message({35: "1", 112: "abc-123"})

    assert len(client.sent) == 1
    assert client.sent[0].TestReqID == "abc-123"


def test_on_message_callback_receives_every_message() -> None:
    client = MockClient()
    seen: list[object] = []
    manager = _manager(client, Clock(), on_message=seen.append)

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})
    client.simulate_message({35: "8", 11: "abc"})

    assert len(seen) == 2


def test_watchdog_sends_test_request_after_two_heartbeat_intervals_of_silence() -> None:
    client = MockClient()
    clock = Clock()
    manager = _manager(client, clock)

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})
    client.sent.clear()

    clock.advance(30)  # elapsed=30s < 2*30 -> nothing yet
    assert client.sent == []

    clock.advance(30)  # elapsed=60s >= 2*30 -> TestRequest sent
    assert len(client.sent) == 1


def test_watchdog_forces_reconnect_when_test_request_gets_no_reply() -> None:
    client = MockClient()
    clock = Clock()
    manager = _manager(client, clock)

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})

    clock.advance(60)  # triggers the TestRequest
    clock.advance(30)  # no reply within one more HeartBtInt -> forced reconnect

    assert client.stop_count == 1
    assert client.start_count == 2  # initial start() + the forced restart


def test_watchdog_clears_pending_test_request_on_any_inbound_message() -> None:
    client = MockClient()
    clock = Clock()
    manager = _manager(client, clock)

    manager.start()
    client.simulate_connect()
    client.simulate_message({35: "A"})

    clock.advance(60)  # triggers the TestRequest
    client.simulate_message({35: "0"})  # any traffic proves liveness
    clock.advance(30)  # would have forced a reconnect, but shouldn't now

    assert client.stop_count == 0


def test_stop_cancels_the_watchdog_and_stops_the_client() -> None:
    client = MockClient()
    manager = _manager(client, Clock())

    manager.start()
    manager.stop()

    assert client.stop_count == 1
    assert manager.state is SessionState.DISCONNECTED
