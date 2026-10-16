"""FIX session lifecycle: connect, logon, reconnect, heartbeat watchdog."""

import asyncio
import enum
import uuid
from collections.abc import Callable, Mapping
from typing import Protocol

import structlog
from ctrader_fix import Heartbeat, LogonRequest, ResponseMessage, TestRequest

from app.core.metrics import fix_messages_total, fix_reconnects_total, fix_session_up

logger = structlog.get_logger()


def _msg_type(message: object) -> str:
    """Read tag 35 off an outbound ctrader_fix message."""
    return str(getattr(message, "_type", "UNKNOWN"))


class ClientProtocol(Protocol):
    """The subset of ctrader_fix.Client's interface SessionManager depends on."""

    def setConnectedCallback(self, callback: Callable[[object], None]) -> None:
        """Register the on-connect callback."""
        ...

    def setDisconnectedCallback(self, callback: Callable[[object, object], None]) -> None:
        """Register the on-disconnect callback."""
        ...

    def setMessageReceivedCallback(self, callback: Callable[[object, ResponseMessage], None]) -> None:
        """Register the on-message callback."""
        ...

    def send(self, message: object) -> object:
        """Send a FIX message."""
        ...

    def startService(self) -> None:
        """Start connecting (and auto-reconnecting on drop)."""
        ...

    def stopService(self) -> object:
        """Stop connecting/reconnecting."""
        ...


class DelayedCallProtocol(Protocol):
    """The subset of IDelayedCall SessionManager depends on."""

    def active(self) -> bool:
        """Report whether this scheduled call has not yet fired or been cancelled."""
        ...

    def cancel(self) -> None:
        """Cancel this scheduled call."""
        ...


class ReactorProtocol(Protocol):
    """The subset of IReactorTime SessionManager depends on - real reactor or a Clock in tests."""

    def callLater(self, delay: float, callable_: Callable[[], None]) -> DelayedCallProtocol:
        """Schedule `callable_` to run after `delay` seconds."""
        ...

    def seconds(self) -> float:
        """Return the current time, in seconds, on this reactor's clock."""
        ...


class SessionState(enum.Enum):
    """One session's lifecycle."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    LOGGING_ON = "LOGGING_ON"
    LOGGED_ON = "LOGGED_ON"
    LOGGING_OUT = "LOGGING_OUT"


class SessionManager:
    """Owns one ctrader_fix.Client-like object and its lifecycle for one session type.

    Reconnection itself is handled by the client. This class tracks state, sends the logon
    and runs a heartbeat watchdog that forces a reconnect if the
    broker goes quiet.
    """

    def __init__(
        self,
        *,
        session_type: str,
        client: ClientProtocol,
        config: Mapping[str, str | int | bool],
        reactor: ReactorProtocol,
        heartbeat_interval: float | None = None,
        on_state_change: Callable[[SessionState], None] | None = None,
        on_message: Callable[[ResponseMessage], None] | None = None,
    ) -> None:
        """Wire up `client`'s callbacks; call start() to actually connect."""
        self.session_type = session_type
        self.state = SessionState.DISCONNECTED

        self._client = client
        self._config = dict(config)
        self._reactor = reactor
        self._heartbeat_interval = heartbeat_interval or float(config.get("HeartBeat", 30))
        self._on_state_change = on_state_change
        self._on_message = on_message

        self._last_message_at: float | None = None
        self._test_request_pending = False
        self._test_request_sent_at: float | None = None
        self._watchdog_call: DelayedCallProtocol | None = None
        self._stopped = True
        self._ready_waiters: list[asyncio.Future[None]] = []

        client.setConnectedCallback(self._handle_connected)
        client.setDisconnectedCallback(self._handle_disconnected)
        client.setMessageReceivedCallback(self._handle_message)

    def start(self) -> None:
        """Start connecting and begin the heartbeat watchdog loop."""
        self._stopped = False
        self._set_state(SessionState.CONNECTING)
        self._client.startService()
        self._schedule_watchdog()

    def stop(self) -> None:
        """Stop reconnecting and cancel the watchdog."""
        self._stopped = True
        if self._watchdog_call is not None and self._watchdog_call.active():
            self._watchdog_call.cancel()
        self._client.stopService()
        self._set_state(SessionState.DISCONNECTED)

    def is_logged_on(self) -> bool:
        """Report whether the session is currently LOGGED_ON."""
        return self.state is SessionState.LOGGED_ON

    def send(self, message: object) -> None:
        """Send a message on this session."""
        self._send(message)

    def _send(self, message: object) -> None:
        fix_messages_total.labels(direction="out", msg_type=_msg_type(message)).inc()
        self._client.send(message)

    async def ready(self) -> None:
        """Resolve once LOGGED_ON. Returns immediately if already logged on."""
        if self.is_logged_on():
            return
        loop = asyncio.get_event_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._ready_waiters.append(future)
        await future

    def _set_state(self, state: SessionState) -> None:
        if self.state is state:
            return
        self.state = state
        if state is SessionState.CONNECTING:
            fix_reconnects_total.labels(session_type=self.session_type).inc()
        fix_session_up.labels(session_type=self.session_type).set(1 if state is SessionState.LOGGED_ON else 0)
        if state is SessionState.LOGGED_ON:
            for future in self._ready_waiters:
                if not future.done():
                    future.set_result(None)
            self._ready_waiters.clear()
        if self._on_state_change is not None:
            self._on_state_change(state)

    def _handle_connected(self, client: object) -> None:
        self._set_state(SessionState.CONNECTED)
        self._set_state(SessionState.LOGGING_ON)
        self._last_message_at = self._reactor.seconds()
        logon = LogonRequest(dict(self._config))
        logon.ResetSeqNum = True
        self._send(logon)

    def _handle_disconnected(self, client: object, reason: object) -> None:
        logger.info("fix_session_disconnected", session_type=self.session_type, reason=str(reason))
        self._set_state(SessionState.DISCONNECTED)
        self._test_request_pending = False

    def _handle_message(self, client: object, message: ResponseMessage) -> None:
        self._last_message_at = self._reactor.seconds()
        self._test_request_pending = False
        msg_type = message.getFieldValue(35)
        fix_messages_total.labels(direction="in", msg_type=str(msg_type)).inc()
        if msg_type == "A":
            self._set_state(SessionState.LOGGED_ON)
        elif msg_type == "1":
            self._reply_to_test_request(message)
        elif msg_type == "5":
            self._set_state(SessionState.DISCONNECTED)
        if self._on_message is not None:
            self._on_message(message)

    def _reply_to_test_request(self, message: ResponseMessage) -> None:
        heartbeat = Heartbeat(dict(self._config))
        test_req_id = message.getFieldValue(112)
        if test_req_id is not None:
            heartbeat.TestReqID = test_req_id
        self._send(heartbeat)

    def _schedule_watchdog(self) -> None:
        if self._stopped:
            return
        self._watchdog_call = self._reactor.callLater(self._heartbeat_interval, self._check_watchdog)

    def _check_watchdog(self) -> None:
        if self._stopped:
            return
        if self.state is not SessionState.LOGGED_ON or self._last_message_at is None:
            self._schedule_watchdog()
            return

        now = self._reactor.seconds()

        if self._test_request_pending:
            sent_at = self._test_request_sent_at
            if sent_at is not None and (now - sent_at) >= self._heartbeat_interval:
                # No reply within HeartBtInt -> force a reconnect.
                self._client.stopService()
                self._client.startService()
                self._test_request_pending = False
                self._schedule_watchdog()
                return
        elif (now - self._last_message_at) >= 2 * self._heartbeat_interval:
            self._test_request_pending = True
            self._test_request_sent_at = now
            request = TestRequest(dict(self._config))
            request.TestReqID = str(uuid.uuid4())
            self._send(request)

        self._schedule_watchdog()
