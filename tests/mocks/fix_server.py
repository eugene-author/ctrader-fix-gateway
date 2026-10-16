"""A minimal FIX 4.4 mock server for integration testing.

FIX 4.4 subset: accepts a logon, answers SecurityListRequest
and MarketDataRequest, and answers NewOrderSingle with either a scripted
sequence of execution reports or a sensible default fill. Can be told to
reject a logon, go silent, or drop the connection after logon.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime

import crochet
from twisted.internet import reactor
from twisted.internet.protocol import Factory, Protocol

crochet.setup()

SOH = "\x01"


def parse_fields(raw: str) -> dict[int, str]:
    """Parse a raw SOH-delimited FIX message into a {tag: value} dict."""
    fields: dict[int, str] = {}
    for chunk in raw.split(SOH):
        if "=" not in chunk:
            continue
        tag, _, value = chunk.partition("=")
        fields[int(tag)] = value
    return fields


def _checksum(data: str) -> str:
    return str(sum(data.encode("ascii")) % 256).zfill(3)


def build_message(
    msg_type: str,
    fields: list[tuple[int, str]],
    *,
    seq_num: int,
    sender_comp_id: str,
    target_comp_id: str,
) -> str:
    """Build a raw FIX 4.4 message, computing BodyLength (9) and CheckSum (10)."""
    sending_time = datetime.now(UTC).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
    body_fields = [
        f"35={msg_type}",
        f"49={sender_comp_id}",
        f"56={target_comp_id}",
        f"34={seq_num}",
        f"52={sending_time}",
        *(f"{tag}={value}" for tag, value in fields),
    ]
    body = SOH.join(body_fields) + SOH
    header = f"8=FIX.4.4{SOH}9={len(body)}{SOH}"
    checksum = _checksum(header + body)
    return f"{header}{body}10={checksum}{SOH}"


@dataclass
class MockFixServerScript:
    """Per-server scripted behavior, mutated by tests before or between sends."""

    reject_logon: bool = False
    drop_after_logon: bool = False
    go_silent: bool = False
    # cl_ord_id -> ordered list of ExecutionReport field-lists to send verbatim.
    execution_reports: dict[str, list[list[tuple[int, str]]]] = field(default_factory=dict)
    # None -> a single default EURUSD entry.
    security_list: list[tuple[int, str]] | None = None
    # broker_symbol_id (as sent in tag 55) -> field-lists appended to each quote sent back.
    quotes: dict[str, list[list[tuple[int, str]]]] = field(default_factory=dict)


class MockFixServerProtocol(Protocol):
    """One accepted connection. `self.factory` is set by Twisted before connectionMade()."""

    factory: MockFixServerFactory

    def __init__(self) -> None:
        self._buffer = ""
        self._seq_out = 0
        self._sender_comp_id = "SERVER"
        self._target_comp_id = "CLIENT"
        self._quote_send_count = 0

    def connectionMade(self) -> None:
        self.factory.connections.append(self)

    def connectionLost(self, reason: object = None) -> None:
        if self in self.factory.connections:
            self.factory.connections.remove(self)

    def dataReceived(self, data: bytes) -> None:
        self._buffer += data.decode("ascii")
        while True:
            marker = f"{SOH}10="
            idx = self._buffer.find(marker)
            if idx == -1:
                break
            end = self._buffer.find(SOH, idx + 1)
            if end == -1:
                break
            message, self._buffer = self._buffer[: end + 1], self._buffer[end + 1 :]
            self._handle(message)

    def _handle(self, raw: str) -> None:
        fields = parse_fields(raw)
        handlers = {
            "A": self._on_logon,
            "D": self._on_new_order_single,
            "x": self._on_security_list_request,
            "V": self._on_market_data_request,
            "F": self._on_order_cancel_request,
            "H": self._on_order_status_request,
        }
        handler = handlers.get(fields.get(35, ""))
        if handler is not None:
            handler(fields)

    def _send(self, msg_type: str, fields: list[tuple[int, str]]) -> None:
        self._seq_out += 1
        message = build_message(
            msg_type,
            fields,
            seq_num=self._seq_out,
            sender_comp_id=self._sender_comp_id,
            target_comp_id=self._target_comp_id,
        )
        self.transport.write(message.encode("ascii"))

    def _on_logon(self, fields: dict[int, str]) -> None:
        # The client's SenderCompID (49) is who we address responses to (56);
        # the client's TargetCompID (56) is who we claim to be (49).
        self._target_comp_id = fields.get(49, self._target_comp_id)
        self._sender_comp_id = fields.get(56, self._sender_comp_id)
        if self.factory.script.reject_logon:
            self.transport.loseConnection()
            return
        heartbeat = fields.get(108, "30")
        self._send("A", [(98, "0"), (108, heartbeat)])
        if self.factory.script.drop_after_logon:
            self.transport.loseConnection()

    def _on_new_order_single(self, fields: dict[int, str]) -> None:
        if self.factory.script.go_silent:
            return
        cl_ord_id = fields.get(11, "")
        symbol = fields.get(55, "")
        side = fields.get(54, "")
        qty = fields.get(38, "0")

        scripted = self.factory.script.execution_reports.get(cl_ord_id)
        if scripted is not None:
            for i, report_fields in enumerate(scripted):
                reactor.callLater(0.05 * i, self._send, "8", report_fields)
            return

        # Default: immediate fill, mirroring the documented happy-path shape.
        exec_id = f"exec-{cl_ord_id}"
        position_id = f"pos-{cl_ord_id}"
        self._send(
            "8",
            [
                (11, cl_ord_id),
                (17, exec_id),
                (150, "2"),  # ExecType Fill
                (39, "2"),  # OrdStatus Filled
                (55, symbol),
                (54, side),
                (32, qty),  # LastQty
                (31, "1.08500"),  # LastPx
                (14, qty),  # CumQty
                (151, "0"),  # LeavesQty
                (721, position_id),
            ],
        )

    def _on_security_list_request(self, fields: dict[int, str]) -> None:
        req_id = fields.get(320, "")
        symbols = self.factory.script.security_list
        if symbols is None:
            symbols = [(55, "1"), (48, "EURUSD")]
        self._send("y", [(320, req_id), *symbols])

    def _on_market_data_request(self, fields: dict[int, str]) -> None:
        symbol = fields.get(55, "")
        md_req_id = fields.get(262, "")
        for quote_fields in self.factory.script.quotes.get(symbol, []):
            reactor.callLater(
                0.05 * self._quote_send_count, self._send, "W", [(262, md_req_id), (55, symbol), *quote_fields]
            )
            self._quote_send_count += 1

    def _on_order_cancel_request(self, fields: dict[int, str]) -> None:
        self._send(
            "8",
            [
                (11, fields.get(11, "")),
                (41, fields.get(41, "")),
                (150, "4"),  # ExecType Cancelled
                (39, "4"),  # OrdStatus Cancelled
            ],
        )

    def _on_order_status_request(self, fields: dict[int, str]) -> None:
        self._send("8", [(11, fields.get(11, "")), (150, "I"), (39, "0")])


class MockFixServerFactory(Factory):
    """Builds one MockFixServerProtocol per accepted connection, sharing one script."""

    def __init__(self, script: MockFixServerScript) -> None:
        self.script = script
        self.connections: list[MockFixServerProtocol] = []

    def buildProtocol(self, addr: object) -> MockFixServerProtocol:
        protocol = MockFixServerProtocol()
        protocol.factory = self
        return protocol


class MockFixServer:
    """Owns the listening port. `start()`/`stop()` block the calling thread until done."""

    def __init__(self) -> None:
        self.script = MockFixServerScript()
        self._factory = MockFixServerFactory(self.script)
        self._port: object | None = None

    @crochet.wait_for(timeout=5.0)
    def start(self) -> int:
        """Start listening on an OS-assigned port on 127.0.0.1 and return it."""
        self._port = reactor.listenTCP(0, self._factory, interface="127.0.0.1")
        return self._port.getHost().port

    @crochet.wait_for(timeout=5.0)
    def stop(self) -> None:
        """Drop every open connection and stop listening."""
        for connection in list(self._factory.connections):
            connection.transport.loseConnection()
        if self._port is not None:
            self._port.stopListening()
            self._port = None
