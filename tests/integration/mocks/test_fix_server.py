import queue

import crochet
from ctrader_fix import Client, LogonRequest, NewOrderSingle

from tests.mocks.fix_server import MockFixServer

_CONFIG = {
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
def _start_client(client: Client) -> None:
    client.startService()


@crochet.wait_for(timeout=5.0)
def _stop_client(client: Client) -> None:
    client.stopService()


@crochet.wait_for(timeout=5.0)
def _send(client: Client, message: object):
    return client.send(message)


def test_mock_server_accepts_logon_and_fills_a_market_order() -> None:
    server = MockFixServer()
    port = server.start()
    connected: queue.Queue[bool] = queue.Queue()
    responses: queue.Queue[object] = queue.Queue()

    client = Client("127.0.0.1", port, ssl=False)
    client.setConnectedCallback(lambda c: connected.put(True))
    client.setMessageReceivedCallback(lambda c, message: responses.put(message))

    try:
        _start_client(client)
        assert connected.get(timeout=5) is True

        _send(client, LogonRequest(_CONFIG))
        logon_ack = responses.get(timeout=5)
        assert logon_ack.getFieldValue(35) == "A"

        order = NewOrderSingle(_CONFIG)
        order.ClOrdID = "abc123"
        order.Symbol = 1
        order.Side = "1"
        order.OrderQty = 1000
        order.OrdType = "1"
        _send(client, order)

        fill = responses.get(timeout=5)
        assert fill.getFieldValue(35) == "8"
        assert fill.getFieldValue(11) == "abc123"
        assert fill.getFieldValue(39) == "2"  # OrdStatus Filled
        assert fill.getFieldValue(721) == "pos-abc123"
    finally:
        _stop_client(client)
        server.stop()


def test_mock_server_rejects_logon_when_scripted() -> None:
    server = MockFixServer()
    server.script.reject_logon = True
    port = server.start()
    disconnected: queue.Queue[bool] = queue.Queue()

    client = Client("127.0.0.1", port, ssl=False)
    client.setDisconnectedCallback(lambda c, reason: disconnected.put(True))

    try:
        _start_client(client)
        _send(client, LogonRequest(_CONFIG))
        assert disconnected.get(timeout=5) is True
    finally:
        _stop_client(client)
        server.stop()


def test_mock_server_replays_a_scripted_execution_report_sequence() -> None:
    server = MockFixServer()
    server.script.execution_reports["order-1"] = [
        [(11, "order-1"), (17, "exec-1"), (150, "0"), (39, "0")],  # New
        [(11, "order-1"), (17, "exec-2"), (150, "2"), (39, "2"), (721, "pos-1")],  # Filled
    ]
    port = server.start()
    connected: queue.Queue[bool] = queue.Queue()
    responses: queue.Queue[object] = queue.Queue()

    client = Client("127.0.0.1", port, ssl=False)
    client.setConnectedCallback(lambda c: connected.put(True))
    client.setMessageReceivedCallback(lambda c, message: responses.put(message))

    try:
        _start_client(client)
        connected.get(timeout=5)
        _send(client, LogonRequest(_CONFIG))
        responses.get(timeout=5)  # logon ack

        order = NewOrderSingle(_CONFIG)
        order.ClOrdID = "order-1"
        order.Symbol = 1
        order.Side = "1"
        order.OrderQty = 1000
        order.OrdType = "1"
        _send(client, order)

        new_report = responses.get(timeout=5)
        filled_report = responses.get(timeout=5)
        assert new_report.getFieldValue(39) == "0"
        assert filled_report.getFieldValue(39) == "2"
        assert filled_report.getFieldValue(721) == "pos-1"
    finally:
        _stop_client(client)
        server.stop()
