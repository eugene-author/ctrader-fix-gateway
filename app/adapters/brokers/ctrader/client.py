"""Builds the ctrader_fix.Client used by both FIX sessions."""

from collections.abc import Mapping

from ctrader_fix import Client, FixProtocol, ResponseMessage

from app.adapters.brokers.ctrader.reconnect import DEFAULT_MAX_DELAY_SECONDS, next_delay


class _FramedFixProtocol(FixProtocol):
    """Splits `dataReceived`'s buffer on FIX message boundaries."""

    _currentMessage = ""

    def dataReceived(self, data: bytes) -> None:
        """Split the accumulated buffer into complete FIX messages and deliver each one."""
        self._currentMessage += data.decode("ascii")
        delimiter = self.factory.delimiter
        terminator = f"{delimiter}10="
        while True:
            start = self._currentMessage.find(terminator)
            if start == -1:
                return
            end = self._currentMessage.find(delimiter, start + len(terminator))
            if end == -1:
                return
            end += len(delimiter)
            message, self._currentMessage = self._currentMessage[:end], self._currentMessage[end:]
            self.factory.received(ResponseMessage(message, delimiter))


def create_client(
    cfg: Mapping[str, str | int | bool], *, reconnect_max_delay: float = DEFAULT_MAX_DELAY_SECONDS
) -> Client:
    """Build a ctrader_fix.Client with automatic reconnection and retry delays.

    `Client` is a Twisted `ClientService`: once started, it reconnects on its
    own using this `retryPolicy`
    """
    host = str(cfg["Host"])
    port = int(cfg["Port"])
    ssl = bool(cfg.get("SSL", False))

    def _retry_policy(failed_attempts: int) -> float:
        return next_delay(failed_attempts, cap=reconnect_max_delay)

    client = Client(host, port, ssl=ssl, retryPolicy=_retry_policy)
    client._factory.protocol = _FramedFixProtocol
    return client
