"""Protocol for the FIX session a command handler dispatches through."""

from typing import Protocol


class FixSession(Protocol):
    """The subset of SessionManager that OrderCommandHandler depends on."""

    def is_logged_on(self) -> bool:
        """Report whether the underlying FIX session has completed logon."""
        ...

    def send(self, message: object) -> None:
        """Send a FIX message on this session."""
        ...
