import typing as t
from .. import common


class PendingEvents:
    """Represents a set of events to be written to the database."""

    def __init__(
        self,
        events: t.List[common.NewEvent],
        version: int,
        # on_confirm_write: t.Callable,
        # on_failure: t.Callable,
    ) -> None:
        self._events = events
        self._version = version
        # self._on_success = on_confirm_write
        # self._on_failure = on_failure

    # def on_success(self) -> None:
    #     """Whatever processes pending events should call this on success."""
    #     self._on_success()

    @property
    def events(self) -> t.List[common.NewEvent]:
        """The list of events to write."""
        return self._events

    # def on_failure(self) -> None:
    #     """Whatever processes pending events should call this on failure."""
    #     self._on_failure()

    @property
    def version(self) -> int:
        """The version of the first event written to the database."""
        return self._version
