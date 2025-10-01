import typing as t

from . import pending
from .. import common


T = t.TypeVar("T", bound=common.EventCompatible)


class EventBuffer(t.Generic[T]):
    """Applies events and keeps them in a buffer of pending events.

    Aggregates can hold onto an instance of this to store their events locally
    before they get saved in the form of a PendingEvents instance.
    """

    def __init__(
        self,
        loader: t.Callable[[common.RecordedEvent], T],
        applier: t.Callable[[T], None],
    ) -> None:
        self._loader: t.Callable[[common.RecordedEvent], T] = loader
        self._pending_events: t.List[T] = []
        self._next_version = 0
        self._applier = applier
        self._busy = False

    def collect_pending_events(self) -> pending.PendingEvents:
        """Returns pending events to code expected to persist them.

        The caller is expected this object when it succeeds or fails so this
        object will know what the next expected version will be.
        """
        pending_events: t.List[common.NewEvent] = []
        for event in self._pending_events:
            pending_events.append(
                common.NewEvent(event_type=event.event_type, json=event.to_event_data())
            )

        pending_count = len(pending_events)

        result = pending.PendingEvents(
            events=pending_events,
            version=self._next_version,
        )

        self._next_version += pending_count
        self._pending_events = []

        return result

    @property
    def current_version(self) -> int:
        return self.db_version + len(self._pending_events)

    @property
    def db_version(self) -> int:
        return self._next_version - 1

    def emit(self, event: T) -> None:
        """Apply an event, and add it to the pending events list."""
        if self._busy:
            # this `_busy` check is only to catch innocent mistakes; this
            # code isn't intended to be thread-safe.
            raise RuntimeError("apply happened while events were being collected")
        self._applier(event)
        self._pending_events.append(event)

    def load_transformed_events(self, events: t.List[T]) -> None:
        """Load events in whatever form this wants."""
        if self._next_version > 0:
            raise RuntimeError("Cannot load events more than once.")

        for event in events:
            self._applier(event)
            self._next_version += 1

    def load_recorded_events(self, events: t.List[common.RecordedEvent]) -> None:
        """Use this to load up events that are already in the DB."""
        for event in events:
            if event.version != self._next_version:
                raise RuntimeError(
                    f"unexpected version of event was loaded. Expected={self._next_version}, received: {event.version}"
                )

            self._applier(self._loader(event))
            self._next_version += 1
