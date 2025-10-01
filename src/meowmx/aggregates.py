import typing as t
from . import common


class EventCompatible(t.Protocol):
    @property
    def event_type(self) -> str: ...

    def to_event_data(self) -> str: ...


class PendingEvents:
    """Represents a set of events to be written to the database."""

    def __init__(
        self,
        events: t.List[common.NewEvent],
        version: int,
        on_confirm_write: t.Callable,
        on_failure: t.Callable,
    ) -> None:
        self._events = events
        self._version = version
        self._on_success = on_confirm_write
        self._on_failure = on_failure

    def on_success(self) -> None:
        """Whatever processes pending events should call this on success."""
        self._on_success()

    @property
    def events(self) -> t.List[common.NewEvent]:
        """The list of events to write."""
        return self._events

    def on_failure(self) -> None:
        """Whatever processes pending events should call this on failure."""
        self._on_failure()

    @property
    def version(self) -> int:
        """The version of the first event written to the database."""
        return self._version


class Aggregate(t.Protocol):
    """Defines the shape of any aggregate type that can be loaded or saved."""

    @property
    def aggregate_id(self) -> str:
        """Used for the aggreate ID row."""
        ...

    aggregate_type: str  # String for the aggregate_type row

    def collect_pending_events(self) -> PendingEvents:
        """Grabs evens emitted by the aggregate but not yet saved."""
        ...


class EventBuffer:
    """Applies events and keeps them in a buffer of pending events.

    Aggregates can hold onto an instance of this to store their events locally
    before they get saved in the form of a PendingEvents instance.
    """

    def __init__(
        self,
        loader: t.Callable[[common.RecordedEvent], EventCompatible],
        applier: t.Callable[[EventCompatible], None],
    ) -> None:
        self._loader: t.Callable[[common.RecordedEvent], EventCompatible] = loader
        self._pending_events: t.List[EventCompatible] = []
        self._next_version = 0
        self._applier = applier
        self._busy = False

    def collect_pending_events(self) -> t.Optional[PendingEvents]:
        """Returns pending events to code expected to persist them.

        The caller is expected this object when it succeeds or fails so this
        object will know what the next expected version will be.
        """
        if len(self._pending_events) == 0:
            return None

        self._busy = True

        pending_events: t.List[common.NewEvent] = []
        for event in self._pending_events:
            pending_events.append(
                common.NewEvent(event_type=event.event_type, json=event.to_event_data())
            )

        pending_count = len(pending_events)

        def on_confirm_write() -> None:
            self._next_version += pending_count
            self._pending_events = []
            self._busy = False

        def on_failure() -> None:
            self._busy = False

        return PendingEvents(
            events=pending_events,
            version=self._next_version,
            on_confirm_write=on_confirm_write,
            on_failure=on_failure,
        )

    @property
    def current_version(self) -> int:
        return self.db_version + len(self._pending_events)

    @property
    def db_version(self) -> int:
        return self._next_version - 1

    def emit(self, event: EventCompatible) -> None:
        """Apply an event, and add it to the pending events list."""
        if self._busy:
            # this `_busy` check is only to catch innocent mistakes; this
            # code isn't intended to be thread-safe.
            raise RuntimeError("apply happened while events were being collected")
        self._applier(event)
        self._pending_events.append(event)

    def load_transformed_events(self, events: t.List[EventCompatible]) -> None:
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
