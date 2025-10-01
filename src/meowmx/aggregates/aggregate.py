import typing as t
from .. import common
from . import pending


class LoadableAggregate(t.Protocol):
    """Defines an aggregate type that can be loaded."""

    aggregate_type: str  # String for the aggregate_type row

    def __init__(self, recorded_events: t.List[common.RecordedEvent]) -> None: ...


class SavableAggregate(t.Protocol):
    """Defines the shape of any aggregate type that can be saved."""

    @property
    def aggregate_id(self) -> str:
        """Used for the aggreate ID row."""
        ...

    @property
    def aggregate_type(self) -> str:
        """String for the aggregate_type row."""
        ...

    def collect_pending_events(self) -> pending.PendingEvents:
        """Grabs evens emitted by the aggregate but not yet saved."""
        ...
