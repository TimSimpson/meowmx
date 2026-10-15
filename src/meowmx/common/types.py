from dataclasses import dataclass
import typing as t

from sqlalchemy.orm import Session


class All:
    """Requests that all aggregate types be considered in a `sub` query."""

    pass


class EventCompatible(t.Protocol):
    """Used by any type which can be converted into event data."""

    @property
    def event_type(self) -> str: ...

    def to_event_data(self) -> str: ...


@dataclass
class NewEvent:
    event_type: str
    json: str

    def to_event_data(self) -> str:
        return self.json


@dataclass
class NewEventRow(NewEvent):
    aggregate_id: str
    version: int

    def to_event_data(self) -> str:
        return self.json


@dataclass
class RecordedEvent(NewEventRow):
    aggregate_type: str
    id: int
    tx_id: int

    def to_event_data(self) -> str:
        return self.json


class LoadResult(t.NamedTuple):
    events: t.List[RecordedEvent]
    has_more: bool


# this is `str` because the sql libraries think Python's int could mean
# "smallint", which can't be cast to an XID8 which is used for the transaction
# ID. The XID8 results are cast to text in the SQL queries, which becomes a
# Python `str` so no precision is lost.
TxId: t.TypeAlias = str


@dataclass
class AggregateInfo:
    id: str
    version: int
    aggregate_type: str


class ListAggregateInfoResult(t.NamedTuple):
    aggregates: t.List[AggregateInfo]
    next_id: t.Optional[str]


@dataclass
class SubCheckpoint:
    last_tx_id: TxId
    last_event_id: int


SessionMaker = t.Callable[[], Session]

EventHandler = t.Callable[[Session, RecordedEvent], None]
