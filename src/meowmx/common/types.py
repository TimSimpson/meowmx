from dataclasses import dataclass
import typing as t

from sqlalchemy.orm import Session


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


@dataclass
class SubCheckpoint:
    last_tx_id: int
    last_event_id: int


SessionMaker = t.Callable[[], Session]

EventHandler = t.Callable[[Session, RecordedEvent], None]
