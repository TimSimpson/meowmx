from dataclasses import dataclass
import typing as t


@dataclass
class NewEvent:
    aggregate_id: str
    event_type: str
    json: t.Dict[str, t.Any]
    version: int


@dataclass
class RecordedEvent:
    aggregate_type: str
    aggregate_id: str
    id: int
    event_type: str
    json: t.Dict[str, t.Any]
    tx_id: int
    version: int


@dataclass
class SubCheckpoint:
    last_tx_id: int
    last_event_id: int


Engine: t.TypeAlias = t.Any
SessionMaker: t.TypeAlias = t.Any
Session: t.TypeAlias = t.Any

EventHandler: t.TypeAlias = t.Callable[[Session, RecordedEvent], None]
