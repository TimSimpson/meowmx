from dataclasses import dataclass
import typing as t

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


@dataclass
class NewEvent:
    event_type: str
    json: t.Dict[str, t.Any]


@dataclass
class NewEventRow:
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


SessionMaker = t.Callable[[], Session]

EventHandler = t.Callable[[Session, RecordedEvent], None]


class Client(t.Protocol):
    def setup_tables(self, engine: Engine) -> None: ...

    def append_event(
        self,
        session: Session,
        event: NewEventRow,
        assumed_aggregate_type: str,
    ) -> RecordedEvent: ...

    def create_aggregate_if_absent(
        self,
        session: Session,
        aggregate_type: str,
        aggregate_id: str,  # UUID string – SQLAlchemy will coerce to UUID if the column type is UUID
    ) -> None: ...

    def create_subscription_if_absent(
        self, session: Session, subscription_name: str
    ) -> None: ...

    def check_and_update_aggregate_version(
        self,
        session: Session,
        aggregate_id: str,
        expected_version: int,
        new_version: int,
    ) -> bool: ...

    def read_checkpoint_and_lock_subscription(
        self, session: Session, subscription_name: str
    ) -> t.Optional[SubCheckpoint]: ...

    def read_all_events(
        self,
        session: Session,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: int,
        reverse: bool = False,
    ) -> t.List[RecordedEvent]: ...

    def read_events_by_aggregate_id(
        self,
        session: Session,
        aggregate_id: str,
        limit: int,
        from_version: t.Optional[int],
        to_version: t.Optional[int],
        reverse: bool = False,
    ) -> t.List[RecordedEvent]: ...

    def read_events_after_checkpoint(
        self,
        session: Session,
        aggregate_type: str,
        last_processed_tx_id: int,
        last_processed_event_id: int,
    ) -> t.List[RecordedEvent]: ...

    def update_event_subscription(
        self,
        session: Session,
        subscription_name: str,
        last_tx_id: int,
        last_event_id: int,
    ) -> bool: ...
