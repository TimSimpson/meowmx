import typing as t
from sqlalchemy import Engine
from .types import (
    NewEventRow,
    RecordedEvent,
    Session,
    SubCheckpoint,
)


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

    def get_aggregate_version(
        self, session: Session, aggregate_type: str, aggregate_id: str
    ) -> t.Optional[int]: ...

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
