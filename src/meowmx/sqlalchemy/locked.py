import threading
import typing as t

from .. import common


class MutexLockedClient:
    """
    This exists just so use sqlite in memory databases. It locks a mutext on
    every single method. As you can guess that makes it pretty slow.
    """

    def __init__(
        self,
        client: common.Client,
    ) -> None:
        self._client = client
        self._mutex = threading.Lock()

    def setup_tables(
        self, engine: common.Engine, aggregate_id_column_type: t.Optional[str]
    ) -> None:
        with self._mutex:
            self._client.setup_tables(engine, aggregate_id_column_type)

    def append_event(
        self,
        session: common.Session,
        event: common.NewEventRow,
        assumed_aggregate_type: str,
    ) -> common.RecordedEvent:
        with self._mutex:
            return self._client.append_event(session, event, assumed_aggregate_type)

    def create_aggregate_if_absent(
        self,
        session: common.Session,
        aggregate_type: str,
        aggregate_id: str,  # UUID string – SQLAlchemy will coerce to UUID if the column type is UUID
    ) -> None:
        with self._mutex:
            self._client.create_aggregate_if_absent(
                session, aggregate_type, aggregate_id
            )

    def create_subscription_if_absent(
        self, session: common.Session, subscription_name: str
    ) -> None:
        with self._mutex:
            self._client.create_subscription_if_absent(session, subscription_name)

    def check_and_update_aggregate_version(
        self,
        session: common.Session,
        aggregate_id: str,
        expected_version: int,
        new_version: int,
    ) -> bool:
        with self._mutex:
            return self._client.check_and_update_aggregate_version(
                session, aggregate_id, expected_version, new_version
            )

    def get_aggregate_version(
        self, session: common.Session, aggregate_type: str, aggregate_id: str
    ) -> t.Optional[int]:
        with self._mutex:
            return self._client.get_aggregate_version(
                session, aggregate_type, aggregate_id
            )

    def read_checkpoint_and_lock_subscription(
        self, session: t.Any, subscription_name: str
    ) -> t.Optional[common.SubCheckpoint]:
        with self._mutex:
            return self._client.read_checkpoint_and_lock_subscription(
                session, subscription_name
            )

    def read_all_events(
        self,
        session: common.Session,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: int,
        reverse: bool = False,
    ) -> t.List[common.RecordedEvent]:
        with self._mutex:
            return self._client.read_all_events(
                session, from_tx_id, to_tx_id, limit, reverse
            )

    def read_events_by_aggregate_id(
        self,
        session: common.Session,
        aggregate_id: str,
        limit: int,
        from_version: t.Optional[int],
        to_version: t.Optional[int],
        reverse: bool = False,
    ) -> t.List[common.RecordedEvent]:
        with self._mutex:
            return self._client.read_events_by_aggregate_id(
                session, aggregate_id, limit, from_version, to_version, reverse
            )

    def read_events_after_checkpoint(
        self,
        session: t.Any,
        aggregate_type: str,
        last_processed_tx_id: int,
        last_processed_event_id: int,
    ) -> t.List[common.RecordedEvent]:
        with self._mutex:
            return self._client.read_events_after_checkpoint(
                session, aggregate_type, last_processed_tx_id, last_processed_event_id
            )

    def update_event_subscription(
        self,
        session: common.Session,
        subscription_name: str,
        last_tx_id: int,
        last_event_id: int,
    ) -> bool:
        with self._mutex:
            return self._client.update_event_subscription(
                session, subscription_name, last_tx_id, last_event_id
            )
