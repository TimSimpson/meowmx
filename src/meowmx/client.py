import time
import typing as t
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

from .esp import esp
from .backoff import BackoffCalc


class Base(DeclarativeBase):
    pass


class ExpectedVersionFailure(RuntimeError):
    pass


DEFAULT_LIMIT = 512

EventHandler = esp.EventHandler


class Client:
    def __init__(self, url: str) -> None:
        self._engine = create_engine(
            "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable",
        )
        self._sessionmaker = sessionmaker(
            autocommit=False, autoflush=False, bind=self._engine
        )
        self._esp = esp.Esp(self._engine, self._sessionmaker)

    def setup_tables(self) -> None:
        self._esp.setup_tables()

    def load_all(
        self,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: t.Optional[int],
    ) -> t.List[esp.RecordedEvent]:
        limit = limit or DEFAULT_LIMIT
        with self._sessionmaker() as session:
            return self._esp.read_all_events(
                session,
                limit=limit,
                from_tx_id=from_tx_id,
                to_tx_id=to_tx_id,
            )

    def load(
        self,
        aggregate_type: str,
        aggregate_id: str,
        from_version: t.Optional[int] = None,
        to_version: t.Optional[int] = None,
        limit: t.Optional[int] = None,
        reverse: bool = False,
    ) -> t.List[esp.RecordedEvent]:
        limit = limit or DEFAULT_LIMIT
        if from_version is None and not reverse:
            from_version = 0

        with self._sessionmaker() as session:
            return self._esp.read_events_by_aggregate_id(
                session,
                aggregate_id=aggregate_id,
                limit=limit,
                from_version=from_version,
                to_version=to_version,
                reverse=reverse,
            )

    def save_events(
        self, aggregate_type: str, aggregate_id: str, events: t.List[esp.NewEvent]
    ) -> t.List[esp.RecordedEvent]:
        if len(events) == 0:
            return []
        new_version = events[0].version
        expected_version = new_version - 1
        for index in range(1, len(events)):
            new_version += 1
            if events[index].version != new_version:
                raise ValueError(
                    "event versions do not increase; no gaps should be found in the version of each new event"
                )

        with self._sessionmaker() as session:
            try:
                self._esp.create_aggregate_if_absent(
                    session, aggregate_type, aggregate_id
                )
                if not self._esp.check_and_update_aggregate_version(
                    session, aggregate_id, expected_version, new_version
                ):
                    raise ExpectedVersionFailure(
                        f"database did not match expected_version of {expected_version}"
                    )
                results = []
                for event in events:
                    recorded_event = self._esp.append_event(
                        session, event, aggregate_type
                    )
                    results.append(recorded_event)
                session.commit()
                return results
            except:
                session.rollback()
                raise

    def sub(
        self,
        subscription_name: str,
        aggregate_type: str,
        handler: EventHandler,
        batch_size: int = 10,
        timelimit: int = 1,
    ) -> None:
        backoff = BackoffCalc(1, timelimit)
        while True:
            processed = self._esp.handle_subscription_events(
                subscription_name=subscription_name,
                aggregate_type=aggregate_type,
                batch_size=batch_size,
                handler=handler,
            )
            if processed == 0:
                time.sleep(backoff.failure())
            else:
                backoff.success()
