import threading
import time
import typing as t

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

from .esp import esp
from .backoff import BackoffCalc
from . import common
from .sqlalchemy.client import Client as SqlAlchemyClient


class Base(DeclarativeBase):
    pass


class ExpectedVersionFailure(RuntimeError):
    pass


DEFAULT_LIMIT = 512


class Client:
    def __init__(
        self,
        engine: common.Engine,
        session_maker: t.Optional[common.SessionMaker] = None,
    ) -> None:
        # self._engine = create_engine(
        #     "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable",
        # )
        self._engine = engine
        if session_maker is not None:
            self._session_maker = session_maker
        else:
            self._session_maker = sessionmaker(
                autocommit=False, autoflush=False, bind=self._engine
            )
        self._esp: common.Client
        if self._engine.dialect.name == "postgresql":
            self._esp = esp.Esp()
        else:
            self._esp = SqlAlchemyClient()

    def setup_tables(self) -> None:
        self._esp.setup_tables(self._engine)

    def _handle_subscription_events(
        self,
        subscription_name: str,
        aggregate_type: str,
        batch_size: int,
        handler: common.EventHandler,
    ) -> int:
        """Handles the next event in the subscription.

        Returns the number of events handled, or zero if there was no event.
        If there is an event, calls the handler. On success updates the
        checkpoint.
        If the handler raises an exception, then releases the lock on the event.
        """
        with self._session_maker() as session:
            self._esp.create_subscription_if_absent(session, subscription_name)
            checkpoint = self._esp.read_checkpoint_and_lock_subscription(
                session, subscription_name
            )
            if not checkpoint:
                # this can happen if we can't lock a record
                session.commit()
                return 0
            else:
                events = self._esp.read_events_after_checkpoint(
                    session,
                    aggregate_type,
                    checkpoint.last_tx_id,
                    checkpoint.last_event_id,
                )

                updated_checkpoint = False

                processed_count = 0
                for event in events:
                    if processed_count >= batch_size:
                        break
                    processed_count += 1

                    with session.begin_nested() as nested_tx:
                        try:
                            handler(session, event)
                        except Exception:
                            nested_tx.rollback()
                            # if we need to update the check point at all,
                            # commit what we got, especially if this is being
                            # a problematic event
                            if updated_checkpoint:
                                session.commit()
                            raise
                        # session2.commit()
                        self._esp.update_event_subscription(
                            session,
                            subscription_name,
                            event.tx_id,
                            event.id,
                        )
                        updated_checkpoint = True

                session.commit()
                return processed_count

    def load_all(
        self,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: t.Optional[int],
    ) -> t.List[common.RecordedEvent]:
        limit = limit or DEFAULT_LIMIT
        with self._session_maker() as session:
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
    ) -> t.List[common.RecordedEvent]:
        limit = limit or DEFAULT_LIMIT
        if from_version is None and not reverse:
            from_version = -1

        with self._session_maker() as session:
            return self._esp.read_events_by_aggregate_id(
                session,
                aggregate_id=aggregate_id,
                limit=limit,
                from_version=from_version,
                to_version=to_version,
                reverse=reverse,
            )

    def save_events(
        self, aggregate_type: str, aggregate_id: str, events: t.List[common.NewEvent]
    ) -> t.List[common.RecordedEvent]:
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

        with self._session_maker() as session:
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
        handler: common.EventHandler,
        batch_size: int = 10,
        max_sleep_time: int = 1,
        stop_signal: t.Optional[threading.Event] = None,
    ) -> None:
        backoff = BackoffCalc(1, max_sleep_time)
        while stop_signal is None or not stop_signal.is_set():
            processed = self._handle_subscription_events(
                subscription_name=subscription_name,
                aggregate_type=aggregate_type,
                batch_size=batch_size,
                handler=handler,
            )
            if processed == 0:
                time.sleep(backoff.failure())
            else:
                backoff.success()
