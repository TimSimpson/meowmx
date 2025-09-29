import contextlib
import threading
import time
import typing as t

from .esp import esp
from .backoff import BackoffCalc
from . import common
from . import sqlalchemy


# class Base(DeclarativeBase):
#     pass


class ExpectedVersionFailure(RuntimeError):
    pass


DEFAULT_LIMIT = 512


class Client:
    def __init__(
        self,
        engine: common.Engine,
        session_maker: t.Optional[common.SessionMaker] = None,
    ) -> None:
        self._engine = engine
        if session_maker is not None:
            self._session_maker = session_maker
        else:
            self._session_maker = sqlalchemy.create_session_maker(engine)
        self._esp: common.Client
        if self._engine.dialect.name == "postgresql":
            self._esp = esp.Esp()
        else:
            self._esp = sqlalchemy.Client()
            if sqlalchemy.engine_is_in_memory_db(self._engine):
                self._esp = sqlalchemy.MutexLockedClient(self._esp)

    def setup_tables(self, aggregate_id_column_type: t.Optional[str] = None) -> None:
        self._esp.setup_tables(self._engine, aggregate_id_column_type)

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
            with session.begin():
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

    def _start_session_if_desired(
        self, session: t.Optional[common.Session]
    ) -> contextlib.AbstractContextManager[common.Session]:
        if session is not None:
            # return the already created session
            return contextlib.nullcontext(session)
        else:
            return self._session_maker()

    def load_all(
        self,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: t.Optional[int],
        session: t.Optional[common.Session] = None,
    ) -> t.List[common.RecordedEvent]:
        limit = limit or DEFAULT_LIMIT
        with self._start_session_if_desired(session) as session:
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
            from_version = 0

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
        self,
        aggregate_type: str,
        aggregate_id: str,
        events: t.List[common.NewEvent],
        version: t.Optional[int],
        session: t.Optional[common.Session] = None,
    ) -> t.List[common.RecordedEvent]:
        """Writes the events to database under the given aggregate.

        `version` should be the first version number of the new events (zero if
        starting a new stream).
        If `session` is passed in the user is responsible for starting and
        committing the transaction. If None is passed this code will do those
        things itself.
        """
        if len(events) == 0:
            return []
        with self._start_session_if_desired(session) as session_2:
            tx: contextlib.AbstractContextManager
            if session is None:
                # If we're controlling things, commit / rollback at the end of this.
                tx = session_2.begin()
            else:
                # If the user is controlling it, don't commit or rollback.
                tx = contextlib.nullcontext()
            with tx:
                if version is None:
                    version = self._esp.get_aggregate_version(
                        session_2, aggregate_type, aggregate_id
                    )
                    if version is None:
                        version = 0

                next_expected_version: int = version
                new_event_rows: t.List[common.NewEventRow] = [
                    common.NewEventRow(
                        aggregate_id=aggregate_id,
                        event_type=element.event_type,
                        json=element.json,
                        version=next_expected_version + index,
                    )
                    for index, element in enumerate(events)
                ]
                expected_version = version - 1

                self._esp.create_aggregate_if_absent(
                    session_2, aggregate_type, aggregate_id
                )
                if not self._esp.check_and_update_aggregate_version(
                    session_2, aggregate_id, expected_version, version
                ):
                    raise ExpectedVersionFailure(
                        f"{aggregate_type} - {aggregate_id} did not match expected_version of {expected_version}"
                    )
                results = []
                for new_event_row in new_event_rows:
                    recorded_event = self._esp.append_event(
                        session_2, new_event_row, aggregate_type
                    )
                    results.append(recorded_event)
                return results

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
