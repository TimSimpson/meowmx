from dataclasses import dataclass
import json
import textwrap
import typing as t
from sqlalchemy import Engine, text, bindparam, Integer, Text, String
from . import migrations


class PostgresExecuter(t.Protocol):
    def exec(self, text: str, arguments: t.Mapping[str, t.Any]) -> None: ...


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


Session: t.TypeAlias = t.Any

EventHandler: t.TypeAlias = t.Callable[[Session, RecordedEvent], None]


class Esp:
    def __init__(self, engine: Engine, sessionmaker: t.Any) -> None:
        self._engine = engine
        self._sessionmaker = sessionmaker

    def setup_tables(self) -> None:
        with self._engine.connect() as conn:
            conn.execute(text(migrations.MIGRATIONS))
            conn.commit()

    def append_event(
        self, session: Session, event: NewEvent, assumed_aggregate_type: str
    ) -> RecordedEvent:
        """Inserts an event.

        The aggregate type is assumed to be known by the caller.
        """
        query = textwrap.dedent("""
        INSERT INTO ES_EVENT (TRANSACTION_ID, AGGREGATE_ID, VERSION, EVENT_TYPE, JSON_DATA)
            VALUES(pg_current_xact_id(), :aggregate_id, :version, :event_type, CAST(:json_obj AS JSON))
            RETURNING ID, TRANSACTION_ID::text, EVENT_TYPE, JSON_DATA
        """)
        row = session.execute(
            text(query),
            {
                "aggregate_id": event.aggregate_id,
                "version": event.version,
                "event_type": event.event_type,
                "json_obj": json.dumps(event.json),
            },
        ).fetchone()
        return RecordedEvent(
            aggregate_id=event.aggregate_id,
            aggregate_type=assumed_aggregate_type,
            event_type=event.event_type,
            id=row[0],
            json=event.json,
            tx_id=row[1],
            version=event.version,
        )

    def create_aggregate_if_absent(
        self, session: Session, aggregate_type: str, aggregate_id: str
    ) -> None:
        """Inserts the aggregate type into the table"""
        query = textwrap.dedent("""
            INSERT INTO ES_AGGREGATE (ID, VERSION, AGGREGATE_TYPE)
                VALUES (:aggregate_id, -1, :aggregate_type)
                ON CONFLICT DO NOTHING
            """)
        session.execute(
            text(query),
            {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
        )

    def create_subscription_if_absent(
        self, session: Session, subscription_name: str
    ) -> None:
        query = textwrap.dedent(
            """
                INSERT INTO ES_EVENT_SUBSCRIPTION (
                    SUBSCRIPTION_NAME,
                    LAST_TRANSACTION_ID,
                    LAST_EVENT_ID
                )
                VALUES (
                    :subscription_name,
                    '0'::xid8,
                    0
                )
                ON CONFLICT DO NOTHING
                """
        )
        session.execute(
            text(query),
            {"subscription_name": subscription_name},
        )

    def check_and_update_aggregate_version(
        self,
        session: Session,
        aggregate_id: str,
        expected_version: int,
        new_version: int,
    ) -> bool:
        query = textwrap.dedent(
            """
                UPDATE ES_AGGREGATE
                SET VERSION = :new_version
                WHERE ID = :aggregate_id
                AND VERSION = :expected_version
                """
        )
        result = session.execute(
            text(query),
            {
                "new_version": new_version,
                "aggregate_id": aggregate_id,
                "expected_version": expected_version,
            },
        )
        return result.rowcount == 1

    def handle_subscription_events(
        self,
        subscription_name: str,
        aggregate_type: str,
        batch_size: int,
        handler: EventHandler,
    ) -> int:
        """Handles the next event in the subscription.

        Returns the number of events handled, or zero if there was no event.
        If there is an event, calls the handler. On success updates the
        checkpoint.
        If the handler raises an exception, then releases the lock on the event.
        """
        with self._sessionmaker() as session:
            self.create_subscription_if_absent(session, subscription_name)
            checkpoint = self.read_checkpoint_and_lock_subscription(
                session, subscription_name
            )
            if not checkpoint:
                # this can happen if we can't lock a record
                session.commit()
                return 0
            else:
                events = self.read_events_after_checkpoint(
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

                    with session.begin_nested() as session2:
                        try:
                            handler(session2, event)
                        except Exception:
                            session2.rollback()
                            # if we need to update the check point at all,
                            # commit what we got, especially if this is being
                            # a problematic event
                            if updated_checkpoint:
                                session.commit()
                            raise
                        # session2.commit()
                        self.update_event_subscription(
                            session,
                            subscription_name,
                            event.tx_id,
                            event.id,
                        )
                        updated_checkpoint = True

                session.commit()
                return processed_count

    def read_checkpoint_and_lock_subscription(
        self, session: t.Any, subscription_name: str
    ) -> t.Optional[SubCheckpoint]:
        query = textwrap.dedent(
            """
            SELECT
                LAST_TRANSACTION_ID::text AS last_transaction_id,
                LAST_EVENT_ID AS last_event_id
            FROM ES_EVENT_SUBSCRIPTION
            WHERE SUBSCRIPTION_NAME = :subscription_name
            FOR UPDATE SKIP LOCKED
            """
        )

        result = session.execute(
            text(query),
            {"subscription_name": subscription_name},
        )
        row = result.fetchone()  # None if the row is locked or absent
        if row is None:
            return None

        return SubCheckpoint(
            last_tx_id=row[0],
            last_event_id=row[1],
        )

    def read_all_events(
        self,
        session: Session,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: int,
        reverse: bool = False,
    ) -> t.List[RecordedEvent]:
        """Reads all the events from the table via the transaction ID."""
        if to_tx_id is None and limit is None:
            raise ValueError(
                "Neither to_tx_id or limit are set. Too many rows would be returned."
            )
        order = "DESC" if reverse else "ASC"
        query = textwrap.dedent(f"""
                SELECT
                    a.aggregate_type,
                    e.id,
                    e.transaction_id::text AS tx_id,
                    e.aggregate_id,                    
                    e.event_type,
                    e.json_data,
                    e.version
                FROM es_event e
                JOIN es_aggregate a ON a.ID = e.aggregate_id
                WHERE (:from_tx_id IS NULL OR e.transaction_id > CAST(:from_tx_id AS xid8))
                AND (:to_tx_id IS NULL OR e.transaction_id <= CAST(:to_tx_id AS xid8))
                ORDER BY transaction_id {order}
                LIMIT :limit
                """)

        stmt = text(query).bindparams(
            bindparam("from_tx_id", type_=String),
            bindparam("to_tx_id", type_=String),
            bindparam("limit", type_=Integer),
        )

        args = {
            "from_tx_id": from_tx_id,
            "to_tx_id": to_tx_id,
            "limit": limit,
        }
        result = session.execute(
            stmt,
            args,
        )
        rows = result.fetchall()
        events: t.List[RecordedEvent] = []
        for row in rows:
            # Row order matches the SELECT list above:
            #   0 → id, 1 → tx_id (as string), 2 → event_type,
            #   3 → json data, 4 → version
            events.append(
                RecordedEvent(
                    aggregate_type=row[0],
                    aggregate_id=row[3],
                    id=row[1],
                    tx_id=int(row[2]),  # cast back to int if needed
                    event_type=row[4],
                    json=row[5],
                    version=row[6],
                )
            )

        return events

    def read_events_by_aggregate_id(
        self,
        session: Session,
        aggregate_id: str,
        limit: int,
        from_version: t.Optional[int],
        to_version: t.Optional[int],
        reverse: bool = False,
    ) -> t.List[RecordedEvent]:
        order = "DESC" if reverse else "ASC"
        query = textwrap.dedent(
            f"""
                SELECT
                    a.aggregate_type,
                    e.id,
                    e.transaction_id::text AS tx_id,
                    e.event_type,
                    e.json_data,
                    e.version
                FROM es_event e
                JOIN es_aggregate a ON a.ID = e.aggregate_id
                WHERE aggregate_id = :aggregate_id
                AND (:from_version IS NULL OR e.version > :from_version)
                AND (:to_version IS NULL OR e.version <= :to_version)
                ORDER BY e.version {order}
                LIMIT :limit
                """
        )
        stmt = text(query).bindparams(
            bindparam("aggregate_id"),
            bindparam("from_version", type_=Integer),
            bindparam("to_version", type_=Integer),
            bindparam("limit", type_=Integer),
        )

        result = session.execute(
            stmt,
            {
                "aggregate_id": aggregate_id,
                "from_version": from_version,
                "to_version": to_version,
                "limit": limit,
            },
        )
        rows = result.fetchall()
        events: t.List[RecordedEvent] = []
        for row in rows:
            # Row order matches the SELECT list above:
            #   0 → id, 1 → tx_id (as string), 2 → event_type,
            #   3 → json data, 4 → version
            events.append(
                RecordedEvent(
                    aggregate_type=row[0],
                    aggregate_id=aggregate_id,
                    id=row[1],
                    tx_id=int(row[2]),  # cast back to int if needed
                    event_type=row[3],
                    json=row[4],
                    version=row[5],
                )
            )

        return events

    def read_events_after_checkpoint(
        self,
        session: t.Any,
        aggregate_type: str,
        last_processed_tx_id: int,
        last_processed_event_id: int,
    ) -> t.List[RecordedEvent]:
        query = textwrap.dedent(
            """
                SELECT
                    e.id,
                    e.transaction_id::text AS tx_id,
                    e.event_type,
                    e.json_data,
                    e.version,
                    e.aggregate_id
                FROM es_event e
                JOIN es_aggregate a ON a.ID = e.aggregate_id
                WHERE a.aggregate_type = :aggregate_type
                AND (e.transaction_id, e.ID) >
                        (CAST(:last_processed_tx_id AS xid8), :last_processed_event_id)
                AND e.transaction_id < pg_snapshot_xmin(pg_current_snapshot())
                ORDER BY e.transaction_id ASC, e.ID ASC
                """
        )
        result = session.execute(
            text(query),
            {
                "aggregate_type": aggregate_type,
                "last_processed_tx_id": last_processed_tx_id,
                "last_processed_event_id": last_processed_event_id,
            },
        )
        rows = result.fetchall()
        events: t.List[RecordedEvent] = []
        for row in rows:
            events.append(
                RecordedEvent(
                    aggregate_type=aggregate_type,
                    aggregate_id=row[5],
                    id=row[0],
                    tx_id=int(row[1]),
                    event_type=row[2],
                    json=row[3],
                    version=row[4],
                )
            )

        return events

    def update_event_subscription(
        self,
        session: t.Any,
        subscription_name: str,
        last_tx_id: int,
        last_event_id: int,
    ) -> bool:
        """Updates the subscription. Does not commit the session."""
        query = textwrap.dedent(
            """
            UPDATE ES_EVENT_SUBSCRIPTION
            SET LAST_TRANSACTION_ID = CAST(:last_tx_id AS xid8),
                LAST_EVENT_ID       = :last_event_id
            WHERE SUBSCRIPTION_NAME = :subscription_name
            """
        )
        stmt = text(query).bindparams(
            bindparam("subscription_name"),
            bindparam("last_tx_id", type_=Text),
            bindparam("last_event_id", type_=Integer),
        )
        result = session.execute(
            stmt,
            {
                "subscription_name": subscription_name,
                "last_tx_id": last_tx_id,
                "last_event_id": last_event_id,
            },
        )
        return result.rowcount > 0
