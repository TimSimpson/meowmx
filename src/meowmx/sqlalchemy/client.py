import typing as t
import sqlalchemy
from sqlalchemy import exc

from .. import common
from . import tables


class Client:
    def __init__(
        self, engine: common.Engine, sessionmaker: common.SessionMaker
    ) -> None:
        self._engine = engine
        self._sessionmaker = sessionmaker

    def setup_tables(self, engine: common.Engine) -> None:
        tables.Base.metadata.create_all(engine)

    def append_event(
        self,
        session: common.Session,
        event: common.NewEvent,
        assumed_aggregate_type: str,
    ) -> common.RecordedEvent:
        """Inserts an event.

        The aggregate type is assumed to be known by the caller.
        """
        new_event = tables.EsEvent(
            aggregate_id=event.aggregate_id,
            version=event.version,
            event_type=event.event_type,
            json_data=event.json,
        )

        session.add(new_event)
        session.flush()

        recorded = common.RecordedEvent(
            aggregate_id=event.aggregate_id,
            aggregate_type=assumed_aggregate_type,
            event_type=event.event_type,
            id=new_event.id,
            json=event.json,
            tx_id=new_event.transaction_id,
            version=event.version,
        )
        return recorded

    def create_aggregate_if_absent(
        self,
        session: common.Session,
        aggregate_type: str,
        aggregate_id: str,  # UUID string – SQLAlchemy will coerce to UUID if the column type is UUID
    ) -> None:
        # start a nested session; if it fails, it's no biggie
        with session.begin_nested() as session2:
            stmt = sqlalchemy.insert(tables.EsAggregate).values(
                id=aggregate_id,
                version=-1,
                aggregate_type=aggregate_type,
            )
            try:
                session2.execute(stmt)
            except exc.IntegrityError:
                pass

    def create_subscription_if_absent(
        self, session: common.Session, subscription_name: str
    ) -> None:
        stmt = sqlalchemy.insert(tables.EsEventSubscription).values(
            subscription_name=subscription_name,
            last_transaction_id=0,
            last_event_id=0,
        )
        try:
            session.execute(stmt)
        except exc.IntegrityError:
            session.rollback()
            session.begin()

    def check_and_update_aggregate_version(
        self,
        session: common.Session,
        aggregate_id: str,
        expected_version: int,
        new_version: int,
    ) -> bool:
        stmt = (
            sqlalchemy.update(tables.EsAggregate)
            .where(
                tables.EsAggregate.id == aggregate_id,
                tables.EsAggregate.version == expected_version,
            )
            .values(version=new_version)
        )

        result = session.execute(stmt)
        return result.rowcount == 1

    def handle_subscription_events(
        self,
        subscription_name: str,
        aggregate_type: str,
        batch_size: int,
        handler: common.EventHandler,
    ) -> int:
        # TODO: This code is duplicated, de-dupe it somehow
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
    ) -> t.Optional[common.SubCheckpoint]:
        stmt = (
            sqlalchemy.select(
                tables.EsEventSubscription.last_transaction_id.label(
                    "last_transaction_id"
                ),
                tables.EsEventSubscription.last_event_id,
            )
            .where(tables.EsEventSubscription.subscription_name == subscription_name)
            .with_for_update(skip_locked=True)
        )

        row = session.execute(stmt).fetchone()
        if row is None:
            return None
        return common.SubCheckpoint(
            last_tx_id=row.last_transaction_id, last_event_id=row.last_event_id
        )

    def read_all_events(
        self,
        session: common.Session,
        from_tx_id: t.Optional[int],
        to_tx_id: t.Optional[int],
        limit: int,
        reverse: bool = False,
    ) -> t.List[common.RecordedEvent]:
        if to_tx_id is None and limit is None:
            raise ValueError(
                "Neither to_tx_id nor limit is set – too many rows would be returned."
            )
        stmt = (
            sqlalchemy.select(
                tables.EsAggregate.aggregate_type,
                tables.EsEvent.id,
                tables.EsEvent.transaction_id.label("tx_id"),
                tables.EsEvent.aggregate_id,
                tables.EsEvent.event_type,
                tables.EsEvent.json_data,
                tables.EsEvent.version,
            )
            .join(
                tables.EsAggregate,
                tables.EsAggregate.id == tables.EsEvent.aggregate_id,
            )
            .where(
                sqlalchemy.and_(
                    (tables.EsEvent.transaction_id > sqlalchemy.literal(from_tx_id))
                    if from_tx_id is not None
                    else sqlalchemy.true(),
                    (tables.EsEvent.transaction_id <= sqlalchemy.literal(to_tx_id))
                    if to_tx_id is not None
                    else sqlalchemy.true(),
                )
            )
            .order_by(
                tables.EsEvent.transaction_id.desc
                if reverse
                else tables.EsEvent.transaction_id.asc
            )
            .limit(limit)
        )

        rows = session.execute(stmt).fetchall()

        return [
            common.RecordedEvent(
                aggregate_type=row[0],
                aggregate_id=row[3],
                id=row[1],
                tx_id=int(row[2]),  # cast back to int if needed
                event_type=row[4],
                json=row[5],
                version=row[6],
            )
            for row in rows
        ]

    def read_events_by_aggregate_id(
        self,
        session: common.Session,
        aggregate_id: str,
        limit: int,
        from_version: t.Optional[int],
        to_version: t.Optional[int],
        reverse: bool = False,
    ) -> t.List[common.RecordedEvent]:
        stmt = (
            sqlalchemy.select(
                tables.EsAggregate.aggregate_type,
                tables.EsEvent.id,
                tables.EsEvent.transaction_id.label("tx_id"),
                tables.EsEvent.event_type,
                tables.EsEvent.json_data,
                tables.EsEvent.version,
            )
            .join(
                tables.EsAggregate,
                tables.EsAggregate.id == tables.EsEvent.aggregate_id,
            )
            .where(
                sqlalchemy.and_(
                    tables.EsEvent.aggregate_id == sqlalchemy.literal(aggregate_id),
                    (tables.EsEvent.version > sqlalchemy.literal(from_version))
                    if from_version is not None
                    else sqlalchemy.true(),
                    (tables.EsEvent.version <= sqlalchemy.literal(to_version))
                    if to_version is not None
                    else sqlalchemy.true(),
                )
            )
            .order_by(
                tables.EsEvent.version.desc if reverse else tables.EsEvent.version.asc
            )
            .limit(limit)
        )
        rows = session.execute(stmt).fetchall()
        return [
            common.RecordedEvent(
                aggregate_type=row[0],
                aggregate_id=aggregate_id,
                id=row[1],
                tx_id=int(row[2]),
                event_type=row[3],
                json=row[4],
                version=row[5],
            )
            for row in rows
        ]

    def read_events_after_checkpoint(
        self,
        session: t.Any,
        aggregate_type: str,
        last_processed_tx_id: int,
        last_processed_event_id: int,
    ) -> t.List[common.RecordedEvent]:
        stmt = (
            sqlalchemy.select(
                tables.EsEvent.id,
                tables.EsEvent.transaction_id.label("tx_id"),
                tables.EsEvent.event_type,
                tables.EsEvent.json_data,
                tables.EsEvent.version,
                tables.EsEvent.aggregate_id,
            )
            .join(
                tables.EsAggregate,
                tables.EsAggregate.id == tables.EsEvent.aggregate_id,
            )
            .where(
                tables.EsAggregate.aggregate_type == sqlalchemy.literal(aggregate_type),
                sqlalchemy.tuple_(tables.EsEvent.transaction_id, tables.EsEvent.id)
                > sqlalchemy.tuple_(
                    sqlalchemy.literal(last_processed_tx_id),
                    sqlalchemy.literal(last_processed_event_id),
                ),
            )
            .order_by(tables.EsEvent.transaction_id.asc(), tables.EsEvent.id.asc())
        )

        rows = session.execute(stmt).fetchall()
        return [
            common.RecordedEvent(
                aggregate_type=aggregate_type,
                aggregate_id=row[5],
                id=row[0],
                tx_id=int(row[1]),
                event_type=row[2],
                json=row[3],
                version=row[4],
            )
            for row in rows
        ]

    def update_event_subscription(
        self,
        session: t.Any,
        subscription_name: str,
        last_tx_id: int,
        last_event_id: int,
    ) -> bool:
        stmt = (
            sqlalchemy.update(tables.EsEventSubscription)
            .where(
                tables.EsEventSubscription.subscription_name
                == sqlalchemy.literal(subscription_name)
            )
            .values(
                last_transaction_id=sqlalchemy.literal(last_tx_id),
                last_event_id=sqlalchemy.literal(last_event_id),
            )
        )

        result = session.execute(stmt)
        return result.rowcount > 0
