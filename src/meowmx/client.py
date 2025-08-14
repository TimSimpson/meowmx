import typing as t
from event_sourcery.event_store import Event, Recorded
from event_sourcery_sqlalchemy import configure_models
from event_sourcery_sqlalchemy import SQLAlchemyBackendFactory
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
import sqlalchemy.exc
from event_sourcery.event_store.exceptions import ConcurrentStreamWriteError

from . import registry


class Base(DeclarativeBase):
    pass


class Client:
    def __init__(self) -> None:
        self._engine = create_engine(
            "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable",
        )
        self._sessionmaker = sessionmaker(
            autocommit=False, autoflush=False, bind=self._engine
        )

    def setup_tables(self) -> None:
        configure_models(Base)  # Base is your declarative base class
        Base.metadata.create_all(self._engine)

    def load(self, *args, **kwargs) -> t.Any:
        with self._sessionmaker() as session:
            factory = SQLAlchemyBackendFactory(session)
            backend = factory.build()
            return backend.event_store.load_stream(*args, **kwargs)

    def publish(self, *args, **kwargs) -> t.Any:
        with self._sessionmaker() as session:
            factory = SQLAlchemyBackendFactory(session)
            backend = factory.build()
            try:
                backend.event_store.append(*args, **kwargs)
                session.commit()
            except sqlalchemy.exc.IntegrityError as ie:
                # If a brand new stream is written from two different processes,
                # it will sometimes throw a SQL Alchemy exception instead of
                # the trusty ConcurrentStreamWriteError.
                if "duplicate key value violates" in str(ie):
                    raise ConcurrentStreamWriteError()

    def sub(
        self,
        category: t.Optional[str] = None,
        start_from: int = 0,
        types: t.Optional[t.List[t.Type[Event]]] = None,
        batch_size: int = 10,
        timelimit: int = 1,
    ) -> t.Iterator[Recorded]:
        with self._sessionmaker() as session:
            factory = SQLAlchemyBackendFactory(session)
            reg = registry.LenientEventRegistry()
            factory.with_event_registry(reg)
            backend = factory.build()
            filter_phase = backend.subscriber.start_from(start_from)
            if types:
                build_phase = filter_phase.to_events(types)
            elif category:
                build_phase = filter_phase.to_category(category)
            else:
                build_phase = filter_phase
            iterator = build_phase.build_batch(size=batch_size, timelimit=timelimit)
            for batch in iterator:
                for event in batch:
                    yield event
