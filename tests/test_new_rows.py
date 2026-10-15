import json
import typing as t

import sqlalchemy
from sqlalchemy import orm

import meowmx


class Base(orm.DeclarativeBase):
    pass


class OrangeReadModel(Base):
    __tablename__ = "oranges"

    id = orm.mapped_column(sqlalchemy.CHAR(64), primary_key=True)
    description = orm.mapped_column(sqlalchemy.String, nullable=False)
    version = orm.mapped_column(sqlalchemy.Integer, nullable=False)


class Event:
    """A simple base event type."""

    def __init__(self, event_type: str, id: str, value: dict) -> None:
        self.event_type = event_type
        self.id = id
        self.value = value

    def to_event_data(self) -> str:
        return json.dumps(self.value)


def load_event(event: meowmx.RecordedEvent) -> Event:
    return Event(
        event_type=event.event_type, id=event.aggregate_id, value=json.loads(event.json)
    )


class OrangeAggregate:
    """Represents the event an orange can go through."""

    def __init__(
        self,
        recorded_events: t.Optional[t.Sequence[meowmx.RecordedEvent]] = None,
        new_id: t.Optional[str] = None,
    ) -> None:
        self._description = ""
        self._id: t.Optional[str] = None
        self._version = -1
        self._events = meowmx.EventBuffer(
            loader=load_event,
            applier=self._apply,
        )
        if recorded_events is not None:
            self._events.load_recorded_events(recorded_events)
        elif new_id is not None:
            self._events.emit(
                Event("OrangeCreated", id=new_id, value={"description": "new"})
            )
        else:
            raise ValueError("bad args")
        if self._id is None:
            raise RuntimeError("malformed event stream: no ID found")

    @property
    def aggregate_id(self) -> str:
        assert self._id is not None
        return self._id

    aggregate_type = "test_read_model"

    def _apply(self, event: Event) -> None:
        # this is a terrible way to design events, but for this tests
        # let's assume we update the state whenever the event has a description
        # field.
        if "description" in event.value:
            self._description = event.value["description"]
        if self._id is None:
            if event.id is None:
                raise RuntimeError("malformed event stream: no ID found")
            self._id = event.id
        else:
            if event.id != self._id:
                raise RuntimeError(
                    "Malformed event: found new, different ID in event stream"
                )
        self._version += 1

    def collect_pending_events(self) -> meowmx.PendingEvents:
        return self._events.collect_pending_events()

    @property
    def description(self) -> str:
        return self._description

    def update_description(self, description: str) -> None:
        if description != self._description:
            self._events.emit(
                Event(
                    "OrangeDescriptionUpdated",
                    id=self.aggregate_id,
                    value={"description": description},
                )
            )

    @property
    def version(self) -> int:
        return self._version


def update_orange_readmodel(
    session: meowmx.Session,
    meow: meowmx.Client,
    orange_id: str,
) -> None:
    orange = meow.load_aggregate(OrangeAggregate, orange_id, session=session)
    new_row = OrangeReadModel(
        id=orange.aggregate_id, description=orange.description, version=orange.version
    )
    session.merge(new_row)


# def update_orange_readmodel(
#     session: meowmx.Session,
#     meow: meowmx.Client,
#     orange_id: str,
#     description: str,
# ) -> None:
#     orange = meow.load_aggregate(OrangeAggregate, orange_id)
#     new_row = OrangeReadModel(id=orange.aggregate_id, description=orange.description)
#     session.merge(new_row)
#     session.commit()

#     orange.update_description(description)


# This exists to test row writes for a simple sanity-check benchmark
def test_write_oranges(
    engine: meowmx.Engine,
    meow: meowmx.Client,
    iterations_cmd_opt: t.Optional[int],
    new_uuid: t.Callable[[], str],
) -> None:
    Base.metadata.create_all(engine)

    expected_agg_info: t.Dict[str, int] = {}

    with orm.Session(engine) as session:
        for i in range(iterations_cmd_opt or 1):
            new_id = new_uuid()
            with session.begin():
                new_orange = OrangeAggregate(new_id=new_id)
                meow.save_aggregate(new_orange, session=session)
                update_orange_readmodel(
                    session,
                    meow,
                    new_id,
                )
            expected_agg_info[new_id] = 0

            if i % 2 == 0:
                continue

            with session.begin():
                loaded_orange = meow.load_aggregate(
                    OrangeAggregate, new_id, session=session
                )
                loaded_orange.update_description("ripe")
                meow.save_aggregate(loaded_orange, session=session)
                update_orange_readmodel(
                    session,
                    meow,
                    new_id,
                )
            expected_agg_info[new_id] = 1

            if i % 3 == 0:
                continue

            with session.begin():
                loaded_orange_2 = meow.load_aggregate(
                    OrangeAggregate, new_id, session=session
                )
                loaded_orange_2.update_description("rotten")
                meow.save_aggregate(loaded_orange_2, session=session)
                update_orange_readmodel(
                    session,
                    meow,
                    new_id,
                )
            expected_agg_info[new_id] = 2

    all_aggs: t.List[meowmx.AggregateInfo] = []
    next_id: t.Optional[str] = None
    while True:
        agg_info, next_id = meow.list_aggregates(
            aggregate_type=OrangeAggregate.aggregate_type, from_id=next_id, limit=128
        )
        all_aggs.extend(agg_info)
        if next_id is None:
            break
