import abc
import json
import typing as t

import coolname  # type: ignore
import meowmx
import pytest


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


class Base(abc.ABC):
    """Represents a base type for events.

    This can be anything as long as it supports the `event_type` property and
    the `to_event_data` function.
    """

    event_type = "huh"

    @abc.abstractmethod
    def to_event_data(self) -> str: ...


class Start(Base):
    event_type = "Start"

    def __init__(self, id: str) -> None:
        self._id = id

    def to_event_data(self) -> str:
        return f'{{"id": "{self._id}"}}'


class Finish(Base):
    event_type = "Finish"

    def to_event_data(self) -> str:
        return "{}"


class Restart(Base):
    event_type = "Restart"

    def to_event_data(self) -> str:
        return "{}"


def load_event(event: meowmx.RecordedEvent) -> Base:
    if event.event_type == "Start":
        id = json.loads(event.json)["id"]
        return Start(id)
    elif event.event_type == "Finish":
        return Finish()
    elif event.event_type == "Restart":
        return Restart()
    else:
        raise RuntimeError("psdgf")


class Aggregate:
    """This uses an EventBuffer to implement an aggregate.

    It also shows how to use an internal _apply method that works with a custom
    Base type.
    """

    def __init__(
        self,
        recorded_events: t.Optional[t.List[meowmx.RecordedEvent]] = None,
        new_id: t.Optional[str] = None,
    ) -> None:
        self._events = meowmx.EventBuffer(
            loader=load_event,
            applier=self._apply,
        )
        self._current_state = "none"
        self._id = ""
        if recorded_events is not None:
            self._events.load_recorded_events(recorded_events)
        elif new_id is not None:
            self._events.emit(Start(new_id))
        else:
            raise ValueError("bad args")

    @property
    def aggregate_id(self) -> str:
        return self._id

    aggregate_type = "test_pending_aggregate"

    def _apply(self, event: Base) -> None:
        match event:
            case Start():
                self._id = event._id
                self._current_state = "new"
            case Finish():
                self._current_state = "old"
            case Restart():
                self._current_state = "reborn"
            case _:
                self._current_state = "error"

    def collect_pending_events(self) -> meowmx.PendingEvents:
        return self._events.collect_pending_events()

    def finish(self) -> None:
        self._events.emit(Finish())

    def restart(self) -> None:
        self._events.emit(Restart())


def test_save_load_aggregate(
    meow: meowmx.Client, new_uuid: t.Callable[[], str]
) -> None:
    new_id = new_uuid()
    agg = Aggregate(new_id=new_id)
    agg._current_state == "new"
    recorded_events = meow.save_aggregate(agg)
    assert len(recorded_events) == 1
    assert recorded_events[0].json == f'{{"id": "{new_id}"}}'

    agg.finish()
    agg._current_state == "old"
    recorded_events = meow.save_aggregate(agg)
    assert len(recorded_events) == 1

    # Now load the aggregate

    agg2 = meow.load_aggregate(Aggregate, new_id)
    assert agg2._current_state == agg._current_state
    assert agg2._id == agg._id

    # save the new instance

    agg2.restart()
    agg2._current_state == "reborn"
    recorded_events = meow.save_aggregate(agg2)
    assert len(recorded_events) == 1

    # try to save the old instance and get a concurrency error

    agg.restart()
    agg._current_state == "reborn"
    with pytest.raises(meowmx.ExpectedVersionFailure):
        meow.save_aggregate(agg)
