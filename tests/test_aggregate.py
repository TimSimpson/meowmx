import json
import typing as t

import coolname  # type: ignore
import meowmx
import pytest


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


class Aggregate:
    """This has the most basic possible implementation of an aggregate.

    To see an example that uses the EventBuffer and also features an apply
    method that uses it's own custom base type, look at `test_buffer`.
    """

    def __init__(
        self, recorded_events: t.Optional[t.List[meowmx.RecordedEvent]] = None
    ) -> None:
        self._current_state = "none"
        self._id = ""
        self._new_events: t.List[meowmx.NewEvent] = []
        self._next_version = 0
        if recorded_events is not None:
            for event in recorded_events:
                self.apply(event)
            self._next_version = len(self._new_events)
            self._new_events = []

    @property
    def aggregate_id(self) -> str:
        return self._id

    aggregate_type = "test_pending_aggregate"

    def apply(self, event: meowmx.NewEvent) -> None:
        if event.event_type == "Start":
            self._id = json.loads(event.json)["id"]
            self._current_state = "new"
        elif event.event_type == "Finish":
            self._current_state = "old"
        elif event.event_type == "Restart":
            self._current_state = "reborn"
        else:
            self._current_state = "error"
        self._new_events.append(event)

    def collect_pending_events(self) -> meowmx.PendingEvents:
        result = meowmx.PendingEvents(self._new_events, self._next_version)
        self._next_version += len(self._new_events)
        self._new_events = []
        return result


def test_save_load_aggregate(
    meow: meowmx.Client, new_uuid: t.Callable[[], str]
) -> None:
    agg = Aggregate()
    recorded_events = meow.save_aggregate(agg)
    assert len(recorded_events) == 0
    new_id = new_uuid()

    agg.apply(meowmx.NewEvent(event_type="Start", json=f'{{"id": "{new_id}"}}'))
    agg._current_state == "new"
    recorded_events = meow.save_aggregate(agg)
    assert len(recorded_events) == 1
    assert recorded_events[0].json == f'{{"id": "{new_id}"}}'

    agg.apply(meowmx.NewEvent(event_type="Finish", json="{}"))
    agg._current_state == "old"
    recorded_events = meow.save_aggregate(agg)
    assert len(recorded_events) == 1

    # Now load the aggregate

    agg2 = meow.load_aggregate(Aggregate, new_id)
    assert agg2._current_state == agg._current_state
    assert agg2._id == agg._id
    assert agg2._next_version == agg._next_version

    # save the new instance

    agg2.apply(meowmx.NewEvent(event_type="Restart", json="{}"))
    agg2._current_state == "reborn"
    recorded_events = meow.save_aggregate(agg2)
    assert len(recorded_events) == 1

    # try to save the old instance and get a concurrency error

    agg.apply(meowmx.NewEvent(event_type="Restart", json="{}"))
    agg._current_state == "reborn"
    with pytest.raises(meowmx.ExpectedVersionFailure):
        meow.save_aggregate(agg)
