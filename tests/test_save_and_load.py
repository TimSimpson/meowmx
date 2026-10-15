from datetime import datetime
import json
import typing as t
from unittest.mock import ANY

import coolname  # type: ignore
import pytest

import meowmx
from meowmx import client as client_module


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


DEFAULT_LIMIT = 512


def test_save_and_load_events(
    meow: meowmx.Client,
    new_uuid: t.Callable[[], str],
    subtests: t.Any,
    monkeypatch: t.Any,
) -> None:
    aggregate_type = "meowmx-test"
    aggregate_id = new_uuid()
    events = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateCreated",
            json=json.dumps(
                {
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateOrderRecieved",
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateDeleted",
            json=json.dumps(
                {
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
    ]
    recorded_events_from_save_result = meow.save_events(
        "meowmx-test", aggregate_id, events, version=0
    )
    actual_times = [
        json.loads(event.json)["time"] for event in recorded_events_from_save_result
    ]

    # Confirm the save function returns the events we expect

    expected_events = [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json=json.dumps(
                {
                    "time": actual_times[0],
                }
            ),
            tx_id=ANY,
            version=0,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": actual_times[1],
                }
            ),
            tx_id=ANY,
            version=1,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateDeleted",
            id=ANY,
            json=json.dumps(
                {
                    "time": actual_times[2],
                }
            ),
            tx_id=ANY,
            version=2,
        ),
    ]

    reversed_expected_events = sorted(
        expected_events, key=lambda r: r.version, reverse=True
    )

    with subtests.test("save result returns valid events"):
        assert recorded_events_from_save_result == expected_events

    with subtests.test("load_events with default arguments"):
        # Confirm they look identical when loading
        all_events_loaded = meow.load_events(
            "meowmx-test", aggregate_id, limit=DEFAULT_LIMIT
        )

        assert not all_events_loaded.has_more
        assert expected_events == all_events_loaded.events

    with subtests.test("load_events with reverse"):
        # Confirm reversing the load is OK

        reversed_all_events_loaded = meow.load_events(
            "meowmx-test",
            aggregate_id,
            reverse=True,
            limit=DEFAULT_LIMIT,
        )

        assert not reversed_all_events_loaded.has_more
        assert reversed_expected_events == reversed_all_events_loaded.events

    with subtests.test("stream_events"):
        all_events_streamed = meow.stream_events(
            "meowmx-test",
            aggregate_id,
        )
        assert expected_events == list(all_events_streamed)

    with subtests.test("stream_events with small chunk and limit"):
        all_events_streamed = meow.stream_events(
            "meowmx-test",
            aggregate_id,
            limit=2,
            chunk_size=1,
        )
        assert expected_events[0:2] == list(all_events_streamed)

    with subtests.test("stream_events with low limits"):
        monkeypatch.setattr(client_module, "DEFAULT_LIMIT", 1)

        all_events_streamed = meow.stream_events(
            "meowmx-test",
            aggregate_id,
        )
        assert expected_events == list(all_events_streamed)

    with subtests.test("delete events"):
        deleted_events = meow.delete_events(aggregate_type, aggregate_id)
        assert deleted_events == 3

        all_events_streamed = meow.stream_events(
            "meowmx-test",
            aggregate_id,
        )
        assert [] == list(all_events_streamed)


def test_save_and_load_events_with_session(
    session_maker: meowmx.SessionMaker,
    meow: meowmx.Client,
    new_uuid: t.Callable[[], str],
) -> None:
    aggregate_type = "meowmx-test"
    aggregate_id = new_uuid()
    events = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateCreated",
            json=json.dumps(
                {
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateOrderRecieved",
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateDeleted",
            json=json.dumps(
                {
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
    ]
    with session_maker() as session:
        with session.begin():
            meow.save_events(
                "meowmx-test", aggregate_id, events, version=0, session=session
            )

            recorded_events, has_more = meow.load_events(
                "meowmx-test", aggregate_id, session=session, limit=DEFAULT_LIMIT
            )

    assert len(recorded_events) == 3
    assert not has_more
    actual_times = [json.loads(event.json)["time"] for event in recorded_events]

    expected_events = [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json=json.dumps(
                {
                    "time": actual_times[0],
                }
            ),
            tx_id=ANY,
            version=0,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": actual_times[1],
                }
            ),
            tx_id=ANY,
            version=1,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateDeleted",
            id=ANY,
            json=json.dumps(
                {
                    "time": actual_times[2],
                }
            ),
            tx_id=ANY,
            version=2,
        ),
    ]

    assert recorded_events == expected_events

    new_time = datetime.now().isoformat()
    with session_maker() as session:
        with session.begin():
            meow.save_events(
                "meowmx-test",
                aggregate_id,
                [
                    meowmx.NewEvent(
                        event_type="MeowMxTestAggregateResurrected",
                        json=json.dumps(
                            {
                                "time": new_time,
                            }
                        ),
                    ),
                ],
                version=3,
                session=session,
            )
            recorded_events_2, has_more = meow.load_events(
                "meowmx-test", aggregate_id, session=session, limit=DEFAULT_LIMIT
            )

    expected_events.append(
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateResurrected",
            id=ANY,
            json=json.dumps(
                {
                    "time": new_time,
                }
            ),
            tx_id=ANY,
            version=3,
        ),
    )
    assert expected_events == recorded_events_2
    assert not has_more

    with session_maker() as session:
        with session.begin():
            meow.save_events(
                "meowmx-test",
                aggregate_id,
                [
                    meowmx.NewEvent(
                        event_type="MeowMxTestAggregateResurrectedAgain",
                        json=json.dumps(
                            {
                                "time": new_time,
                            }
                        ),
                    ),
                ],
                version=None,
                session=session,
            )
            recorded_events_3, has_more = meow.load_events(
                "meowmx-test", aggregate_id, session=session, limit=DEFAULT_LIMIT
            )

    expected_events.append(
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateResurrectedAgain",
            id=ANY,
            json=json.dumps(
                {
                    "time": new_time,
                }
            ),
            tx_id=ANY,
            version=4,
        ),
    )
    assert not has_more
    assert expected_events == recorded_events_3


def test_concurrent_save_check(
    meow: meowmx.Client, new_uuid: t.Callable[[], str]
) -> None:
    aggregate_type = "meowmx-test"
    aggregate_id = new_uuid()

    events = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateCreated",
            json=json.dumps(
                {
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
    ]
    recorded_events = meow.save_events("meowmx-test", aggregate_id, events, version=0)
    event_time_1 = json.loads(recorded_events[0].json)["time"]

    assert recorded_events == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json=json.dumps(
                {
                    "time": event_time_1,
                }
            ),
            tx_id=ANY,
            version=0,
        )
    ]

    with pytest.raises(meowmx.ExpectedVersionFailure):
        meow.save_events("meowmx-test", aggregate_id, events, version=0)

    events2 = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateOrderRecieved",
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": datetime.now().isoformat(),
                }
            ),
        ),
    ]

    recorded_events_2 = meow.save_events(
        "meowmx-test", aggregate_id, events2, version=1
    )
    event_time_2 = json.loads(recorded_events_2[0].json)["time"]

    assert recorded_events_2 == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": event_time_2,
                }
            ),
            tx_id=ANY,
            version=1,
        )
    ]

    recorded_events_from_load, has_more = meow.load_events(
        "meowmx-test", aggregate_id, limit=DEFAULT_LIMIT
    )

    assert has_more is False
    assert recorded_events_from_load == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json=json.dumps(
                {
                    "time": event_time_1,
                }
            ),
            tx_id=ANY,
            version=0,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json=json.dumps(
                {
                    "order_no": 52328,
                    "time": event_time_2,
                }
            ),
            tx_id=ANY,
            version=1,
        ),
    ]


def test_delete_and_recreate(
    meow: meowmx.Client,
    new_uuid: t.Callable[[], str],
    subtests: t.Any,
    monkeypatch: t.Any,
) -> None:
    aggregate_type = "meowmx-test"
    aggregate_id = new_uuid()

    def write_event(version: int) -> None:
        meow.save_events(
            "meowmx-test",
            aggregate_id,
            [
                meowmx.NewEvent(
                    event_type=aggregate_type,
                    json=json.dumps(
                        {
                            "time": datetime.now().isoformat(),
                        }
                    ),
                ),
            ],
            version=version,
        )

    write_event(0)

    deleted_count = meow.delete_events(aggregate_type, aggregate_id)

    assert deleted_count == 1
    assert [] == list(
        meow.stream_events(
            "meowmx-test",
            aggregate_id,
        )
    )
    with pytest.raises(meowmx.ExpectedVersionFailure):
        write_event(1)

    write_event(0)
    write_event(1)
    write_event(2)

    deleted_count = meow.delete_events(aggregate_type, aggregate_id, from_version=1)
    assert deleted_count == 2
    assert 1 == len(
        list(
            meow.stream_events(
                "meowmx-test",
                aggregate_id,
            )
        )
    )

    with pytest.raises(meowmx.ExpectedVersionFailure):
        write_event(0)

    with pytest.raises(meowmx.ExpectedVersionFailure):
        write_event(2)

    for i in range(1, 30):
        write_event(i)

    deleted_count = meow.delete_events(aggregate_type, aggregate_id, from_version=15)
    assert 15 == deleted_count
    assert 15 == len(
        list(
            meow.stream_events(
                "meowmx-test",
                aggregate_id,
            )
        )
    )

    meow.delete_events(aggregate_type, aggregate_id)
