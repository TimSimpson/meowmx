from datetime import datetime
import json
import typing as t
from unittest.mock import ANY

import coolname  # type: ignore
import pytest

import meowmx


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


def test_save_and_load_events(
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
    recorded_events = meow.save_events("meowmx-test", aggregate_id, events, version=0)
    actual_times = [json.loads(event.json)["time"] for event in recorded_events]

    assert recorded_events == [
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

            recorded_events = meow.load_events(
                "meowmx-test", aggregate_id, session=session
            )

    assert len(recorded_events) == 3
    actual_times = [json.loads(event.json)["time"] for event in recorded_events]

    assert recorded_events == [
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

    recorded_events_from_load = meow.load_events("meowmx-test", aggregate_id)

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
