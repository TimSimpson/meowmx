from datetime import datetime
import typing as t
import uuid
from unittest.mock import ANY

import coolname  # type: ignore
import pytest

import meowmx


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


def test_save_and_load_events(meow: meowmx.Client) -> None:
    aggregate_type = "meowmx-test"
    aggregate_id = str(uuid.uuid4())
    events = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateCreated",
            json={
                "time": datetime.now().isoformat(),
            },
        ),
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateOrderRecieved",
            json={
                "order_no": 52328,
                "time": datetime.now().isoformat(),
            },
        ),
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateDeleted",
            json={
                "time": datetime.now().isoformat(),
            },
        ),
    ]
    recorded_events = meow.save_events("meowmx-test", aggregate_id, events, version=0)

    assert recorded_events == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json={
                "time": ANY,
            },
            tx_id=ANY,
            version=0,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json={
                "order_no": 52328,
                "time": ANY,
            },
            tx_id=ANY,
            version=1,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateDeleted",
            id=ANY,
            json={
                "time": ANY,
            },
            tx_id=ANY,
            version=2,
        ),
    ]


def test_concurrent_save_check(meow: meowmx.Client) -> None:
    aggregate_type = "meowmx-test"
    aggregate_id = str(uuid.uuid4())

    events = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateCreated",
            json={
                "time": datetime.now().isoformat(),
            },
        ),
    ]
    recorded_events = meow.save_events("meowmx-test", aggregate_id, events, version=0)
    assert recorded_events == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json={
                "time": ANY,
            },
            tx_id=ANY,
            version=0,
        )
    ]

    with pytest.raises(meowmx.ExpectedVersionFailure):
        meow.save_events("meowmx-test", aggregate_id, events, version=0)

    events2 = [
        meowmx.NewEvent(
            event_type="MeowMxTestAggregateOrderRecieved",
            json={
                "order_no": 52328,
                "time": datetime.now().isoformat(),
            },
        ),
    ]

    recorded_events_2 = meow.save_events(
        "meowmx-test", aggregate_id, events2, version=1
    )

    assert recorded_events_2 == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json={
                "order_no": 52328,
                "time": ANY,
            },
            tx_id=ANY,
            version=1,
        )
    ]

    recorded_events_from_load = meow.load("meowmx-test", aggregate_id)

    assert recorded_events_from_load == [
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateCreated",
            id=ANY,
            json={
                "time": ANY,
            },
            tx_id=ANY,
            version=0,
        ),
        meowmx.RecordedEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type="MeowMxTestAggregateOrderRecieved",
            id=ANY,
            json={
                "order_no": 52328,
                "time": ANY,
            },
            tx_id=ANY,
            version=1,
        ),
    ]
