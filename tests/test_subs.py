import random
import threading
import time
import typing as t
import uuid

import coolname  # type: ignore

import meowmx


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


class AggregateWriter:
    """Writes semi-random events where the aggregate and event types are known."""

    def __init__(
        self,
        aggregate_type: str,
        event_types: t.List[str],
        percent_new: int,
        max_length: int,
    ) -> None:
        self._aggregate_type = aggregate_type
        self._aggregate_versions: t.Dict[str, int] = {}
        self._aggregate_ids: t.List = []
        self._event_types = event_types
        self._count = 0
        self._percent_new = percent_new
        self._max_length = max_length
        self._events: t.List[meowmx.RecordedEvent] = []

    @property
    def written_events(self) -> t.List[meowmx.RecordedEvent]:
        return self._events

    def write_event(self, meow: meowmx.Client) -> None:
        """Performs an iteration by writing an aggregate."""
        choice = random.random() * 100
        if len(self._aggregate_ids) == 0 or choice < self._percent_new:
            aggregate_id = str(uuid.uuid4())
            self._aggregate_versions[aggregate_id] = -1
            self._aggregate_ids = list(self._aggregate_versions.keys())
            version = 0
        else:
            random_index = int(random.random() * (len(self._aggregate_ids) - 1))
            aggregate_id = self._aggregate_ids[random_index]
            # take time to load events
            existing_events = meow.load(
                self._aggregate_type, aggregate_id, 0, limit=36500
            )
            version = self._aggregate_versions[aggregate_id]
            if version != len(existing_events):
                print(f"ERROR! version we expected={version}")
                print(f"                but we got={len(existing_events)}")
                raise RuntimeError("bad version")
            version += 1

        if len(self._event_types) > 1:
            random_index = int(random.random() * (len(self._event_types) - 1))
            event_type = self._event_types[random_index]
        else:
            event_type = self._event_types[0]

        events = [
            meowmx.NewEvent(
                aggregate_id=aggregate_id,
                event_type=event_type,
                json={
                    "version": version,
                    "random_slug": _generate_slug(),
                },
                version=version,
            )
        ]
        recorded_events = meow.save_events(self._aggregate_type, aggregate_id, events)
        self._events.append(recorded_events[0])
        self._aggregate_versions[aggregate_id] += 1
        self._count += 1

        if version > self._max_length:
            # remove this so we stop writing to it
            del self._aggregate_versions[aggregate_id]
            self._aggregate_ids = list(self._aggregate_versions.keys())


class Worker:
    def __init__(self, sub_name: str, aggregate_type: str) -> None:
        """Simulates a typical worker which subscribes to some aggregate type."""
        self._sub_name = sub_name
        self._aggregate_type = aggregate_type
        self._events: t.Dict[int, meowmx.RecordedEvent] = {}
        self._replayed_events = 0
        self._seen_event_count = 0

    @property
    def seen_events(self) -> t.Dict[int, meowmx.RecordedEvent]:
        """Every event this worker has seen. Probably not thread safe?"""
        return self._events

    @property
    def seen_event_count(self) -> int:
        """The count of all events."""
        return self._seen_event_count

    def start_subscription(
        self, meow: meowmx.Client, stop_signal: threading.Event
    ) -> None:
        def handler(session: meowmx.Session, event: meowmx.RecordedEvent) -> None:
            if event.id in self._events:
                # reminder: the goal is at least once delivery
                self._replayed_events += 1
            self._seen_event_count += 1
            self._events[event.id] = event
            # load the event stream to simulate what most workers would do
            meow.load(event.aggregate_type, event.aggregate_id)

        meow.sub(
            self._sub_name,
            self._aggregate_type,
            batch_size=200,
            max_sleep_time=10,
            handler=handler,
            stop_signal=stop_signal,
        )


def test_subscriptions(meow: meowmx.Client) -> None:
    rname = _generate_slug()
    aggregate_type = f"meowmx-st-{rname}"
    event_types = [
        "MeoxMxStOrderCreated",
        "MeoxMxStOrderShipped",
        "MeoxMxStOrderLost",
        "MeoxMxStOrderFulfilled",
    ]
    writer = AggregateWriter(aggregate_type, event_types, percent_new=25, max_length=85)

    stop_signal = threading.Event()

    # imagine we have three workers:
    # * one looks at all the events of an order, maybe to build a read model
    # * one just cares about shipping
    # * one only cares about tracking
    worker_orders = Worker(f"meowmx-st-{rname}-orders", aggregate_type)
    worker_shipped = Worker(f"meowmx-st-{rname}-shipped", aggregate_type)
    worker_tracker = Worker(f"meowmx-st-{rname}-tracker", aggregate_type)

    # start the workers, each in a different thread

    def worker1() -> None:
        worker_orders.start_subscription(meow, stop_signal)

    def worker2() -> None:
        worker_shipped.start_subscription(meow, stop_signal)

    def worker3() -> None:
        worker_tracker.start_subscription(meow, stop_signal)

    t1 = threading.Thread(target=worker1)
    t1.start()
    t2 = threading.Thread(target=worker2)
    t2.start()
    t3 = threading.Thread(target=worker3)
    t3.start()

    # Now write some number of events

    event_count = 400
    for i in range(event_count):
        writer.write_event(meow)

    # wait for the threads to be done processing them. This will hang
    # if there's a bug!

    while (
        worker_orders.seen_event_count < event_count
        or worker_shipped.seen_event_count < event_count
        or worker_tracker.seen_event_count < event_count
    ):
        time.sleep(1)

    # Signal the worker threads to stop
    stop_signal.set()

    t1.join()
    t2.join()
    t3.join()

    if True:
        for event in writer.written_events:
            print(f"wrote event: {event}")

        for event2 in worker_orders.seen_events:
            print(f"worker orders: {event2}")

        for event2 in worker_shipped.seen_events:
            print(f"worker shipped: {event2}")

        for event2 in worker_tracker.seen_events:
            print(f"worker tracker: {event2}")

    missing = []
    for event in writer.written_events:
        print(f"wrote event: {event}")
        # check that all workers got this event
        if event.id not in worker_orders.seen_events:
            print(f"!!! missing event.id: {event.id}")
            missing.append(event)
        else:
            workers_version = worker_orders.seen_events[event.id]
            assert workers_version == event

    assert worker_orders.seen_event_count == event_count
    assert len(missing) == 0
