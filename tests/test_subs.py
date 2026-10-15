import json
import random
import threading
import time
import traceback
import typing as t

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

    def write_event(self, meow: meowmx.Client, new_uuid: t.Callable[[], str]) -> None:
        """Performs an iteration by writing an aggregate."""
        choice = random.random() * 100
        if len(self._aggregate_ids) == 0 or choice < self._percent_new:
            aggregate_id = new_uuid()
            self._aggregate_versions[aggregate_id] = -1
            self._aggregate_ids = list(self._aggregate_versions.keys())
            version = 0
        else:
            random_index = int(random.random() * (len(self._aggregate_ids) - 1))
            aggregate_id = self._aggregate_ids[random_index]
            # take time to load events
            existing_events = list(
                meow.load_events(aggregate_id, from_version=0, limit=36500).events
            )
            version = self._aggregate_versions[aggregate_id]
            if version != (len(existing_events) - 1):
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
                event_type=event_type,
                json=json.dumps(
                    {
                        "version": version,
                        "random_slug": _generate_slug(),
                    }
                ),
            )
        ]
        print(f" -> writing {self._aggregate_type} - {aggregate_id} - {version}...")
        recorded_events = meow.save_events(
            self._aggregate_type, aggregate_id, events, version
        )
        self._events.append(recorded_events[0])
        self._aggregate_versions[aggregate_id] += 1
        self._count += 1

        if version > self._max_length:
            # remove this so we stop writing to it
            del self._aggregate_versions[aggregate_id]
            self._aggregate_ids = list(self._aggregate_versions.keys())


class Worker:
    def __init__(
        self, sub_name: str, aggregate_types: str | list[str] | meowmx.All
    ) -> None:
        """Simulates a typical worker which subscribes to some aggregate type."""
        self._sub_name = sub_name
        self._aggregate_types = aggregate_types
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
            if isinstance(self._aggregate_types, str):
                if event.aggregate_type != self._aggregate_types:
                    raise AssertionError(
                        "Received an aggregate type the worker isn't subscribed to"
                    )
            elif isinstance(self._aggregate_types, list):
                if event.aggregate_type not in self._aggregate_types:
                    raise AssertionError(
                        "Received an aggregate type the worker isn't subscribed to"
                    )

            if event.id in self._events:
                # reminder: the goal is at least once delivery
                self._replayed_events += 1
            self._seen_event_count += 1
            self._events[event.id] = event
            # load the event stream to simulate what most workers would do
            list(meow.iter_events(event.aggregate_id))

        meow.sub(
            self._sub_name,
            aggregate_types=self._aggregate_types,
            batch_size=200,
            max_sleep_time=10,
            handler=handler,
            stop_signal=stop_signal,
        )


# @pytest.mark.timeout(30)
def test_parallel_subscriptions(
    meow: meowmx.Client,
    iterations_cmd_opt: t.Optional[int],
    new_uuid: t.Callable[[], str],
) -> None:
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

    errors_in_thread = None

    def worker1() -> None:
        try:
            worker_orders.start_subscription(meow, stop_signal)
        except Exception as e:
            print(f"ERROR IN THREAD: {e}")
            nonlocal errors_in_thread
            errors_in_thread = True
            traceback.print_exc()

    def worker2() -> None:
        try:
            worker_shipped.start_subscription(meow, stop_signal)
        except Exception as e:
            print(f"ERROR IN THREAD: {e}")
            nonlocal errors_in_thread
            errors_in_thread = True
            traceback.print_exc()

    def worker3() -> None:
        try:
            worker_tracker.start_subscription(meow, stop_signal)
        except Exception as e:
            print(f"ERROR IN THREAD: {e}")
            nonlocal errors_in_thread
            errors_in_thread = True
            traceback.print_exc()

    t1 = threading.Thread(target=worker1)
    t1.start()
    t2 = threading.Thread(target=worker2)
    t2.start()
    t3 = threading.Thread(target=worker3)
    t3.start()

    # Now write some number of events

    max_count = 400
    if iterations_cmd_opt is not None:
        max_count = iterations_cmd_opt

    try:
        event_count = max_count
        for i in range(event_count):
            writer.write_event(meow, new_uuid)
    except Exception as e:
        print(f"ERROR IN MAIN THREAD: {e}")
        traceback.print_exc()
        errors_in_thread = True

    # wait for the threads to be done processing them. This will hang
    # if there's a bug!
    if not errors_in_thread:
        while (
            worker_orders.seen_event_count < event_count
            or worker_shipped.seen_event_count < event_count
            or worker_tracker.seen_event_count < event_count
        ):
            if errors_in_thread:
                break
            time.sleep(1)

    # Signal the worker threads to stop
    stop_signal.set()

    t1.join()
    t2.join()
    t3.join()

    assert not errors_in_thread

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


def test_subscriptions_to_multiple_aggregate_types(
    meow: meowmx.Client,
    iterations_cmd_opt: t.Optional[int],
    new_uuid: t.Callable[[], str],
) -> None:
    rname = _generate_slug()
    aggregate_type_1 = f"meowmx-st-1-{rname}"
    aggregate_type_2 = f"meowmx-st-2-{rname}"
    aggregate_type_3 = f"meowmx-st-3-{rname}"
    event_types = [
        "MeoxMxStOrderCreated",
        "MeoxMxStOrderShipped",
        "MeoxMxStOrderLost",
        "MeoxMxStOrderFulfilled",
    ]
    writer_1 = AggregateWriter(
        aggregate_type_1, event_types, percent_new=25, max_length=85
    )
    writer_2 = AggregateWriter(
        aggregate_type_2, event_types, percent_new=25, max_length=85
    )
    writer_3 = AggregateWriter(
        aggregate_type_3, event_types, percent_new=25, max_length=85
    )

    stop_signal = threading.Event()

    # set up three workers. One listens only to one aggregate, the next listens
    # to the first and second, while the last listens to all three.
    workers: list[Worker] = []
    workers.append(Worker(f"meowmx-st-1-{rname}", aggregate_type_1))
    workers.append(
        Worker(f"meowmx-st-12-{rname}", [aggregate_type_1, aggregate_type_2])
    )
    workers.append(
        Worker(
            f"meowmx-st-123-{rname}",
            [aggregate_type_1, aggregate_type_2, aggregate_type_3],
        )
    )
    workers.append(Worker(f"meowmx-st-all-{rname}", meowmx.All()))

    # start the workers, each in a different thread

    errors_in_thread = None

    def make_worker_func(worker: Worker) -> t.Callable:
        def func() -> None:
            try:
                worker.start_subscription(meow, stop_signal)
            except Exception as e:
                print(f"ERROR IN THREAD: {e}")
                nonlocal errors_in_thread
                errors_in_thread = True
                traceback.print_exc()

        return func

    threads: list[threading.Thread] = [
        threading.Thread(target=make_worker_func(worker)) for worker in workers
    ]
    for th in threads:
        th.start()

    # Now write some number of events

    max_count = 400
    assert max_count % 4 == 0
    if iterations_cmd_opt is not None:
        max_count = iterations_cmd_opt

    stop_at = [0, 0, 0, 0]
    try:
        event_count = max_count
        for i in range(event_count):
            writer_1.write_event(meow, new_uuid)
            stop_at[0] += 1
            stop_at[1] += 1
            stop_at[2] += 1
            stop_at[3] += 1
            if i % 2 == 0:
                writer_2.write_event(meow, new_uuid)
                stop_at[1] += 1
                stop_at[2] += 1
                stop_at[3] += 1
            if i % 4 == 0:
                writer_3.write_event(meow, new_uuid)
                stop_at[2] += 1
                stop_at[3] += 1
    except Exception as e:
        print(f"ERROR IN MAIN THREAD: {e}")
        traceback.print_exc()
        errors_in_thread = True

    # wait for the threads to be done processing them. This will hang
    # if there's a bug!
    if not errors_in_thread:
        while any(
            worker.seen_event_count < stop_at[index]
            for index, worker in enumerate(workers)
        ):
            if errors_in_thread:
                break
            time.sleep(1)

    # Signal the worker threads to stop
    stop_signal.set()

    for th in threads:
        th.join()

    assert not errors_in_thread
