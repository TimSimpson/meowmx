import argparse
import random
import time
import typing as t
import uuid

import coolname  # type: ignore

import demolib
import meowmx


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="demo of what writing to an aggregate looks like"
    )
    parser.add_argument(
        "--aggregate-type",
        type=str,
        required=True,
        help="the type of aggregate. Sometimes called the category of a stream.",
    )
    parser.add_argument(
        "--event-types",
        type=str,
        required=True,
        help="list of event types, seperated by a comma",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=None,
        required=True,
        help="the maximum length of a stream",
    )
    parser.add_argument(
        "--percent-new",
        type=int,
        default=None,
        required=True,
        help="# out of 100. The percent of writes which should be new aggregates",
    )

    args = parser.parse_args()
    args.percent_new = args.percent_new or 25

    meow = demolib.create_meowmx()

    aggregate_versions: t.Dict[str, int] = {}
    aggregate_ids: t.List = []

    event_types = args.event_types.split(",")

    start_time = time.perf_counter()

    count = 0

    while True:
        choice = random.random() * 100
        if len(aggregate_ids) == 0 or choice < args.percent_new:
            aggregate_id = str(uuid.uuid4())
            aggregate_versions[aggregate_id] = -1
            aggregate_ids = list(aggregate_versions.keys())
            version = 0
        else:
            random_index = int(random.random() * (len(aggregate_ids) - 1))
            aggregate_id = aggregate_ids[random_index]
            # take time to load events
            existing_events = meow.load(
                args.aggregate_type, aggregate_id, 0, limit=36500
            )
            version = aggregate_versions[aggregate_id]
            if version != len(existing_events):
                print(f"ERROR! version we expected={version}")
                print(f"                but we got={len(existing_events)}")
                raise RuntimeError("bad version")
            version += 1

        if len(event_types) > 1:
            random_index = int(random.random() * (len(event_types) - 1))
            event_type = event_types[random_index]
        else:
            event_type = event_types[0]

        events = [
            meowmx.NewEvent(
                event_type=event_type,
                json={
                    "version": version,
                    "random_slug": _generate_slug(),
                },
            )
        ]
        meow.save_events(args.aggregate_type, aggregate_id, events, version)
        aggregate_versions[aggregate_id] += 1
        count += 1

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        avg_count_per_second = count / elapsed

        print(
            f"saved {args.aggregate_type} / {aggregate_id} / {version}, {count} total... ({elapsed} elapsed, {avg_count_per_second} per/s)"
        )

        if version > args.max_length:
            # remove this so we stop writing to it
            del aggregate_versions[aggregate_id]
            aggregate_ids = list(aggregate_versions.keys())


if __name__ == "__main__":
    main()
