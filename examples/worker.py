import argparse
import time

import demolib
import meowmx


def main() -> None:
    parser = argparse.ArgumentParser(description="demo of a worker")
    parser.add_argument(
        "--sub-name",
        type=str,
        required=True,
        help="the name of the subscription",
    )
    parser.add_argument(
        "--aggregate-type",
        type=str,
        required=True,
        help="the type of aggregate to watch",
    )

    args = parser.parse_args()

    meow = meowmx.Client(demolib.DEMO_PG_URL)

    start_time = time.perf_counter()
    count = 0

    def handler(session: meowmx.Session, event: meowmx.RecordedEvent):
        existing_events = meow.load(event.aggregate_type, event.aggregate_id)
        end_time = time.perf_counter()
        nonlocal count
        count += 1
        elapsed = end_time - start_time
        avg_count_per_second = count / elapsed
        print(
            f"loaded {len(existing_events)} event(s) {count} total... ({elapsed} elapsed, {avg_count_per_second} per/s)"
        )

    meow.sub(
        args.sub_name,
        args.aggregate_type,
        batch_size=200,
        timelimit=10,
        handler=handler,
    )


if __name__ == "__main__":
    main()
