import argparse
import json
import sys
import time
import demolib
from meowmx.backoff import BackoffCalc


def main() -> None:
    parser = argparse.ArgumentParser(description="read all events")
    parser.add_argument(
        "--aggregate-type",
        type=str,
        default=None,
        help="The aggregate type to read from. If left unspecified all events are read.",
    )
    parser.add_argument(
        "--aggregate-id",
        type=str,
        default=None,
        help="The event ID to read from. If left unspecified all events are read.",
    )
    parser.add_argument(
        "--from",
        type=int,
        default=None,
        dest="from_var",
        help="the first tx id to read from",
    )
    parser.add_argument(
        "--to", type=int, default=None, help="the last tx id to read to"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="the max number of events to fetch"
    )
    parser.add_argument(
        "--pretty-json",
        action="store_true",
        help="if true indent the JSON, make it look nice",
    )
    parser.add_argument(
        "--tail",
        action="store_true",
        help="If true, treat `limit` as batch size and continue to watch for new events",
    )

    args = parser.parse_args()

    meow = demolib.create_meowmx()

    if args.limit == 0:
        print("bad value for limit: 0", file=sys.stderr)
        sys.exit(1)

    id_mode = args.aggregate_id or args.aggregate_type
    if id_mode and not (args.aggregate_id or args.aggregate_type):
        print(
            "must specify both aggregate type and aggregate ID or niether",
            file=sys.stderr,
        )
        sys.exit(1)

    backoff = BackoffCalc(1, 5)
    while True:
        if id_mode:
            events = meow.load_events(
                aggregate_id=args.aggregate_id,
                aggregate_type=args.aggregate_type,
                from_version=args.from_var,
                to_version=args.to,
                limit=args.limit,
            )
        else:
            events = meow.load_all_events(
                from_tx_id=args.from_var, to_tx_id=args.to, limit=args.limit
            )

        indent = 4 if args.pretty_json else None
        for event in events:
            print(
                f"{event.id} (tx {event.tx_id}) : {event.aggregate_type} / {event.aggregate_id} [{event.version}] {event.event_type} : {json.dumps(event.json, indent=indent)}"
            )

        if not args.tail:
            break
        else:
            if len(events) == 0:
                # sleep and try again
                time.sleep(backoff.failure())
            else:
                current_position: int
                if id_mode:
                    current_position = events[-1].version
                else:
                    current_position = events[-1].tx_id
                if args.to is not None and args.to <= current_position:
                    break

                backoff.success()
                args.from_var = current_position


if __name__ == "__main__":
    main()
