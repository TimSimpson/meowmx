import argparse
import json
import demolib
import meowmx


def main() -> None:
    parser = argparse.ArgumentParser(description="saves a single event")
    parser.add_argument(
        "--aggregate-type",
        type=str,
        required=True,
        help="the type of aggregate. Sometimes called the category of a stream.",
    )
    parser.add_argument(
        "--aggregate-id", type=str, required=True, help="the ID of the aggregate"
    )
    parser.add_argument("--event-type", type=str, required=True, help="Adds event type")
    parser.add_argument(
        "--data",
        type=str,
        required=True,
        help="the JSON payload",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=None,
        required=True,
        help="the expected version (useful to test optimistic concurrency controls)",
    )

    args = parser.parse_args()

    meow = meowmx.Client(demolib.DEMO_PG_URL)

    try:
        json_obj = json.loads(args.data)
    except json.decoder.JSONDecodeError:
        print(f"Error with input data: {args.data}")
        raise

    events = [
        meowmx.NewEvent(
            aggregate_id=args.aggregate_id,
            event_type=args.event_type,
            json=json_obj,
            version=args.version,
        )
    ]
    meow.save_events(args.aggregate_type, args.aggregate_id, events)


if __name__ == "__main__":
    main()
