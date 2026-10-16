import argparse
import typing as t

import demolib


def main() -> None:
    parser = argparse.ArgumentParser(
        description="demo of what deleting aggregates looks like (WARNING! THIS IS DANGEROUS!)"
    )
    parser.add_argument(
        "--aggregate-type",
        type=str,
        required=False,
        help="the type of aggregate. Sometimes called the category of a stream.",
    )
    parser.add_argument(
        "--from-id",
        type=str,
        required=False,
        help="the aggregate ID to delete",
    )

    args = parser.parse_args()

    meow = demolib.create_meowmx()

    count = 0
    next_id: t.Optional[str] = None
    while True:
        agg_info, next_id = meow.list_aggregates(
            aggregate_type=args.aggregate_type, from_id=args.from_id, limit=128
        )
        for agg in agg_info:
            print(f"Deleting {agg.id} - {agg.aggregate_type} [{agg.version}]...")
            meow.delete_events(aggregate_id=agg.id)
            count += 1

        if next_id is None:
            break

    print(f"Deleted {count} aggregate(s).")


if __name__ == "__main__":
    main()
