import argparse
import typing as t

import demolib


def main() -> None:
    parser = argparse.ArgumentParser(
        description="demo of what deleting aggregates looks like"
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
        help="the aggregate ID to start deleting at",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        required=False,
        help="this must be passed if you wish to destroy all aggregates in the database",
    )
    parser.add_argument(
        "--anything",
        action="store_true",
        required=False,
        help="this must be passed if you wish to pass a limit and delete some arbitrary aggregates in the database",
    )
    parser.add_argument(
        "--limit",
        type=int,
        required=False,
        help="how many aggregates to delete",
    )

    args = parser.parse_args()

    meow = demolib.create_meowmx()

    if args.anything and args.limit is None:
        raise ValueError(
            "--anything just confirms you want to delete `anything` and "
            "requires --limit. Pass `--all` if you want to delete the entire db"
        )

    if not args.all and not args.anything:
        if args.aggregate_type is None and args.from_id is None:
            raise ValueError(
                "--aggregate-type, --from-id, --anything, or --all must be passed"
            )

    if args.all and args.limit is not None:
        raise ValueError(
            "--limit makes no sense with `--all` (pass `--anything` with --limit)"
        )

    limit = 128 if args.limit is None else args.limit
    count = 0
    next_id: t.Optional[str] = args.from_id
    while True:
        agg_info, next_id = meow.list_aggregates(
            aggregate_type=args.aggregate_type, from_id=next_id, limit=limit
        )
        for agg in agg_info:
            print(f"Deleting {agg.id} - {agg.aggregate_type} [{agg.version}]...")
            meow.delete_events(aggregate_id=agg.id)
            count += 1

        if next_id is None or not args.all:
            break

    print(f"Deleted {count} aggregate(s).")


if __name__ == "__main__":
    main()
