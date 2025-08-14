import meowmx

# We have to import the event or else it won't be known at runtime.
# The "watch_all" demo doesn't have this problem as it relies on using the
# "unknown" type. But unfortunately there doesn't seem to be a way to see
# what the type was in that case.
import demolib  # noqa


def main() -> None:
    meow = meowmx.Client()
    meow.setup_tables()

    for event in meow.sub(category="cats"):
        if not event:
            print("Waiting...")
        else:
            print(f"{event.stream_id}")
            print(f"\tposition={event.position}")
            print(f"\tversion ={event.wrapped_event.version}")
            print(f"\twrap    ={event.wrapped_event.event.model_dump_json()}")


if __name__ == "__main__":
    main()
