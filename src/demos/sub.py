import meowmx

# We have to import the event or else it won't be known at runtime:
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
