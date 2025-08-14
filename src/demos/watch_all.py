import meowmx


def main() -> None:
    meow = meowmx.Client()
    meow.setup_tables()

    for event in meow.sub():
        if not event:
            print("Waiting...")
        else:
            print(f"{event.stream_id}")
            print(f"\tposition={event.position}")
            print(f"\tversion ={event.wrapped_event.version}")
            print(f"\twrap    ={event.wrapped_event.event.model_dump_json()}")


if __name__ == "__main__":
    main()
