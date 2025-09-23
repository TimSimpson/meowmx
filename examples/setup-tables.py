# We have to import the event or else it won't be known at runtime.
# The "watch_all" demo doesn't have this problem as it relies on using the
# "unknown" type. But unfortunately there doesn't seem to be a way to see
# what the type was in that case.
import demolib  # noqa


def main() -> None:
    meow = demolib.create_meowmx()
    meow.setup_tables()


if __name__ == "__main__":
    main()
