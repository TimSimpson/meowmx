# import meowmx
# from meowmx.esp import esp

# # We have to import the event or else it won't be known at runtime.
# # The "watch_all" demo doesn't have this problem as it relies on using the
# # "unknown" type. But unfortunately there doesn't seem to be a way to see
# # what the type was in that case.
# import demolib  # noqa


# def main() -> None:
#     meow = meowmx.Client(demolib.DEMO_PG_URL)
#     meow.setup_tables()
#     es = meow.esp()
#     es.setup_tables()

#     aggregate_id = "b5ce6861-1572-4900-9a88-e05fce485c54"

#     version=1185
#     event = esp.NewEvent(
#         aggregate_id=aggregate_id,
#         version=version,
#         event_type="create_user",
#         json={"name": "Tim"},
#     )
#     es.create_aggregate_if_absent("user", aggregate_id)
#     es.append_event(event)

#     recorded_events = es.read_events(aggregate_id, from_version=None, to_version=None)
#     for e in recorded_events:
#         print(e)

#     while True:
#         version+= 1
#         event = esp.NewEvent(
#             aggregate_id=aggregate_id,
#             version=version,
#             event_type="create_user",
#             json={"name": "Tim"},
#         )
#         es.append_event(event)
#         print(f"appended: {event}")

# if __name__ == "__main__":
#     main()
