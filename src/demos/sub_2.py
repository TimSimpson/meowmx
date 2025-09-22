# import time

# import meowmx
# from meowmx.esp import esp

# # We have to import the event or else it won't be known at runtime.
# # The "watch_all" demo doesn't have this problem as it relies on using the
# # "unknown" type. But unfortunately there doesn't seem to be a way to see
# # what the type was in that case.
# import demolib  # noqa


# def main() -> None:
#     meow = meowmx.Client()
#     meow.setup_tables()
#     es = meow.esp()

#     sub_name = "demo_sub"
#     es.create_subscription_if_absent(sub_name)

#     def handle_event(session: esp.Session, event: esp.RecordedEvent) -> None:
#         # use with session.begin_nested(): here somehow
#         print(f"EVENT ID: {event.id}, VERSION: {event.version}, TX_ID: {event.tx_id}")

#     while True:
#         processed = es.handle_subscription_events(sub_name, "user", 10, handle_event)
#         if processed == 0:
#             time.sleep(2)
#         else:
#             print(f"processed {processed} event(s)")


#     print("end of events")


# if __name__ == "__main__":
#     main()
