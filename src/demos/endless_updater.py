import base64
import random
import sys
import typing as t

import demolib
import meowmx

def validate_events(events: t.List[meowmx.Event]) -> None:
    expected = 1
    for e in events:
        if expected> 1:
            actual = t.cast(demolib.CatUpdated, e).version
            if expected != actual:
                raise RuntimeError(f"Oh no, version expected was {expected} but go {actual}")
        expected += 1        

def main() -> None:
    cat_id = sys.argv[1]

    meow = meowmx.Client()
    meow.setup_tables()

    stream_id = meowmx.StreamId(category="cats", name=cat_id)

    while True:
        events = meow.load(stream_id)            
        validate_events(events)
        event = events[-1] if events else None        
        print(event)
        
        expected_version = 0
        if event:
            expected_version = event.version

        if expected_version == 0:
            new_event: meowmx.Event = demolib.CatCreated(cat_name=cat_id)
        else:
            random.randbytes(8)
            s = str(base64.urlsafe_b64encode(random.randbytes(8)))
            new_event = demolib.CatUpdated(
                new_random_value=s, version=int(expected_version)
            )

        try:
            meow.publish(new_event, stream_id=stream_id, expected_version=expected_version)
        except meowmx.ConcurrentStreamWriteError:
            pass # just retry


if __name__ == "__main__":
    main()
