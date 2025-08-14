import base64
import random
import sys

import demolib
import meowmx


def main() -> None:
    cat_id = sys.argv[1]

    meow = meowmx.Client()
    meow.setup_tables()
    
    stream_id = meowmx.StreamId(category="cats", name=cat_id)

    while True:
        events = meow.load(stream_id)
        event = events[-1] if events else None
        print(event)

        print("Hit enter!")
        input()

        expected_version = 0
        if event:
            expected_version = event.version

        if expected_version == 0:
            new_event: meowmx.Event = demolib.CatCreated(cat_name=cat_id)
        else:
            random.randbytes(8)
            s = str(base64.urlsafe_b64encode(random.randbytes(8)))
            new_event = demolib.CatUpdated(new_random_value=s)    

        meow.publish(new_event, stream_id=stream_id, expected_version=expected_version)


if __name__ == "__main__":
    main()
