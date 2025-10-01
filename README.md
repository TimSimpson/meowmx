# meowmx - Managed Event Orchestration With Multiple eXecutors

This takes many of the ideas from Eugene Khyst's [postgresql-event-sourcing](https://github.com/eugene-khyst/postgresql-event-sourcing) project and implements them worse in Python.

The end result, meowmx, lets you use PostgreSQL to:

* write events containing plain JSON data, which can later be looked up by category or aggregate IDs
* subscribe to events, allowing you to create workers that iterate through them.

Also, check out this cat! 

```
        ^__^         
    \   o . o    <  M E O W >
     |    ---
     ..    ..
```

## Why would anyone want to do this?

Think of how many systems where different components communicate by passing events. Usually you have some system that gets notified of a change or otherwise spurred to action by an event payload. It then loads a bunch of persisted data, does some stuff, and moves on.

The thing is quite often the persisted data should be the event itself. For instance let's say you have multiple events concerning "orders" in a system, ie a customer initiates an order, an order is confirmed, an order is shipped, etc. The traditional way to handle this is to model the current state of the order as a row in a SQL database. Then you have all these event handlers (maybe they're Lambda functions, maybe they listen to rabbit, etc) noifying components in a modern system which all then have to load the current order row, figure out how it should be modified, consider if another component has also updated the row, etc.

To make a gross simplification event-sourcing just says hey maybe all those events _are_ the persisted state; just load them all to figure out what the current situation is, make an update to the same set of events, and as a cherry on top use optimistic concurrency so the update fails if we find some other system updated our set of events between the time we read them just know and when we went to write them, in which case we'll cancle our write, reload all the events and try this logic again.

There's also the notion of aggregates, which are basically objects that can be constructed by reading a set of events. In my experience that kind of "helper" code is extremely easy to write but obscures the basic utility of event sourcing libraries like this one. The right way to construct it is also fairly opinionated. So for now this project has no "aggregate" code. 

## Notes on SqlAlchemy

This code assumes Postgres via SqlAlchemy.

The code also has nerfed support for other databases with SqlAlchemy, but this is just to be useful for testing. In memory databases have some errors when it comes to listening to subscribers, so only file based sqlite databases are supported for now.

## Usage

Create an engine with sqlalchemy, then create a meowmx client by passing it in:

```py
import meowmx
import sqlalchemy.orm

test_db_url = "postgresql+psycopg://user:pass@localhost:5445/myapp?sslmode=disable"
engine = sqlalchemy.create_engine(test_db_url)
# note: the session_maker arg is optional
session_maker = sqlalchemy.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)
meow = meowmx.Client(engine=engine, session_maker=session_maker)
```

### Initial table setup

To initially create the tables used by meowmx, call `meowmx.setup_tables()`. 

```py
meow.setup_tables()
```

Note: by default meowmx creates a UUID column for the aggregate IDs, but this can be changed if an argument is passed in as follows:

```py
meow.setup_tables(aggregate_id_column_type="CHAR(20)")
```

This argument has no effect if other database besides postgres are used; instead the column type is `CHAR(64)`. This argument also only works the first time `setup_tables` is called.

For a production ready app you probably already have a method of standing up your tables. You can see what tables meowmx builds by looking at [migrations.py](src/meowmx/esp/migrations.py), which was mostly lifted from [postgresql-event-sourcing](https://github.com/eugene-khyst/postgresql-event-sourcing).

### Writing Events

Call meow.save_events to persist / publish events:

```py
order_id = "<order-id-here>"
order_created = meowmx.NewEvent(
    event_type="OrderCreated",
    json={
        "customer_id": customer_id,
        "order_id": order_id,
        "time": datetime.now().isoformat(),
    },
)
meow.save_events("order", order_id, [order_created], version=0)

# save a second event

order_created = meowmx.NewEvent(
    event_type="OrderShipped",
    json={
        "order_id": order_id,
        "time": datetime.now().isoformat(),
    },
)
meow.save_events("order", order_id, [order_created], version=1)
```

### Subscribing to Events

Let's say you want to create a read model for an aggregate that is updated every time an event is written for the aggregate.

One way to do that is by subscribing to all changes in the events from another process:

```py
from sqlalchemy import orm

class Order(Base):
    __tablename__ = "orders"

    id = orm.mapped_column(sqlalchemy.CHAR(20), primary_key=True)
    version = orm.mapped_column(sqlalchemy.Integer, nullable=False)
    customer_id = orm.mapped_column(sqlalchemy.String, nullable=False)

def build_read_model(events: list[meowmx.RecordedEvent]) -> Order:
    order = Order()
    for event in events:
        if event.event_type == "OrderCreated":
            order.id = event.json["order_id"]
            order.customer_id = event.json["customer_id"]
            order.shipped = False
        elif event.event_type == "OrderShipped":
            order.shipped = True
        else:
            log.warning("unknown event type")
            # don't raise an exception as the events are historical
        order.version = event.version
    
    return order


# runs until the process is killed
def start_subscription(meow: meowmx.Client) -> None:
    def handler(session: meowmx.Session, event: meowmx.RecordedEvent) -> None:
        order_events = meow.load(event.aggregate_type, event.aggregate_id)
        order = build_read_model(order_events)
        session.merge(order)
        session.commit()

    meow.sub(
        subscription_name="order-rm-builder", 
        aggregate_name="order", 
        batch_size=10,
        max_sleep_time=30,
        handler=handler: 
    )



```
See the files in [examples](examples/).


Setup:

```bash
just start-docker-db
just usql  # open repl
just test-psql
just examples read-events # view all events written by the tests
just examples # see examples
```

## TODO

[ ] don't force `version` on NewEvent, it's a headache
[ ] Allow for other aggregate IDs types