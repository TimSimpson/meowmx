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

The code also has nerfed support for other databases with SqlAlchemy, but this is just useful for testing (for example: if an app uses sqlalchemy with Postgres in production but supports using Sqlite for automated tests).

## Usage

Import the `meowmx` package and create a client.

Import `meowmx`. 

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