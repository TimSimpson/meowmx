# meowmx - Managed Event Orchestration With Multiple eXecutors

This project explored orchestrating managed events with multiple executors.

It uses an aggregate which models the installation of security agents.

```
        ^__^         
    \   o . o    <  M E O W >
     |    ---
     ..    ..
```

## Usage

Setup:

```bash
just start-docker-db
just usql  # open repl
uv sync
```

To view all events as they happen, run this in a terminal:

```bash
uv run -- demo/sub  # watch for changes in a screen
```

Now tun this command in another terminal:

```bash
uv run -- demo/update-cat blanco  # update a stream in a screen. Run this twice to look at 
```

It will load a stream named "blanco", prompt you to hit enter, and then save a new event to the stream.

To test atomic writes, start this in two terminals, then hit enter in one and then the other. It will fail in the second one since the version number for the stream will be old.
