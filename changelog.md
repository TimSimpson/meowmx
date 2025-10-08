# Changelog

## [0.2.0] - 2025-10-08

- Renamed all of the tables to plural versions of the nouns. So `es_event` became `es_events`, `es_aggregate` turned into `es_aggregates`, etc. It's absolutely bike-shedding nonsense but it just looked too out of place next to all the tables I work with, though I readily concede that because the author of [postgresql-event-sourcing](https://github.com/eugene-khyst/postgresql-event-sourcing) did it the other way I might be the one who is wrong here.
- Fixed a bug where saving multiple events failed to bump up the aggregate version number high enough.


## [0.1.3] - 2025-10-06

- Renamed `load` and `load_all` to `load_events` and `load_all_events`.
- Added code to load_events and load_aggregate to accept a session.

## [0.1.2] - 2025-09-30

### Added

- Added the ability to customize the type of the `id` column in the `es_aggregate` table when it's first created.
- Changed the type of event "json_data" to the string representation of the underlying data which users must load themselves.
- Added abiity to save and load aggregate types directly, and an EventBuffer class to make it easier to build these types.

## [0.1.1] - 2025-09-26

### Added

- Several functions in meowmx.Client now accept SqlAlchemy sessions and if given won't commit the transactions.

- when reading from events, change `from` to be inclusive, to to be exclusive? Not sure why it wasn't like that before.

## [0.1.0] - 2025-09-23

### Added

Initial functionality.
