# Changelog

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
