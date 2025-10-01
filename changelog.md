# Changelog

## [unreleased]

### Added

- Added the ability to customize the type of the `id` column in the `es_aggregate` table when it's first created.
- Changed the type of event "json_data" to the string representation of the underlying data which users must load themselves.
- Added abiity to save and load aggregate types directly, and an EventBuffer class to make it easier to build these types.

## [0.1.0] - 2025-09-26

### Added

- Several functions in meowmx.Client now accept SqlAlchemy sessions and if given won't commit the transactions.

- when reading from events, change `from` to be inclusive, to to be exclusive? Not sure why it wasn't like that before.

## [0.1.0] - 2025-09-23

### Added

Initial functionality.
