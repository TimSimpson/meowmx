docker := env_var_or_default("DOCKER_CLI", "docker")

_default:
    just --list
    
check:
    uv run -- ruff check
    uv run -- mypy ./
    uv run -- ruff format

# runs an example
examples *args:
    just --justfile '{{ justfile_directory() }}/examples/justfile' {{args}}

run:
    uv run -- python main.py

start-docker-db:
    {{ docker }} run -d --name postgres-sqlalchemy -e POSTGRES_PASSWORD=eventsourcing -e POSTGRES_USER=eventsourcing -e POSTGRES_DB=eventsourcing -p 5443:5432 docker.io/postgres

start-docker-db-2:
    {{ docker }} run -d --name postgres-meowmx -e POSTGRES_PASSWORD=meowmx -e POSTGRES_USER=meowmx -e POSTGRES_DB=meowmx -p 5445:5432 docker.io/postgres


clean-sqlite:
    mkdir -p target
    rm target/sqlite.db

test:
    mkdir -p target
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db'

test-psql:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable'

test-psql-2:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type xid --iterations 1

test-subs-1 *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable' --uuid-type uuid4 tests/test_subs.py {{ args }}

test-subs-2 *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type xid tests/test_subs.py {{ args }}

test-new-rows *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type xid tests/test_new_rows.py {{ args }}

usql:
    PAGER=cat usql 'postgres://eventsourcing:eventsourcing@localhost:5443/eventsourcing'

usql-2:
    PAGER=cat usql 'postgres://meowmx:meowmx@localhost:5445/meowmx'
