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

clean-sqlite:
    mkdir -p target
    rm target/sqlite.db

test:
    mkdir -p target
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db'

test-psql:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable'

usql:
    PAGER=cat usql 'postgres://eventsourcing:eventsourcing@localhost:5443/eventsourcing'
