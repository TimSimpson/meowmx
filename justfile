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

usql:
    PAGER=cat usql 'postgres://eventsourcing:eventsourcing@localhost:5443/eventsourcing'
