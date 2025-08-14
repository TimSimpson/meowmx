docker := env_var_or_default("DOCKER_CLI", "docker")


_default:
    just --list
    
check:
    uv run -- ruff check
    uv run -- mypy ./
    uv run -- ruff fmt

run:
    uv run -- python main.py

start-docker-db:
    {{ docker }} run -d --name postgres-sqlalchemy -e POSTGRES_PASSWORD=eventsourcing -e POSTGRES_USER=eventsourcing -e POSTGRES_DB=eventsourcing -p 5443:5432 docker.io/postgres


test-basic:
    uv run -- pytest tests/test_basic.py

test-pg:
    uv run -- pytest tests/test_pg.py

test-sqla:
    uv run -- pytest tests/test_sqla.py

repl-pg:
    PAGER=cat usql 'postgres://eventsourcing:eventsourcing@localhost:5442/eventsourcing'

usql:
    PAGER=cat usql 'postgres://eventsourcing:eventsourcing@localhost:5443/eventsourcing'
