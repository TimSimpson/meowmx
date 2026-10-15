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

setup:
    uv sync --locked --all-extras --dev
    uv pip install psycopg[binary]

start-docker-db:
    {{ docker }} run -d --name postgres-meowmx -e POSTGRES_PASSWORD=meowmx -e POSTGRES_USER=meowmx -e POSTGRES_DB=meowmx -p 5445:5432 docker.io/postgres


clean-sqlite:
    mkdir -p target
    rm target/sqlite.db

test:
    mkdir -p target
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db'

test-psql:
    uv run -- pytest -vv --sql-type postgresql --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type 'char 36' --iterations 1

test-subs *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type 'char 36' tests/test_subs.py {{ args }}

test-new-rows *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type 'char 36' tests/test_new_rows.py {{ args }}

# runs every configuration, leaving a coverage report in ./htmlcov
test-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{ justfile_directory() }}'
    if [[ -f '.coverage' ]]; then
        rm '.coverage'
    fi    
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db' --cov=meowmx --cov-append     
    uv run -- pytest -vv --sql-type postgresql --sql-url 'postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable' --uuid-type 'char 36' --iterations 1 --cov=meowmx --cov-append 
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db' --uuid-type 'xid' --iterations 1 --cov=meowmx --cov-append 
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///:memory:' --cov=meowmx --cov-append 
    uv run -- coverage html

usql:
    PAGER=cat usql 'postgres://meowmx:meowmx@localhost:5445/meowmx'
