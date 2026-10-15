docker := env_var_or_default("DOCKER_CLI", "docker")
docker_container := 'postgres-meowmx'
db_name := "meowmx"
db_user := "meowmx"
db_password := "meowmx"

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
    {{ docker }} run -d --name '{{ docker_container }}' -e 'POSTGRES_PASSWORD={{ db_password }}' -e 'POSTGRES_USER={{ db_user }}' -e 'POSTGRES_DB={{ db_name }}' -p 5445:5432 docker.io/postgres


clean-sqlite:
    mkdir -p target
    rm target/sqlite.db

test:
    mkdir -p target
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db'

test-psql:
    uv run -- pytest -vv --sql-type postgresql --sql-url 'postgresql+psycopg://{{ db_user }}:{{ db_password }}@localhost:5445/{{ db_name }}?sslmode=disable' --uuid-type 'char 36' --iterations 1

test-subs *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://{{ db_user }}:{{ db_password }}@localhost:5445/{{ db_name }}?sslmode=disable' --uuid-type 'char 36' tests/test_subs.py {{ args }}

test-new-rows *args:
    uv run -- pytest -vv --sql-type sqlite --sql-url 'postgresql+psycopg://{{ db_user }}:{{ db_password }}@localhost:5445/{{ db_name }}?sslmode=disable' --uuid-type 'char 36' tests/test_new_rows.py {{ args }}

# runs every configuration, leaving a coverage report in ./htmlcov
test-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{ justfile_directory() }}'
    if [[ -f '.coverage' ]]; then
        rm '.coverage'
    fi    
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db' --cov=meowmx --cov-append     
    uv run -- pytest -vv --sql-type postgresql --sql-url 'postgresql+psycopg://{{ db_user }}:{{ db_password }}@localhost:5445/{{ db_name }}?sslmode=disable' --uuid-type 'char 36' --iterations 1 --cov=meowmx --cov-append 
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///target/sqlite.db' --uuid-type 'xid' --iterations 1 --cov=meowmx --cov-append 
    uv run -- pytest -vv --sql-type sqlite --sql-url 'sqlite:///:memory:' --cov=meowmx --cov-append 
    uv run -- coverage html

usql:
    PAGER=cat usql 'postgres://{{ db_user }}:{{ db_password }}@localhost:5445/{{ db_name }}'


dump-docker-db:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target
    readonly dump_file="target/meowmx-$(date +%Y%m%d-%H%M%S).dump"        
    {{ docker }} exec '{{ docker_container }}' pg_dump -U '{{ db_user }}' -d '{{ db_name }}' -Fc > "${dump_file}"
    echo "Wrote to ${dump_file}."

restore-docker-db dump_file:
    #!/usr/bin/env bash
    set -euo pipefail
    
    # {{ docker }} exec -i '{{ docker_container }}' createdb -U '{{ db_user }}' '{{ db_name }}'
    {{ docker }} exec -i '{{ docker_container }}' pg_restore -U '{{ db_user }}' -d '{{ db_name }}' --no-owner < '{{ dump_file }}'
    echo 'Wrote {{ dump_file }} to database'