import typing as t
import pytest

import meowmx
import sqlalchemy


def pytest_addoption(parser: t.Any) -> None:
    parser.addoption("--sql-type", action="store", default="sqlite")
    parser.addoption("--sql-url", action="store", default="sqlite:///:memory:")


@pytest.fixture
def sql_type_cmd_opt(request: t.Any) -> str:
    return t.cast(str, request.config.getoption("--sql-type"))


@pytest.fixture
def sql_url_cmd_opt(request: t.Any) -> str:
    return t.cast(str, request.config.getoption("--sql-url"))


@pytest.fixture
def engine(sql_type_cmd_opt: str, sql_url_cmd_opt: str) -> meowmx.Engine:
    """Creates the SQL client based on the command line option --sql-type."""
    if sql_url_cmd_opt.endswith(":memory:"):
        # tell SqlAlchemy to give the same connection to each thread, which
        # is required for the subscription / worker test.
        return sqlalchemy.create_engine(
            sql_url_cmd_opt,
            poolclass=sqlalchemy.pool.StaticPool,
            connect_args={"check_same_thread": False},
        )
    else:
        return sqlalchemy.create_engine(sql_url_cmd_opt)


@pytest.fixture
def session_maker(engine: meowmx.Engine) -> meowmx.SessionMaker:
    return sqlalchemy.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)


_setup_was_run = False


@pytest.fixture
def meow(
    sql_url_cmd_opt: str, engine: meowmx.Engine, session_maker: meowmx.SessionMaker
) -> meowmx.Client:
    client = meowmx.Client(engine=engine, session_maker=session_maker)
    global _setup_was_run
    if not _setup_was_run:
        client.setup_tables()
        # for in memory sqlite we need to create it for every test
        if not sql_url_cmd_opt.endswith(":memory:"):
            _setup_was_run = True
    return client

    # "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable"
