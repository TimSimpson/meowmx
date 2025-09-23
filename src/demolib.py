import sqlalchemy
import meowmx


DEMO_PG_URL = "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable"


def create_meowmx() -> meowmx.Client:
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable"
    )
    return meowmx.Client(engine=engine)


class CatCreated(meowmx.Event):
    cat_name: str


class CatUpdated(meowmx.Event):
    new_random_value: str
    version: int
