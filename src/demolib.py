import meowmx


DEMO_PG_URL = "postgresql+psycopg://eventsourcing:eventsourcing@localhost:5443/eventsourcing?sslmode=disable"


class CatCreated(meowmx.Event):
    cat_name: str


class CatUpdated(meowmx.Event):
    new_random_value: str
    version: int
