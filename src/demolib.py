import os
import sqlalchemy
import meowmx


DEMO_PG_URL = os.environ.get(
    "MEOWMX_PG_URL",
    "postgresql+psycopg://meowmx:meowmx@localhost:5445/meowmx?sslmode=disable",
)


def create_meowmx() -> meowmx.Client:
    engine = sqlalchemy.create_engine(DEMO_PG_URL)
    return meowmx.Client(engine=engine)
