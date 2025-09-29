import typing as t

import coolname  # type: ignore
import sqlalchemy
from sqlalchemy import orm

import meowmx


class Base(orm.DeclarativeBase):
    pass


class Orange(Base):
    __tablename__ = "oranges"

    id = orm.mapped_column(sqlalchemy.CHAR(64), primary_key=True)
    description = orm.mapped_column(sqlalchemy.String, nullable=False)


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


# This exists to test row writes for a simple sanity-check benchmark
def test_write_organges(
    engine: meowmx.Engine,
    iterations_cmd_opt: t.Optional[int],
    new_uuid: t.Callable[[], str],
) -> None:
    Base.metadata.create_all(engine)

    with orm.Session(engine) as session:
        for i in range(iterations_cmd_opt or 1):
            with session.begin():
                new_row = Orange(id=new_uuid(), description=_generate_slug())
                session.merge(new_row)
                session.commit()
