from datetime import datetime
import typing as t

import coolname  # type: ignore
import pytest
import sqlalchemy
from sqlalchemy import orm

import meowmx


class Base(orm.DeclarativeBase):
    pass


class Potato(Base):
    __tablename__ = "potatoes"

    id = orm.mapped_column(sqlalchemy.CHAR(64), primary_key=True)
    version = orm.mapped_column(sqlalchemy.Integer, nullable=False)
    description = orm.mapped_column(sqlalchemy.String, nullable=False)


def _generate_slug() -> str:
    return t.cast(str, coolname.generate_slug())


class ExampleError(RuntimeError):
    pass


def test_combined_read_model(
    engine: meowmx.Engine, meow: meowmx.Client, new_uuid: t.Callable[[], str]
) -> None:
    Base.metadata.create_all(engine)

    aggregate_type = "potato"
    potato_id = new_uuid()

    def load_potato() -> t.Optional[Potato]:
        with orm.Session(engine) as session:
            select = sqlalchemy.select(Potato).where(Potato.id == potato_id)
            return session.execute(select).scalar_one_or_none()

    def update_potato(
        version: int,
        event_type: str,
        event_json: t.Mapping,
        description: str,
        raise_error: bool,
    ) -> None:
        events = [
            meowmx.NewEvent(
                event_type="PotatoCreated",
                json={
                    "description": description,
                    "time": datetime.now().isoformat(),
                },
            ),
        ]

        with orm.Session(engine) as session:
            with session.begin():
                meow.save_events(
                    aggregate_type, potato_id, events, version=version, session=session
                )

                new_row = Potato(id=potato_id, version=version, description=description)
                session.merge(new_row)
                if raise_error:
                    raise ExampleError("pretend an error happened")
                # session.commit()

    created_desc = _generate_slug()
    update_potato(
        0,
        "PotatoCreated",
        {
            "description": created_desc,
            "time": datetime.now().isoformat(),
        },
        created_desc,
        raise_error=False,
    )

    potato_events = meow.load(aggregate_type, potato_id, from_version=0)
    # assert 1 == len(potato_events)
    potato_rm = load_potato()
    assert potato_rm is not None
    assert potato_rm.version == 0

    modified_desc = _generate_slug()
    with pytest.raises(ExampleError):
        update_potato(
            1,
            "PotatoModified",
            {
                "description": modified_desc,
                "time": datetime.now().isoformat(),
            },
            modified_desc,
            raise_error=True,
        )

    potato_events = meow.load(aggregate_type, potato_id, from_version=0)
    assert 1 == len(potato_events)
    potato_rm = load_potato()
    assert potato_rm is not None
    assert potato_rm.version == 0

    update_potato(
        1,
        "PotatoModified",
        {
            "description": modified_desc,
            "time": datetime.now().isoformat(),
        },
        modified_desc,
        raise_error=False,
    )

    potato_events = meow.load(aggregate_type, potato_id, from_version=0)
    assert 2 == len(potato_events)
    potato_rm = load_potato()
    assert potato_rm is not None
    assert potato_rm.version == 1
    assert potato_rm.description == modified_desc


def test_combined_event_writes(
    engine: meowmx.Engine, meow: meowmx.Client, new_uuid: t.Callable[[], str]
) -> None:
    Base.metadata.create_all(engine)

    aggregate_type_1 = "tree"
    aggregate_type_2 = "flower"

    tree_id = new_uuid()
    flower_id = new_uuid()

    with orm.Session(engine) as session:
        with session.begin():
            meow.save_events(
                aggregate_type_1,
                tree_id,
                version=0,
                session=session,
                events=[
                    meowmx.NewEvent(
                        event_type="TreeCreated",
                        json={
                            "flower_id": flower_id,
                            "time": datetime.now().isoformat(),
                        },
                    ),
                ],
            )

            tree_events = meow.load(aggregate_type_1, tree_id, from_version=0)
            assert 0 == len(tree_events)

            meow.save_events(
                aggregate_type_2,
                flower_id,
                version=0,
                session=session,
                events=[
                    meowmx.NewEvent(
                        event_type="TreeCreated",
                        json={
                            "tree_id": tree_id,
                            "time": datetime.now().isoformat(),
                        },
                    ),
                ],
            )

    tree_events = meow.load(aggregate_type_1, tree_id, from_version=0)

    assert 1 == len(tree_events)
