from datetime import datetime
import typing as t
from sqlalchemy import orm



def configure_models(base: type[t.Any]) -> None:
    for model_cls in (Workers, WorkerEvents):
        orm.registry(metadata=base.metadata, class_registry={}).map_declaratively(model_cls)


class Workers:
    __tablename__ = "workers"
    __table_args__ = orm.Index(
        "ix_worker_names",
        "name",
        unique=True,
    )

    name = orm.mapped_column(orm.String(64), primary_key=True)
    last_update_time = orm.mapped_column(orm.DateTime(), nullable=False, index=True)
    last_update_time = datetime


class WorkerEvents:
    __tablename__ = "worker_events"
    __table_args__ = orm.Index(
        "ix_worker_names",
        "worker_name",
        "position",
        unique=True,
        primary_key=True, 
    )

    worker_name = orm.mapped_column(orm.String(64))
    position = orm.BigInteger().with_variant(orm.Integer(), "sqlite"),
    stream_id = orm.mapped_column(orm.String(255))
    event_id = orm.mapped_column(orm.BigInteger().with_variant(orm.Integer(), "sqlite"))
    update_time = orm.mapped_column(orm.DateTime(), nullable=False)
    success = orm.mapped_column(orm.Bool(), nullable=False)
    